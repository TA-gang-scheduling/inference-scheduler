package plugin

import (
	"context"
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"
	"k8s.io/kube-scheduler/framework"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

const (
	Name = "InferenceGangScheduler"

	LabelJob  = "inference.example.com/job"
	LabelRole = "inference.example.com/role"

	RolePrefill = "prefill"
	RoleDecode  = "decode"

	PermitTimeout = 5 * time.Minute
)

// ─────────────────────────────────────────────
// Minimal InferenceJob types & Scheme
// ─────────────────────────────────────────────

type InferenceJobSpec struct {
	MinPrefillReplicas int32 `json:"minPrefillReplicas,omitempty"`
	MinDecodeReplicas  int32 `json:"minDecodeReplicas,omitempty"`
}

type InferenceJobStatus struct {
	Phase string `json:"phase,omitempty"`
}

type InferenceJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              InferenceJobSpec   `json:"spec,omitempty"`
	Status            InferenceJobStatus `json:"status,omitempty"`
}

type InferenceJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []InferenceJob `json:"items"`
}

func (ij *InferenceJob) DeepCopyObject() runtime.Object {
	out := &InferenceJob{}
	*out = *ij
	return out
}

func (ijl *InferenceJobList) DeepCopyObject() runtime.Object {
	out := &InferenceJobList{}
	*out = *ijl
	out.Items = make([]InferenceJob, len(ijl.Items))
	copy(out.Items, ijl.Items)
	return out
}

// ─────────────────────────────────────────────
// Gang state
// ─────────────────────────────────────────────

type gangState struct {
	mu              sync.Mutex
	reservedPods    map[types.UID]string
	prefillReserved int32
	decodeReserved  int32
}

type InferenceSchedulerPlugin struct {
	handle framework.Handle
	client client.Client

	mu     sync.Mutex
	groups map[string]*gangState
}

// Compile-time interface checks (Unreserve is part of ReservePlugin).
var _ framework.PreFilterPlugin = &InferenceSchedulerPlugin{}
var _ framework.ReservePlugin = &InferenceSchedulerPlugin{}
var _ framework.PermitPlugin = &InferenceSchedulerPlugin{}
var _ framework.PostBindPlugin = &InferenceSchedulerPlugin{}

var (
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	AddToScheme   = SchemeBuilder.AddToScheme
)

func addKnownTypes(s *runtime.Scheme) error {
	gv := schema.GroupVersion{Group: "inference.example.com", Version: "v1alpha1"}
	s.AddKnownTypes(gv, &InferenceJob{}, &InferenceJobList{})
	metav1.AddToGroupVersion(s, gv)
	return nil
}

// ─────────────────────────────────────────────
// Constructor
// ─────────────────────────────────────────────

func New(_ context.Context, _ runtime.Object, h framework.Handle) (framework.Plugin, error) {
	cfg, err := config.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("getting kubeconfig: %w", err)
	}

	s := runtime.NewScheme()
	if err := scheme.AddToScheme(s); err != nil {
		return nil, fmt.Errorf("adding standard scheme: %w", err)
	}
	if err := AddToScheme(s); err != nil {
		return nil, fmt.Errorf("adding custom scheme: %w", err)
	}

	c, err := client.New(cfg, client.Options{Scheme: s})
	if err != nil {
		return nil, fmt.Errorf("creating controller-runtime client: %w", err)
	}

	klog.InfoS("InferenceGangScheduler plugin initialized")

	return &InferenceSchedulerPlugin{
		handle: h,
		client: c,
		groups: make(map[string]*gangState),
	}, nil
}

func (p *InferenceSchedulerPlugin) Name() string { return Name }

func newGangState() *gangState {
	return &gangState{
		reservedPods: make(map[types.UID]string),
	}
}

func (p *InferenceSchedulerPlugin) getGroup(groupKey string) *gangState {
	p.mu.Lock()
	defer p.mu.Unlock()

	g, ok := p.groups[groupKey]
	if !ok {
		g = newGangState()
		p.groups[groupKey] = g
	} else if g.reservedPods == nil {
		g.reservedPods = make(map[types.UID]string)
	}

	return g
}

func (p *InferenceSchedulerPlugin) getMinima(ctx context.Context, pod *v1.Pod) (int32, int32, *framework.Status) {
	jobName := pod.Labels[LabelJob]

	var job InferenceJob
	if err := p.client.Get(ctx, types.NamespacedName{Name: jobName, Namespace: pod.Namespace}, &job); err != nil {
		return 0, 0, framework.NewStatus(framework.UnschedulableAndUnresolvable, err.Error())
	}

	minPrefill := job.Spec.MinPrefillReplicas
	if minPrefill < 1 {
		minPrefill = 1
	}

	minDecode := job.Spec.MinDecodeReplicas
	if minDecode < 1 {
		minDecode = 1
	}

	return minPrefill, minDecode, framework.NewStatus(framework.Success)
}

// ─────────────────────────────────────────────
// PreFilter
// ─────────────────────────────────────────────

func (p *InferenceSchedulerPlugin) PreFilter(ctx context.Context, _ framework.CycleState, pod *v1.Pod, _ []framework.NodeInfo) (*framework.PreFilterResult, *framework.Status) {
	jobName, hasJob := pod.Labels[LabelJob]
	role, hasRole := pod.Labels[LabelRole]

	if !hasJob || !hasRole {
		return nil, framework.NewStatus(framework.Skip)
	}

	if role != RolePrefill && role != RoleDecode {
		return nil, framework.NewStatus(framework.UnschedulableAndUnresolvable, fmt.Sprintf("unknown role %q", role))
	}

	var job InferenceJob
	if err := p.client.Get(ctx, types.NamespacedName{Name: jobName, Namespace: pod.Namespace}, &job); err != nil {
		return nil, framework.NewStatus(framework.UnschedulableAndUnresolvable, fmt.Sprintf("InferenceJob not found: %v", err))
	}

	return nil, framework.NewStatus(framework.Success)
}

func (p *InferenceSchedulerPlugin) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

// ─────────────────────────────────────────────
// Reserve & Unreserve
// ─────────────────────────────────────────────

func (p *InferenceSchedulerPlugin) Reserve(ctx context.Context, _ framework.CycleState, pod *v1.Pod, _ string) *framework.Status {
	jobName, hasJob := pod.Labels[LabelJob]
	role, hasRole := pod.Labels[LabelRole]

	if !hasJob || !hasRole {
		return framework.NewStatus(framework.Success)
	}

	groupKey := pod.Namespace + "/" + jobName

	minPrefill, minDecode, status := p.getMinima(ctx, pod)
	if !status.IsSuccess() {
		return status
	}

	g := p.getGroup(groupKey)
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, ok := g.reservedPods[pod.UID]; ok {
		return framework.NewStatus(framework.Success)
	}

	switch role {
	case RolePrefill:
		if g.prefillReserved >= minPrefill {
			klog.InfoS("Reserve: role quota full", "pod", klog.KObj(pod), "job", groupKey, "role", role, "reserved", g.prefillReserved, "min", minPrefill)
			return framework.NewStatus(framework.Unschedulable, "prefill quorum already reserved for this wave")
		}
		g.reservedPods[pod.UID] = role
		g.prefillReserved++
	case RoleDecode:
		if g.decodeReserved >= minDecode {
			klog.InfoS("Reserve: role quota full", "pod", klog.KObj(pod), "job", groupKey, "role", role, "reserved", g.decodeReserved, "min", minDecode)
			return framework.NewStatus(framework.Unschedulable, "decode quorum already reserved for this wave")
		}
		g.reservedPods[pod.UID] = role
		g.decodeReserved++
	}

	return framework.NewStatus(framework.Success)
}

func (p *InferenceSchedulerPlugin) Unreserve(ctx context.Context, _ framework.CycleState, pod *v1.Pod, _ string) {
	jobName := pod.Labels[LabelJob]
	groupKey := pod.Namespace + "/" + jobName

	p.mu.Lock()
	g, ok := p.groups[groupKey]
	p.mu.Unlock()

	if ok {
		g.mu.Lock()
		if role, reserved := g.reservedPods[pod.UID]; reserved {
			if role == RolePrefill && g.prefillReserved > 0 {
				g.prefillReserved--
			} else if role == RoleDecode && g.decodeReserved > 0 {
				g.decodeReserved--
			}
			delete(g.reservedPods, pod.UID)
		}
		g.mu.Unlock()
	}
}

// ─────────────────────────────────────────────
// Permit
// ─────────────────────────────────────────────

func (p *InferenceSchedulerPlugin) Permit(ctx context.Context, _ framework.CycleState, pod *v1.Pod, _ string) (*framework.Status, time.Duration) {
	jobName, hasJob := pod.Labels[LabelJob]
	if !hasJob {
		return framework.NewStatus(framework.Success), 0
	}

	groupKey := pod.Namespace + "/" + jobName

	minPrefill, minDecode, status := p.getMinima(ctx, pod)
	if !status.IsSuccess() {
		return status, 0
	}

	g := p.getGroup(groupKey)
	g.mu.Lock()
	currentPrefill := g.prefillReserved
	currentDecode := g.decodeReserved

	if currentPrefill >= minPrefill && currentDecode >= minDecode {
		klog.InfoS("Permit: gang released", "job", groupKey, "prefillReady", currentPrefill, "decodeReady", currentDecode)

		releasePods := make(map[types.UID]struct{}, len(g.reservedPods))
		for uid := range g.reservedPods {
			releasePods[uid] = struct{}{}
		}
		g.reservedPods = make(map[types.UID]string)
		g.prefillReserved = 0
		g.decodeReserved = 0
		g.mu.Unlock()

		p.handle.IterateOverWaitingPods(func(wp framework.WaitingPod) {
			if _, ok := releasePods[wp.GetPod().UID]; ok {
				wp.Allow(Name)
			}
		})

		return framework.NewStatus(framework.Success), 0
	}
	g.mu.Unlock()

	klog.V(2).InfoS("Permit: holding pod", "pod", klog.KObj(pod), "job", groupKey, "prefillReady", currentPrefill, "decodeReady", currentDecode, "minPrefill", minPrefill, "minDecode", minDecode)
	return framework.NewStatus(framework.Wait), PermitTimeout
}

// ─────────────────────────────────────────────
// PostBind
// ─────────────────────────────────────────────

func (p *InferenceSchedulerPlugin) PostBind(ctx context.Context, _ framework.CycleState, pod *v1.Pod, _ string) {
	jobName := pod.Labels[LabelJob]
	key := types.NamespacedName{Name: jobName, Namespace: pod.Namespace}

	var job InferenceJob
	if err := p.client.Get(ctx, key, &job); err != nil {
		klog.ErrorS(err, "PostBind: failed to get InferenceJob", "job", key)
		return
	}

	if job.Status.Phase == "Running" {
		return
	}

	job.SetGroupVersionKind(schema.GroupVersionKind{Group: "inference.example.com", Version: "v1alpha1", Kind: "InferenceJob"})
	patch := client.MergeFrom(job.DeepCopyObject().(client.Object))
	job.Status.Phase = "Running"

	if err := p.client.Status().Patch(ctx, &job, patch); err != nil {
		klog.ErrorS(err, "PostBind: failed to patch InferenceJob status", "job", key)
	}
}
