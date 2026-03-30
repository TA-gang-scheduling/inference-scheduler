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
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
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
	mu           sync.Mutex
	prefillReady int32
	decodeReady  int32
}

type InferenceSchedulerPlugin struct {
	handle framework.Handle
	client client.Client

	mu     sync.Mutex
	groups map[string]*gangState
}

// Compile-time interface checks (Unreserve is part of ReservePlugin in v1.30)
var _ framework.PreFilterPlugin = &InferenceSchedulerPlugin{}
var _ framework.ReservePlugin = &InferenceSchedulerPlugin{}
var _ framework.PermitPlugin = &InferenceSchedulerPlugin{}
var _ framework.PostBindPlugin = &InferenceSchedulerPlugin{}

// ─────────────────────────────────────────────
// Constructor
// ─────────────────────────────────────────────

func New(_ context.Context, _ runtime.Object, h framework.Handle) (framework.Plugin, error) {
	cfg, err := config.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("getting kubeconfig: %w", err)
	}

	scheme := runtime.NewScheme()
	GroupVersion := schema.GroupVersion{Group: "inference.example.com", Version: "v1alpha1"}
	scheme.AddKnownTypes(GroupVersion, &InferenceJob{}, &InferenceJobList{})

	c, err := client.New(cfg, client.Options{Scheme: scheme})
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

// ─────────────────────────────────────────────
// PreFilter
// ─────────────────────────────────────────────

func (p *InferenceSchedulerPlugin) PreFilter(ctx context.Context, _ *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
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

func (p *InferenceSchedulerPlugin) Reserve(ctx context.Context, _ *framework.CycleState, pod *v1.Pod, _ string) *framework.Status {
	jobName := pod.Labels[LabelJob]
	role := pod.Labels[LabelRole]
	groupKey := pod.Namespace + "/" + jobName

	p.mu.Lock()
	g, ok := p.groups[groupKey]
	if !ok {
		g = &gangState{}
		p.groups[groupKey] = g
	}
	p.mu.Unlock()

	g.mu.Lock()
	if role == RolePrefill {
		g.prefillReady++
	} else if role == RoleDecode {
		g.decodeReady++
	}
	g.mu.Unlock()

	return framework.NewStatus(framework.Success)
}

func (p *InferenceSchedulerPlugin) Unreserve(ctx context.Context, _ *framework.CycleState, pod *v1.Pod, _ string) {
	jobName := pod.Labels[LabelJob]
	role := pod.Labels[LabelRole]
	groupKey := pod.Namespace + "/" + jobName

	p.mu.Lock()
	g, ok := p.groups[groupKey]
	p.mu.Unlock()

	if ok {
		g.mu.Lock()
		if role == RolePrefill && g.prefillReady > 0 {
			g.prefillReady--
		} else if role == RoleDecode && g.decodeReady > 0 {
			g.decodeReady--
		}
		g.mu.Unlock()
	}
}

// ─────────────────────────────────────────────
// Permit
// ─────────────────────────────────────────────

func (p *InferenceSchedulerPlugin) Permit(ctx context.Context, _ *framework.CycleState, pod *v1.Pod, _ string) (*framework.Status, time.Duration) {
	jobName := pod.Labels[LabelJob]
	groupKey := pod.Namespace + "/" + jobName

	var job InferenceJob
	if err := p.client.Get(ctx, types.NamespacedName{Name: jobName, Namespace: pod.Namespace}, &job); err != nil {
		return framework.NewStatus(framework.UnschedulableAndUnresolvable, err.Error()), 0
	}

	minPrefill := job.Spec.MinPrefillReplicas
	if minPrefill < 1 { minPrefill = 1 }
	minDecode := job.Spec.MinDecodeReplicas
	if minDecode < 1 { minDecode = 1 }

	p.mu.Lock()
	g, ok := p.groups[groupKey]
	if !ok {
		g = &gangState{}
		p.groups[groupKey] = g
	}
	p.mu.Unlock()

	g.mu.Lock()
	currentPrefill := g.prefillReady
	currentDecode := g.decodeReady
	g.mu.Unlock()

	// Condition met: release all waiting pods for this job
	if currentPrefill >= minPrefill && currentDecode >= minDecode {
		klog.InfoS("Permit: gang released", "job", groupKey, "prefillReady", currentPrefill, "decodeReady", currentDecode)
		
		p.handle.IterateOverWaitingPods(func(wp framework.WaitingPod) {
			if wp.GetPod().Labels[LabelJob] == jobName {
				wp.Allow(Name)
			}
		})
		
		// Reset counters for the next wave
		g.mu.Lock()
		g.prefillReady = 0
		g.decodeReady = 0
		g.mu.Unlock()
		
		return framework.NewStatus(framework.Success), 0
	}

	klog.V(2).InfoS("Permit: holding pod", "pod", klog.KObj(pod), "job", groupKey)
	return framework.NewStatus(framework.Wait), PermitTimeout
}

// ─────────────────────────────────────────────
// PostBind
// ─────────────────────────────────────────────

func (p *InferenceSchedulerPlugin) PostBind(ctx context.Context, _ *framework.CycleState, pod *v1.Pod, _ string) {
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
