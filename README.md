# SLO-Driven Disaggregated LLM Inference Gang Scheduler

Modern LLM serving splits inference into two phases — **Prefill** (compute-bound, processes the prompt) and **Decode** (memory-bandwidth-bound, generates tokens one by one). Standard Kubernetes gang scheduling uses an all-or-nothing model that cannot express the hierarchical dependency between a prefill gang and a decode gang.

This scheduler introduces a **Minimum Viable Execution** threshold:

```
G_P >= m_P  AND  G_D >= m_D
```

Where `G_P` / `G_D` are the active prefill / decode pods and `m_P` / `m_D` are the minimums declared in the `InferenceJob` CR. Pods are held in the `Permit` phase until both thresholds are satisfied, then released simultaneously.

---

## Prerequisites

- Kubernetes v1.30 cluster
- Go 1.25+
- The `InferenceJob` CRD (`inference.example.com/v1alpha1`) applied to the cluster

Apply the CRD before running the scheduler. The CRD is defined in the `controller-scaling` components:

```bash
kubectl apply -f ../controller-scaling/config/crd/inferencejob_crd.yaml
```

---

## Building

```bash
go mod tidy
go build -o bin/inference-scheduler .
```

---

## Configuration

The scheduler requires a `scheduler-config.yaml` to register the plugin and connect to the API server.

```yaml
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
leaderElection:
  leaderElect: false
clientConnection:
  kubeconfig: "/root/.kube/config"
profiles:
  - schedulerName: inference-scheduler
    plugins:
      preFilter:
        enabled:
          - name: InferenceGangScheduler
      reserve:
        enabled:
          - name: InferenceGangScheduler
      permit:
        enabled:
          - name: InferenceGangScheduler
      postBind:
        enabled:
          - name: InferenceGangScheduler
```

---

## Running

Use `--secure-port=10260` to avoid conflicting with the default `kube-scheduler` running on port `10259`:

```bash
./bin/inference-scheduler \
  --config=scheduler-config.yaml \
  --secure-port=10260 \
  --v=2
```

Both schedulers run side-by-side. Workload routing is determined entirely by the `schedulerName` field on each pod spec.

---

## Scheduling Pipeline

The `InferenceGangScheduler` plugin implements five extension points:

| Extension Point | Responsibility                                                                                                                                       |
| --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `PreFilter`     | Validates pod labels and verifies the `InferenceJob` CR exists. Pods without labels are skipped back to the default scheduler.                       |
| `Reserve`       | Increments the `prefillReady` or `decodeReady` counter for the pod's job using mutex-protected gang state.                                           |
| `Unreserve`     | Decrements counters if a pod fails at a later stage, preventing state corruption or deadlocks.                                                       |
| `Permit`        | Holds pods in `Wait` state (5-minute timeout) until `G_P >= m_P AND G_D >= m_D`. Releases all waiting pods simultaneously when the threshold is met. |
| `PostBind`      | Patches the `InferenceJob` CR's `status.phase` to `Running` after pods are bound to a node.                                                          |

---

## Workload Configuration

This scheduler does not create pods — it intercepts them. To route a Deployment through the custom scheduler, add the following to your pod template:

**Required labels:**

| Label                        | Value                                               |
| ---------------------------- | --------------------------------------------------- |
| `inference.example.com/job`  | Name of the `InferenceJob` CR in the same namespace |
| `inference.example.com/role` | `prefill` or `decode`                               |

**Required scheduler field:**

```yaml
spec:
  schedulerName: inference-scheduler
```

### Example — Prefill Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: llama3-prefill
  namespace: default
spec:
  replicas: 2
  selector:
    matchLabels:
      app: llama3-prefill
  template:
    metadata:
      labels:
        app: llama3-prefill
        inference.example.com/job: my-inference-job
        inference.example.com/role: prefill
    spec:
      schedulerName: inference-scheduler
      containers:
        - name: llm-container
          image: my-registry/vllm-prefill:latest
```

### Example — Decode Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: llama3-decode
  namespace: default
spec:
  replicas: 4
  selector:
    matchLabels:
      app: llama3-decode
  template:
    metadata:
      labels:
        app: llama3-decode
        inference.example.com/job: my-inference-job
        inference.example.com/role: decode
    spec:
      schedulerName: inference-scheduler
      containers:
        - name: llm-container
          image: my-registry/vllm-decode:latest
```

### Example — InferenceJob CR

```yaml
apiVersion: inference.example.com/v1alpha1
kind: InferenceJob
metadata:
  name: my-inference-job
  namespace: default
spec:
  slo:
    maxTTFT: 2000
    maxTPOT: 100
  prometheusEndpoint: "http://prometheus.monitoring.svc:9090"
  ttftQuery: "histogram_quantile(0.99, sum(rate(vllm:time_to_first_token_seconds_bucket[5m])) by (le)) * 1000"
  tpotQuery: "histogram_quantile(0.99, sum(rate(vllm:time_per_output_token_seconds_bucket[5m])) by (le)) * 1000"
  prefillDeployment:
    name: llama3-prefill
  decodeDeployment:
    name: llama3-decode
  minPrefillReplicas: 2
  maxPrefillReplicas: 8
  minDecodeReplicas: 4
  maxDecodeReplicas: 16
```

Pods will remain in `Pending` state (held in `Permit`) until both the prefill and decode counters satisfy the minimums declared in the `InferenceJob` CR. Once satisfied, all pods are released and bound simultaneously.

---

## Project Structure

```
inference-scheduler/
├── main.go                        # Registers plugin, starts scheduler binary
├── scheduler-config.yaml          # Scheduler configuration template
├── go.mod
├── go.sum
└── internal/
    └── plugin/
        └── plugin.go              # PreFilter, Reserve, Unreserve, Permit, PostBind
```

---

## Related Components

This scheduler is one part of a three-component thesis system:

| Component                  | Owner    | Description                                                              |
| -------------------------- | -------- | ------------------------------------------------------------------------ |
| Gang Scheduler (this repo) | Person A | Custom `kube-scheduler` plugin — P/D-aware gang scheduling               |
| SLO Autoscaler             | Person B | Kubernetes Operator — PID controller scaling pods based on TTFT/TPOT     |
| Mock Metric Server         | Person C | Python server — replays DistServe/Splitwise traces to simulate TTFT/TPOT |
