English | [简体中文](README.zh-CN.md)

# TrainCache

TrainCache runs on Kubernetes. It fetches dataset blocks into node-local disk before a Pod is scheduled, uses warmup progress to decide which node the Pod is placed on, and keeps prefetching the blocks training reads next.

[![verify](https://github.com/TheWor1d877/TrainCache/actions/workflows/verify.yml/badge.svg?branch=main)](https://github.com/TheWor1d877/TrainCache/actions/workflows/verify.yml) ![Go 1.26](https://img.shields.io/badge/Go-1.26-00ADD8?logo=go&logoColor=white) ![Kubernetes 1.28+](https://img.shields.io/badge/Kubernetes-1.28%2B-326CE5?logo=kubernetes&logoColor=white) ![S3 compatible](https://img.shields.io/badge/S3-compatible-569A31?logo=amazons3&logoColor=white) ![License Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-4C8EDA)

<img src="docs/assets/logo/traincache-lockup.svg" alt="TrainCache" width="420">

```bash
make e2e    # create a kind cluster, load images, run six assertions
```

![Figure 1: resources after e2e](docs/assets/screenshots/e2e-demo_v0.4.1.gif)

*Figure 1: resources queried after `make e2e`. The WarmupTask is Released, both training Pods sit on nodes labeled `traincache.io/s3-access=true`, and the Pod annotation `node-scores` reads 100.*

## Background

Fluid 1.1.0-alpha.9 runs DataLoad apart from scheduling: a Pod enters scheduling right after creation, and warmup progress plays no part in node selection ([field survey](docs/3-field-survey.md) §2.2). JuiceFS Community Edition ships a local cache only; distributed cache groups, IORPC and RDMA are Enterprise features (ibid. §0). A training Pod stays on one node from epoch 0 to the end. Epoch and shuffle switches happen inside the Pod and trigger no rescheduling, so the blocks written at startup stop matching what later epochs read, and no component fetches ahead of the reader.

## Where it applies

TrainCache targets training workloads where data has to be fetched from remote storage, the consumption order can be computed ahead of time, and fetch bandwidth is the bottleneck. It pulls blocks onto the node's local disk before the Pod is scheduled, uses block-level readiness to decide which node the Pod lands on, and keeps prefetching later blocks in the order training consumes them.

### Workload classes

| Workload | Dataset size | Consumption pattern | Where TrainCache sits |
|---|---|---|---|
| Large-scale pretraining | TB to PB | Mostly consumed once | Disadvantage region: the working set does not fit the node cache, reuse ratio is low |
| Intermediate training, large-scale fine-tuning | Hundreds of GB to a few TB | Consumed repeatedly across runs and epochs | Advantage region |
| Task-specific fine-tuning, domain training | A few GB to hundreds of GB | Repeated iterations | Edge of the advantage region: reuse from the iterations offsets part of the fixed system cost |
| Streaming, online data | No fixed total | Consumption order cannot be computed | Not applicable: neither order-based prefetch nor the gate can be set up from the Job spec |

### Conditions for the advantage region

Four conditions hold at once:

1. Data has to be fetched from remote storage. The local disk cannot hold the whole working set, or the cold start has no local copy.
2. The consumption order can be computed ahead of time. Epoch, shuffle seed and batch size are known from the Job spec.
3. Fetch bandwidth is the bottleneck. Going straight to the data source stalls the trainer when it reads a block.
4. The same data is consumed repeatedly: across epochs, across runs, and across cold starts on different nodes.

Two benefit paths cover different dataset sizes:

- **Reuse**: the dataset size (basis: total bytes of the whole dataset) falls between a fraction and a few multiples of the cache space available per node (basis: per node, taken as 8 to 12 TB), that is hundreds of GB to a few TB. The node cache holds most of the working set, so repeated consumption no longer goes back to remote storage.
- **Overlap**: when the working set exceeds the node cache, order-based prefetch makes the download and the compute run at the same time. The gain comes from that, and the cache reuse ratio is low there. The current experiments cover this path; see "Known limitations" for their scale.

One order of magnitude more data pushes the reuse ratio down and leaves only the overlap from order-based prefetch; one order of magnitude less data leaves the fixed cost of the controller, the gate, agentd, the order sidecar and the hook instrumentation above the gain.

### Conditions for the disadvantage region

- Large-scale pretraining with mostly single-pass consumption: the working set does not fit the node cache, the data is not consumed twice, and the cache reuse ratio stays low.
- Streaming and online data: the consumption order cannot be computed ahead of time, so neither order-based prefetch nor the gate can be set up from the Job spec.
- The node already has a copy: the image ships the data, or another job on the same node left a usable cache, which makes warmup a redundant action.
- The dataset is far larger than the cache space of the whole cluster: the cache only serves as a streaming buffer and the reuse ratio approaches zero.

## Features

- After a `WarmupTask` is submitted, the webhook injects the `traincache.io/warmup` SchedulingGate into matching training Pods, which stay Pending. The controller requests blocks from candidate nodes per Pod shard and removes the gate once block readiness reaches the threshold `M`.
- If readiness is still below `M` after `t_max`, the controller removes the gate and the Pod goes through default scheduling ([metrics](docs/4.3-bench-metrics-and-decisions.md)).
- The scheduler plugin reads the Pod annotation `traincache.io/node-scores`, scores candidate nodes by readiness with global position decay, and picks the highest score.
- Once the Pod is scheduled, agentd prefetches the next blocks in the order training consumes them. When an epoch, shuffle or curriculum switch arrives, agentd prefetches in the new order (`agent/ordersrc/`).
- Bytes fetched to candidate nodes during warmup are counted in `startup_warmup_bytes`; bytes fetched during sustained prefetch are counted in `sustained_prefetch_bytes`. Every configuration in a cell shares these definitions ([metrics](docs/4.3-bench-metrics-and-decisions.md) §2).

## Quick start

Requirements: Kubernetes 1.28+, kubectl, openssl and an S3-compatible bucket. Release artifacts target linux/amd64; e2e passes on `kindest/node:v1.37.0`.

**1. Install the control plane.** Five images are pinned by digest:

```bash
curl -LO https://github.com/TheWor1d877/TrainCache/releases/download/v0.4.1/install.yaml
kubectl apply -f install.yaml
```

**2. Webhook certificate.** `caBundle` in the manifest is empty. Generate a server certificate and patch it in:

```bash
NS=traincache; SVC=traincache-webhook
TLS=$(mktemp -d)
openssl req -x509 -newkey rsa:2048 -nodes -days 365 -keyout "$TLS/ca.key" -out "$TLS/ca.crt" -subj "/CN=traincache-ca"
openssl req -newkey rsa:2048 -nodes -keyout "$TLS/tls.key" -out "$TLS/tls.csr" -subj "/CN=$SVC"
printf 'subjectAltName=DNS:%s,DNS:%s.%s.svc.cluster.local\n' "$SVC" "$SVC" "$NS" >"$TLS/san.ext"
openssl x509 -req -in "$TLS/tls.csr" -CA "$TLS/ca.crt" -CAkey "$TLS/ca.key" -CAcreateserial -out "$TLS/tls.crt" -days 365 -extfile "$TLS/san.ext"
kubectl -n "$NS" create secret tls traincache-webhook-tls --cert="$TLS/tls.crt" --key="$TLS/tls.key"
kubectl patch mutatingwebhookconfiguration traincache-pod-warmup --type=strategic \
  -p "{\"webhooks\":[{\"name\":\"pods.traincache.io\",\"clientConfig\":{\"caBundle\":\"$(base64 -w0 "$TLS/ca.crt")\"}},{\"name\":\"pods-checkpoint.traincache.io\",\"clientConfig\":{\"caBundle\":\"$(base64 -w0 "$TLS/ca.crt")\"}}]}"
```

The certificate is valid for 365 days. The SAN covers the Service name and the DNS name inside the namespace. The CA goes into both webhook entries: the checkpoint entry has `failurePolicy: Fail`, so a Pod labeled `traincache.io/checkpoint` (the label value names a ConfigMap in the same namespace) is rejected when its `caBundle` is empty.

**3. Bucket and credentials.** `DataSet.spec` takes `endpoint` (no scheme), `bucket` and `secretRef`. The Secret holds `accessKey` and `secretKey` and lives in the same namespace as the DataSet. For bucket setup see [cloud object storage deployment](docs/0-cloud-object-storage-deployment.md).

**4. Verify the control plane.**

```bash
kubectl -n traincache rollout status deploy/traincache-controller-manager --timeout=300s
kubectl -n traincache get pods
```

```text
NAME                                             READY   STATUS
traincache-agentd-<hash>                         1/1     Running
traincache-controller-manager-<hash>             1/1     Running
traincache-gateway-<hash>                        1/1     Running
traincache-webhook-<hash>                        1/1     Running
```

The scheduler runs in `kube-system`; check it with `kubectl -n kube-system rollout status deploy/traincache-scheduler`. A sample training Pod is in `agent/deploy/train-pod.yaml`; `DataSet` and `WarmupTask` fields are in [4.4 runbook](docs/4.4-bench-runbook.md).

Uninstall:

```bash
kubectl delete -f install.yaml
```

## Results

Seven configurations: direct, JuiceFS cold/hot, Fluid cold/hot and TrainCache cold/hot, three runs per workload shape. Values are means; bootstrap 95% intervals are in the `ci_low` and `ci_high` columns of the CSV. Data version `v0.4.0`; aggregates used in the figures are in [`compare_total_v0.4.0.csv`](docs/assets/data/compare_total_v0.4.0.csv). L-1000 has a single run.

| Cell | direct | Fluid cold | TrainCache cold | TrainCache hot | n |
|---|---:|---:|---:|---:|---:|
| S-100 | 576.7 | 352.0 | 412.9 | 349.8 | 3 |
| S-300 | 297.0 | 255.8 | 302.4 | — | 3 |
| S-1000 | 258.4 | 251.6 | 272.7 | 264.4 | 3 |
| M-1000 | 83.4 | 122.6 | 106.8 | — | 3 |
| L-1000 | — | 302.4 | 287.9 | — | 1 |

- S-100 (100 mbit, 2 GiB): TrainCache cold takes 163.7 s less than direct (28.4%), Fluid cold 60.9 s less than TrainCache cold. TrainCache hot runs 349.8 s.
- Stall share in the same cell drops from 52.9% (direct) to 0.1% (TrainCache cold). Cold warmup took 124.3 s, and the total still beats direct.
- At 1000 mbit, gated warmup is net overhead: S-1000 goes from 258.4 s to 272.7 s (5.6%), M-1000 from 83.4 s to 106.8 s (28.1%).
- After a switch, stall recovery lands between 2.24 s and 2.36 s across six cells. The 0.11 s spread (4.9%) stays below the 20% criterion in the protocol.

![Figure 2: makespan comparison](docs/assets/figures/compare_total_v0.4.0.en.png)

*Figure 2: full-run makespan for five cells and seven configurations. Bars are configuration means; shorter is better.*

![Figure 3: startup gating surface](docs/assets/figures/fig1_startup_surface_v0.4.0.en.png)

*Figure 3: makespan change from startup gating against free scheduling, split by fanout. Blue bars to the right mean gating is faster, orange bars to the left mean slower. The −443.9% cell is clipped at the axis end and marked `◀`.*

![Figure 4: makespan by bandwidth and epoch count](docs/assets/figures/fig2_total_makespan_v0.4.0.en.png)

*Figure 4: full-run makespan at three bandwidths and 1, 3 and 6 epochs. Measured values sit above the bars; the y-axis is log scale.*

![Figure 5: stall recovery after a switch](docs/assets/figures/fig3_recovery_v0.4.0.en.png)

*Figure 5: stall recovery after a shuffle × epoch switch, six cells. Small dots are single switch events, large dots are means, and values are labeled on the right.*

## Architecture

![Figure 6: control plane, nodes and object storage](docs/assets/figures/architecture_v0.4.1.en.svg)

*Figure 6: control plane, node plane and object storage.*

The control plane issues warmup tasks, gate releases and node scores, and stays out of the data path. agentd runs on every node and writes blocks to node-local disk; training Pods mount that directory read-only through hostPath, so reads never pass through agentd. Read-side signals come from a DataLoader hook and an inotify side channel.

Blocks come from one of two paths. With `gatewayEndpoint` empty, agentd sends every request to object storage. Once the field is set, agentd sends block GETs to the gateway, which merges duplicate requests into one origin fetch. The gateway is a single-replica Deployment listening on `:9413`; the contract and trade-offs are in [9.1 design](docs/9.1-design.md).

## Relationship to upstream

Fluid 1.1.0-alpha.9 and JuiceFS CE 1.3.0 were deployed and benchmarked on a kind cluster as control groups among the seven configurations ([field survey](docs/3-field-survey.md), [4.1 apparatus](docs/4.1-bench-apparatus.md)). The `DataSet` and `Runtime` naming and the nodeAffinity injection follow Fluid. Block readiness lives in `controller/cmd/controller-manager`, SchedulingGate injection in `controller/cmd/webhook`, node scoring in `controller/cmd/scheduler`, and prefetch in `agent/cmd/agentd`. No upstream code is vendored. Both projects are Apache-2.0: [fluid-cloudnative/fluid](https://github.com/fluid-cloudnative/fluid), [juicedata/juicefs](https://github.com/juicedata/juicefs).


## Build and test

```bash
make verify           # build, vet, go test -race and gofmt for three modules
make verify-metadata  # version fields, image names, relative doc links
hack/build-images.sh  # development images (:dev)
make e2e              # six assertions in a kind cluster
```

`make e2e` covers six assertions: webhook gate injection, warmup progress and gate release, Pod binding to the custom scheduler, `node-scores` annotation parsing, the scheduler reading that annotation, and the `NodeDeviation` event. The `verify` workflow in CI runs the same unit tests plus the MinIO integration test.

## Documents

Documents under `docs/` are written in Chinese.

| Document | Contents |
|---|---|
| [1 Introduction and background](docs/1-introduction-and-background.md) | Problem statement, external facts, positioning against Fluid and JuiceFS |
| [2 Three-layer architecture](docs/2-three-layer-architecture.md) | Control plane, node plane, data sources and data-plane conventions |
| [3 Field survey](docs/3-field-survey.md) | How Fluid and JuiceFS work, and the gaps found |
| [4.0 Bench guide](docs/4.0-bench-guide.md) | Division of work across 4.1–4.5 and standing commands |
| [4.1 Apparatus and variable space](docs/4.1-bench-apparatus.md) | Rig, ρ ladder, parameters, data generation and locations |
| [4.2 Families and grids](docs/4.2-bench-families.md) | Nine experiment families, cell matrices, COND contract, machine-hour account |
| [4.3 Metrics and decisions](docs/4.3-bench-metrics-and-decisions.md) | Metric formulas, pass criteria, numbered decisions |
| [4.4 Runbook](docs/4.4-bench-runbook.md) | Environment, startup, data preparation, running a family, troubleshooting, artifacts |
| [4.5 Version log](docs/4.5-experiment-version-log.md) | Version × family artifact records |
| [5.x Cache core library](docs/5.1-design.md) | Requirements, plan, decisions and tests |
| [6.x Agent service](docs/6.1-design.md) | Node-plane interfaces and decisions |
| [7.x Controller and Scheduler](docs/7.1-design.md) | Control-plane interfaces and decisions |
| [8.x Logging and monitoring](docs/8.1-design.md) | Metrics, log contract and usage |
| [9.x Merge gateway](docs/9.1-design.md) | Gateway contract, decisions and tests |
| [Release constraints](docs/RELEASE-CONSTRAINTS.md) | Versions, images, artifacts and CI rules |

## License

Apache-2.0, see [LICENSE](LICENSE).
