#!/usr/bin/env python3
"""多读端执行体：R 个节点同时读同一份（或不相交分片）数据。

只在 `TRAINCACHE_READERS>1` 时由 run_bench.run_group 分派进来。单读端路径
完全不经过本文件，106 个既有实验点的行为不受影响。

四条不变量：
  1. 一节点一个 Pod（Pod 反亲和），否则同节点的两个 Pod 共用一个 agentd 与
     一份 hostPath 缓存，合并会被进程内去重吃掉。反亲和由 TRAINCACHE_ANTI_AFFINITY
     控制，默认开启；设为 0/false/off 时关闭，让多个读端落同一节点共用一个 agentd，
     用于观察进程内去重对合并效果的影响。
  2. 起跑用绝对时刻栅栏：所有 Pod 等到同一个 epoch 秒再读，启动偏差不进入
     合并窗口。
  3. makespan 取各 Pod 的最大值，字节账取各 Pod 之和；origin egress 是集群级，
     与字节和直接对账。
  4. 网关计数只作诊断，主判据是源站字节与 makespan。
"""
import json
import os
import time

START_DELAY_S = 25.0          # 提交到统一开跑的间隔，容纳 4 个 Pod 启动
GATEWAY_DEPLOY = "traincache-gateway"
ANTI_AFFINITY_ENV = "TRAINCACHE_ANTI_AFFINITY"
ANTI_AFFINITY_OFF = ("0", "false", "off")


def anti_affinity_enabled() -> bool:
    """读开关而不是常量：同一进程内换取值可以对照两种调度效果。"""
    raw = os.environ.get(ANTI_AFFINITY_ENV, "1").strip().lower()
    return raw not in ANTI_AFFINITY_OFF


print(f"[gateway] anti-affinity={'on' if anti_affinity_enabled() else 'off'}",
      flush=True)


def trace_for_pod(rb, trace: dict, index: int, readers: int, pattern: str) -> str:
    """按读法给每个 Pod 生成 trace。

    replicated：四个 Pod 拿同一份 trace，同 key 并发最密。
    sharded：只保留 shard % readers == index 的分片，四个 Pod 读不相交子集。
    """
    if pattern != "sharded":
        return json.dumps(trace, ensure_ascii=False)
    import copy
    t = copy.deepcopy(trace)
    for ep in t["per_epoch"]:
        ep["shard_perm"] = [s for s in ep["shard_perm"] if s % readers == index]
    return json.dumps(t, ensure_ascii=False)


def ensure_configmap(rb, group: str, run_id: str, script: str,
                     trace_by_pod: dict[int, str]) -> str:
    """建一份供全部读端共享的 ConfigMap：measure.py + trace-<i>.json。"""
    import os
    import tempfile
    ns = rb.GROUP_NS[group]
    name = f"measure-script-{run_id}"
    rb.kubectl(["delete", "configmap", name], namespace=ns, timeout=30)
    paths = []
    try:
        with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as f:
            f.write(script)
            paths.append(("measure.py=" + f.name, f.name))
        for i, txt in trace_by_pod.items():
            with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
                f.write(txt)
                paths.append((f"trace-{i}.json=" + f.name, f.name))
        args = ["create", "configmap", name]
        for arg, _ in paths:
            args.append("--from-file=" + arg)
        rc, _, err = rb.kubectl(args, namespace=ns, timeout=30)
    finally:
        for _, p in paths:
            try:
                os.unlink(p)
            except OSError:
                pass
    if rc != 0:
        raise SystemExit(f"[multi] ConfigMap 创建失败: {err[:300]}")
    return name


def _pod_spec(rb, group, pod_name, cm_name, pod_index, readers, start_at,
              t_submit, ns):
    """traincache-cold 的多读端 Pod 规格。与单读端同一份 measure 脚本。"""
    container = {
        "name": "train",
        "image": rb.TRAIN_IMAGE,
        "imagePullPolicy": "IfNotPresent",
        "command": ["python3", "/scripts/measure.py"],
        "env": [
            {"name": "TRAINCACHE_JOB_CREATED_AT", "value": str(t_submit)},
            {"name": "TRAINCACHE_GROUP", "value": group},
            {"name": "TRAINCACHE_TRACE_PATH",
             "value": f"/scripts/trace-{pod_index}.json"},
            {"name": "COMPUTE_MS", "value": str(rb.COMPUTE_MS)},
            {"name": "READ_WORKERS", "value": str(rb.READ_WORKERS)},
            {"name": "READ_QUEUE_DEPTH", "value": str(rb.READ_QUEUE_DEPTH)},
            {"name": "READ_TIMEOUT_S", "value": str(rb.READ_TIMEOUT_S)},
            {"name": "TRAINCACHE_START_AT", "value": str(start_at)},
            {"name": "TRAINCACHE_POD_INDEX", "value": str(pod_index)},
            {"name": "TRAINCACHE_READERS", "value": str(readers)},
            {"name": "POD_UID", "valueFrom": {"fieldRef":
                                              {"fieldPath": "metadata.uid"}}},
            {"name": "TRAINCACHE_DATASET", "value": rb.DATASET_NAME},
            {"name": "TRAINCACHE_CHUNK_SIZE", "value": str(rb.CHUNK_SIZE)},
            {"name": "TRAINCACHE_PREFETCH_K", "value": str(rb.PREFETCH_K)},
            {"name": "TRAINCACHE_HOOK_SOCK", "value": "/run/tc-hook/agent.sock"},
        ],
        "volumeMounts": [
            {"name": "script", "mountPath": "/scripts", "readOnly": True},
            {"name": "cache-root", "mountPath": "/var/cache/traincache",
             "readOnly": True},
            {"name": "orders-dir", "mountPath": "/run/traincache/orders"},
            {"name": "hook-dir", "mountPath": "/run/tc-hook"},
            {"name": "ordersrc", "mountPath": "/app/ordersrc.py",
             "subPath": "ordersrc.py"},
        ],
        "resources": {"requests": {"cpu": "500m", "memory": "1Gi"},
                      "limits": {"cpu": "2", "memory": "4Gi"}},
    }
    spec = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": pod_name, "namespace": ns,
                     "labels": {"app": "train-job"}},
        "spec": {
            "restartPolicy": "Never",
            "containers": [container],
            "volumes": [
                {"name": "script", "configMap": {"name": cm_name}},
                {"name": "cache-root",
                 "hostPath": {"path": "/var/cache/traincache",
                              "type": "Directory"}},
                {"name": "orders-dir",
                 "hostPath": {"path": "/run/traincache/orders",
                              "type": "DirectoryOrCreate"}},
                {"name": "hook-dir",
                 "hostPath": {"path": "/run/traincache",
                              "type": "DirectoryOrCreate"}},
                {"name": "ordersrc",
                 "configMap": {"name": "traincache-ordersrc"}},
            ],
        },
    }
    # 一节点一读端：同节点会共用一个 agentd，合并被进程内去重吃掉。关闭时整段
    # affinity 都不下发，改动其它调度字段会改变已跑过的对照点。
    if anti_affinity_enabled():
        spec["spec"]["affinity"] = {"podAntiAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": [{
                "labelSelector": {"matchLabels": {"app": "train-job"}},
                "topologyKey": "kubernetes.io/hostname",
            }]}}
    return spec


def create_pods(rb, group, run_id, trace, readers, pattern, start_at, t_submit):
    """建 R 个 Pod，返回名字列表。全部在 start_at 之前创建完即可（栅栏吸收偏差）。"""
    import tempfile
    ns = rb.GROUP_NS[group]
    script = rb.build_measure_script(group)
    traces = {i: trace_for_pod(rb, trace, i, readers, pattern)
              for i in range(readers)}
    cm = ensure_configmap(rb, group, run_id, script, traces)
    names = []
    for i in range(readers):
        name = f"train-job-{i}"
        rb.kubectl(["delete", "pod", name, "--grace-period=0", "--force"],
                   namespace=ns, timeout=30)
        spec = _pod_spec(rb, group, name, cm, i, readers, start_at, t_submit, ns)
        import yaml
        with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
            f.write(yaml.safe_dump(spec, default_flow_style=False))
            path = f.name
        rc, _, err = rb.kubectl(["apply", "-f", path], namespace=ns, timeout=30)
        import os
        os.unlink(path)
        if rc != 0:
            raise SystemExit(f"[multi] Pod {name} 创建失败: {err[:300]}")
        names.append(name)
    return names


def restart_gateway(rb):
    """每轮网关臂前重启一次：清在途表，让 60s 汇总日志从零开始。"""
    ns = "traincache"
    rb.kubectl(["rollout", "restart", f"deployment/{GATEWAY_DEPLOY}"],
               namespace=ns, timeout=60)
    rc, out, err = rb.kubectl(["rollout", "status", f"deployment/{GATEWAY_DEPLOY}",
                               "--timeout=120s"], namespace=ns, timeout=150)
    if rc != 0:
        raise SystemExit(f"[multi] 网关重启失败: {err[:300]}")


def gateway_counters(rb):
    """累加网关 60s 汇总日志。返回 dict 与覆盖的完整窗口秒数。"""
    rc, out, _ = rb.kubectl(["logs", f"deployment/{GATEWAY_DEPLOY}",
                             "--since=30m"], namespace="traincache", timeout=60)
    keys = ("gets", "merged", "owners", "passthrough", "kicked",
            "rejected", "bytes")
    total = {k: 0 for k in keys}
    windows = 0
    if rc != 0:
        return total, windows
    for line in out.splitlines():
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if rec.get("msg") != "gateway: 60s 汇总":
            continue
        windows += 1
        for k in keys:
            total[k] += int(rec.get(k) or 0)
    return total, windows


def aggregate(pod_logs: dict[str, str]) -> dict | None:
    """把 R 份 RESULT 合成一行：makespan 取 max，字节账取和。"""
    parsed = {}
    for name, logs in pod_logs.items():
        one = None
        # 复用 run_bench 的解析器在调用侧传进来更麻烦，这里只做同一个约定。
        for line in logs.splitlines():
            if not line.startswith("RESULT "):
                continue
            kv = {}
            for tok in line[len("RESULT "):].split():
                if "=" in tok:
                    k, v = tok.split("=", 1)
                    kv[k] = v
            if "total_makespan" in kv and "startup_makespan" in kv:
                one = kv
        if one is None:
            return None
        parsed[name] = one

    def num(kv, k, default=0.0):
        try:
            return float(kv.get(k, ""))
        except ValueError:
            return default

    out = {}
    totals = [num(kv, "total_makespan") for kv in parsed.values()]
    starts = [num(kv, "startup_makespan") for kv in parsed.values()]
    out["total_makespan"] = max(totals)
    out["startup_makespan"] = max(starts)
    out["pod_makespan_max"] = max(totals)
    out["pod_makespan_min"] = min(totals)
    # stall：按步数加权平均；p90 取最差
    n = sum(int(num(kv, "stall_n")) for kv in parsed.values())
    if n:
        out["stall_ratio"] = sum(num(kv, "stall_ratio") * int(num(kv, "stall_n"))
                                 for kv in parsed.values()) / n
        out["stall_p90"] = max(num(kv, "stall_p90") for kv in parsed.values())
    else:
        out["stall_ratio"] = ""
        out["stall_p90"] = ""
    out["stall_n"] = n
    for k in ("read_bytes_startup", "read_bytes_steady", "read_bytes_switch",
              "demand_miss_bytes_startup", "demand_miss_bytes_steady",
              "demand_miss_bytes_switch", "total_read_bytes", "reads",
              "hits", "misses", "queue_timeouts", "read_errors"):
        out[k] = sum(int(num(kv, k)) for kv in parsed.values())
    out["peak_resident_bytes"] = max(
        int(num(kv, "peak_resident_bytes")) for kv in parsed.values())
    out["compute_actual_ms"] = max(num(kv, "compute_actual_ms")
                                   for kv in parsed.values())
    rb_read = out["total_read_bytes"]
    rb_miss = sum(out[f"demand_miss_bytes_{p}"]
                  for p in ("startup", "steady", "switch"))
    out["hit_rate"] = round(max(0.0, 1.0 - rb_miss / rb_read), 4) if rb_read else ""
    out["hit_rate_diag"] = out["hit_rate"]
    out["measure_void"] = max(int(num(kv, "void")) for kv in parsed.values())
    out["snapshot_missing"] = max(int(num(kv, "snapshot_missing"))
                                  for kv in parsed.values())
    # 栅栏偏差：Pod 内打印的 go 时刻与本轮统一开跑时刻之差
    skews = []
    for logs in pod_logs.values():
        for line in logs.splitlines():
            if line.startswith("[BARRIER]"):
                for tok in line.split():
                    if tok.startswith("skew_ms="):
                        try:
                            skews.append(float(tok.split("=", 1)[1]))
                        except ValueError:
                            pass
    out["barrier_skew_ms"] = max(skews) if skews else ""
    out["readers_ok"] = len(parsed)
    return out


def run_group_multi(rb, group, reps, out_csv, trace_json, trace_sha,
                    substitute_for=""):
    """多读端跑分主循环。只支持 traincache-cold（网关实验的唯一组态）。"""
    if group != "traincache-cold":
        raise SystemExit(f"[multi] 多读端只支持 traincache-cold，收到 {group}")
    if rb.READERS > 4:
        raise SystemExit("[multi] readers 最多 4（kind 只有 4 个 worker）")
    import threading
    trace = json.loads(trace_json)
    results = []
    rb.cold_reset()
    rb.reset_traincache_agent()
    ok, _, _ = rb.group_up(group)
    if not ok:
        return rb.stamp_rows(
            [{"group": group, "rep": r, "void": True, "reason": "up_failed",
              "warmup_seconds": 0.0, "trace_sha256": trace_sha}
             for r in range(reps)], group, substitute_for)

    for rep in range(reps):
        print(f"\n{'='*60}\n[multi] group={group} rep={rep+1}/{reps} "
              f"readers={rb.READERS} pattern={rb.ACCESS_PATTERN} "
              f"gateway={int(rb.GATEWAY_ON)}\n{'='*60}", flush=True)
        if rep:
            rb.rep_cold_reset(group)
            rb.reset_traincache_agent()
        if rb.GATEWAY_ON:
            restart_gateway(rb)

        base_snap = rb.fetch_snapshot()
        cache_base = rb.cache_counter_snapshot()
        guard_base = rb.eviction_guard_snapshot()
        origin_base = rb.minio_egress_snapshot()
        samples = [base_snap] if base_snap else []
        dir_peaks = []
        stop = threading.Event()

        def _sample():
            while not stop.wait(rb.SAMPLE_INTERVAL_S):
                snap = rb.fetch_snapshot()
                if stop.is_set():
                    break
                if snap is not None:
                    samples.append(snap)
                dir_peaks.append(rb.blocks_dir_peak_bytes())

        th = threading.Thread(target=_sample, daemon=True)
        th.start()

        t_submit = time.time()
        start_at = t_submit + START_DELAY_S
        try:
            pods = create_pods(rb, group, f"{rep:02d}", trace, rb.READERS,
                               rb.ACCESS_PATTERN, start_at, t_submit)
        except SystemExit as exc:
            stop.set()
            results.append({"group": group, "rep": rep, "void": True,
                            "reason": "pod_create_failed",
                            "warmup_seconds": 0.0, "trace_sha256": trace_sha})
            print(exc, flush=True)
            continue

        ready = {}
        ns = rb.GROUP_NS[group]
        logs_by_pod = {}
        success = True
        for name in pods:
            def _tick(n=name):
                if n not in ready:
                    ts = rb.pod_ready_epoch(ns, n)
                    if ts is not None:
                        ready[n] = ts
            ok, logs = rb.wait_pod_complete(ns, name,
                                            timeout=rb.pod_timeout_s(),
                                            on_tick=_tick, tick_s=5)
            logs_by_pod[name] = logs
            success = success and ok
        stop.set()
        th.join(timeout=10)
        origin_final = rb.minio_egress_snapshot()

        if not success:
            results.append({"group": group, "rep": rep, "void": True,
                            "reason": "pod_failed", "warmup_seconds": 0.0,
                            "trace_sha256": trace_sha})
            continue

        metrics = aggregate(logs_by_pod)
        if metrics is None:
            results.append({"group": group, "rep": rep, "void": True,
                            "reason": "parse_failed", "warmup_seconds": 0.0,
                            "trace_sha256": trace_sha})
            continue

        metrics.update({
            "group": group, "rep": rep, "run_id": f"{group}-r{rep:02d}",
            "epochs": rb.EPOCHS, "shuffle": rb.TRACE_SHUFFLE,
            "dataset_bytes": rb.N_TARS * rb.SHARD_SIZE_MB * 1024 * 1024,
            "prefetch_lookahead": rb.AGENT_PREFETCH_K,
            "evict_policy": rb.EVICT_POLICY, "gate_mode": rb.GATE_MODE,
            "data_source": rb.DATA_SOURCE, "warmup_seconds": 0.0,
            "warmup_time_basis": "none", "trace_sha256": trace_sha,
            "agent_degraded_max": "",
            "readers": rb.READERS, "access_pattern": rb.ACCESS_PATTERN,
        })

        final_snap = rb.fetch_snapshot()
        cache_final = rb.cache_counter_snapshot()
        guard_final = rb.eviction_guard_snapshot()
        ready_ts = max(ready.values()) if ready else None
        if ready_ts is not None:
            metrics["t_pod_ready_s"] = round(ready_ts - t_submit, 3)
            metrics["post_pod_s"] = round(
                metrics["total_makespan"] - metrics["t_pod_ready_s"], 3)
        else:
            metrics["t_pod_ready_s"] = ""
            metrics["post_pod_s"] = ""
        origin_egress = None
        if origin_base is not None and origin_final is not None:
            origin_egress = origin_final["total"] - origin_base["total"]
            if origin_egress < 0:
                origin_egress = origin_final["total"]

        metrics = rb.reconcile_metrics(
            group, metrics, base_snap, samples, final_snap,
            rb.parse_epoch_boundaries(logs_by_pod.get(pods[0], "")),
            cache_base=cache_base, cache_final=cache_final,
            pre_up_snap=None, origin_egress=origin_egress,
            guard_base=guard_base, guard_final=guard_final,
            dir_peaks=list(dir_peaks))

        gw, windows = gateway_counters(rb) if rb.GATEWAY_ON else (
            {k: "" for k in ("gets", "merged", "owners", "passthrough",
                             "kicked", "rejected", "bytes")}, 0)
        metrics.update({
            "gateway_gets": gw["gets"], "gateway_merged": gw["merged"],
            "gateway_owners": gw["owners"],
            "gateway_passthrough": gw["passthrough"],
            "gateway_kicked": gw["kicked"],
            "gateway_rejected": gw["rejected"],
            "gateway_bytes": gw["bytes"], "gateway_windows": windows,
        })
        metrics.setdefault("void", False)
        if not metrics.get("void"):
            reason = rb.measure_void_reason(metrics)
            if reason:
                metrics["void"] = True
                metrics["reason"] = reason

        print(f"[multi] {group} rep={rep} 结果: "
              f"total={metrics['total_makespan']:.2f}s "
              f"bytes={metrics.get('total_read_bytes')} "
              f"gw_merged={metrics['gateway_merged']}", flush=True)
        results.append(metrics)

    rb.group_down(group)
    return rb.stamp_rows(results, group, substitute_for)
