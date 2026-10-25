#!/usr/bin/env python3
"""M1 离线采集器:把 run.json + K8s 状态 + Prometheus 计数器汇成 metrics.csv。

口径(预注册 §1.1/§1.4/§1.5):
  * makespan 两条边界全部来自 run.json(t_submit/t_first_step/t_last_step);
    t_pod_ready 从 K8s Pod condition 取,只做拆分,不改主口径;
  * 三笔字节成本 = 启动窗口、steady 窗口、每个 switch 窗口的计数器增量。
    fetch_bytes_total 按 Agent instance 求和,fanout 的重复下载已包含在
    多副本计数器里;另用 WarmupTask 的 startupWindowReady 给出独立估算;
  * 命中率用 hits/misses 计数器按 phase 求增量,ratio gauge 只做对账;
  * 所有边界误差单独报,不静默吞掉。

用法:
  python3 bench/experiments/collect.py --run-dir bench/results/track_d/<run> \
      --prometheus-url http://127.0.0.1:9090 \
      --namespace traincache --warmup-task warmup-xxx --pod-selector job-name=train-xxx

只依赖标准库;Prometheus / kubectl 不可用时写 null 并在 notes 说明,不伪造数据。
"""
import argparse
import csv
import json
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path


def parse_ts(value):
    """RFC3339 / Prometheus 秒时间戳 → aware UTC datetime;空值返回 None。"""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    text = str(value).strip()
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def fmt_ts(dt):
    if dt is None:
        return ""
    return dt.isoformat(timespec="microseconds").replace("+00:00", "Z")


def secs(a, b):
    if a is None or b is None:
        return None
    return round((b - a).total_seconds(), 3)


def prom_query_range(base, query, start, end, step_s=5):
    """Prometheus range query → data.result;失败抛 RuntimeError。"""
    params = urllib.parse.urlencode({
        "query": query,
        "start": f"{start.timestamp():.3f}",
        "end": f"{end.timestamp():.3f}",
        "step": str(step_s),
    })
    url = f"{base.rstrip('/')}/api/v1/query_range?{params}"
    with urllib.request.urlopen(url, timeout=15) as resp:
        payload = json.load(resp)
    if payload.get("status") != "success":
        raise RuntimeError(f"prometheus: {payload.get('error', 'unknown error')}")
    return payload["data"]["result"]


def prom_query(base, query, at):
    """Prometheus instant query → data.result;失败抛 RuntimeError。"""
    params = urllib.parse.urlencode({"query": query, "time": f"{at.timestamp():.3f}"})
    url = f"{base.rstrip('/')}/api/v1/query?{params}"
    with urllib.request.urlopen(url, timeout=15) as resp:
        payload = json.load(resp)
    if payload.get("status") != "success":
        raise RuntimeError(f"prometheus: {payload.get('error', 'unknown error')}")
    return payload["data"]["result"]


def _series_points(series):
    raw = series.get("values")
    if raw is None and "value" in series:  # instant query 的单个样本
        raw = [series["value"]]
    pts = []
    for ts, val in raw or []:
        try:
            v = float(val)
        except (TypeError, ValueError):
            continue
        if v != v:  # NaN
            continue
        pts.append((float(ts), v))
    return sorted(pts)


def counter_delta(result, start, end, label=None):
    """区间内 counter 增量,逐 series 处理 reset。

    返回 (总量, 最大边界误差秒, 次数, 是否假设过零基线, {label值: 增量})。
    某 series 在区间前无样本时,把区间内首个样本当作从 0 起算,并标记
    assumed_zero;这会在 Prometheus 刚启动时高估,误差写进 notes。
    """
    if end <= start:
        return 0.0, 0.0, 0, False, {}
    total = 0.0
    max_err = 0.0
    n_series = 0
    assumed_zero = False
    by_label = {}
    for series in result:
        pts = _series_points(series)
        if not pts:
            continue
        pre = [p for p in pts if p[0] <= start.timestamp()]
        inside = [p for p in pts if start.timestamp() < p[0] <= end.timestamp()]
        if not pre and not inside:
            continue
        n_series += 1
        if pre:
            seq = [pre[-1]] + inside
            max_err = max(max_err, start.timestamp() - pre[-1][0])
        else:
            seq = inside
            assumed_zero = True
            max_err = max(max_err, max(0.0, inside[0][0] - start.timestamp()))
        if len(seq) < 2:
            continue
        inc = 0.0
        last_ts = seq[-1][0]
        for (_, a), (_, b) in zip(seq, seq[1:]):
            inc += b - a if b >= a else b  # 下降视为进程重启,新增量从 0 起算
        max_err = max(max_err, max(0.0, end.timestamp() - last_ts))
        total += inc
        if label:
            key = series.get("metric", {}).get(label, "unknown")
            by_label[key] = by_label.get(key, 0.0) + inc
    return total, round(max_err, 3), n_series, assumed_zero, by_label


def gauge_at(result, at):
    """取每个 series 在 at 之前最后一个样本,按 label 汇总(只用于对账)。"""
    out = {}
    for series in result:
        pts = [p for p in _series_points(series) if p[0] <= at.timestamp()]
        if not pts:
            continue
        key = series.get("metric", {}).get("phase", "unknown")
        out[key] = pts[-1][1]
    return out


def run_kubectl(context, namespace, resource, name=None, selector=None):
    cmd = ["kubectl"]
    if context:
        cmd += ["--context", context]
    cmd += ["-n", namespace, "get", resource]
    if name:
        cmd.append(name)
    if selector:
        cmd += ["-l", selector]
    cmd += ["-o", "json"]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return None, f"kubectl {resource} 不可用: {exc}"
    if proc.returncode != 0:
        return None, f"kubectl {resource} 失败: {proc.stderr.strip()[:200]}"
    try:
        return json.loads(proc.stdout), None
    except json.JSONDecodeError as exc:
        return None, f"kubectl {resource} 返回非 JSON: {exc}"


def pod_facts(pod_json):
    """Pod → Ready 时刻、容器退出时刻、Pod 名。"""
    meta = pod_json.get("metadata", {})
    status = pod_json.get("status", {})
    ready_at = None
    for cond in status.get("conditions", []) or []:
        if cond.get("type") == "Ready" and cond.get("status") == "True":
            ready_at = parse_ts(cond.get("lastTransitionTime"))
            break
    exit_at = None
    for cs in status.get("containerStatuses", []) or []:
        term = (cs.get("state") or {}).get("terminated") or {}
        ts = parse_ts(term.get("finishedAt"))
        if ts and (exit_at is None or ts > exit_at):
            exit_at = ts
    return {
        "pod_name": meta.get("name"),
        "t_pod_created": parse_ts(meta.get("creationTimestamp")),
        "t_pod_start": parse_ts(status.get("startTime")),
        "t_pod_ready": ready_at,
        "t_container_exit": exit_at,
    }


def warmup_facts(wt_json, chunk_bytes):
    """WarmupTask.status → 门控放行、启动窗口就绪、fanout 与启动块估算。"""
    status = wt_json.get("status", {})
    gate = status.get("gateDecision", {}) or {}
    state = gate.get("state")
    release_at = parse_ts(gate.get("decidedAt")) if state == "Released" else None
    entries = status.get("podNodeReadiness", []) or []
    per_pod = {}
    ready_times = []
    status_bytes = 0
    for entry in entries:
        node = entry.get("nodeName") or ""
        if node:
            per_pod.setdefault(entry.get("podIndex", -1), set()).add(node)
        ready = int(entry.get("startupWindowReady") or 0)
        required = int(entry.get("requiredReady") or 0)
        status_bytes += ready * chunk_bytes
        ts = parse_ts(entry.get("lastReportTime"))
        if ts and (required <= 0 or ready >= required):
            ready_times.append(ts)
    fanouts = [len(nodes) for nodes in per_pod.values() if nodes]
    startup_ready_at = max(ready_times) if ready_times else None
    if startup_ready_at is None:
        all_times = [parse_ts(e.get("lastReportTime")) for e in entries]
        all_times = [t for t in all_times if t]
        startup_ready_at = max(all_times) if all_times else None
    return {
        "gate_state": state,
        "gate_reason": gate.get("reason"),
        "t_gate_release": release_at,
        "t_startup_ready": startup_ready_at,
        "startup_ready_source": "status.podNodeReadiness.lastReportTime",
        "t_warmup_started": parse_ts(status.get("startedAt")),
        "t_warmup_last_updated": parse_ts(status.get("lastUpdated")),
        "warmup_phase": status.get("phase"),
        "fanout_actual": round(sum(fanouts) / len(fanouts), 3) if fanouts else None,
        "fanout_max": max(fanouts) if fanouts else None,
        "startup_bytes_status_est": status_bytes,
        "warmup_pods": len(per_pod),
    }


def pick_pod(context, namespace, pod_name, selector):
    if pod_name:
        obj, err = run_kubectl(context, namespace, "pod", name=pod_name)
        return obj, err
    if not selector:
        return None, "未指定 --pod/--pod-selector"
    obj, err = run_kubectl(context, namespace, "pod", selector=selector)
    if err:
        return None, err
    items = obj.get("items", []) if obj else []
    if not items:
        return None, f"selector {selector} 没有 Pod"
    items.sort(key=lambda p: p.get("metadata", {}).get("creationTimestamp", ""))
    return items[0], None


def collect(args):
    run_path = Path(args.run_json) if args.run_json else Path(args.run_dir) / "run.json"
    run = json.loads(run_path.read_text())
    notes = []

    t_submit = parse_ts(args.t_submit or run.get("t_submit"))
    t_first = parse_ts(run.get("t_first_step"))
    t_last = parse_ts(run.get("t_last_step"))
    if run.get("t_submit_source") != "env":
        notes.append(f"t_submit_source={run.get('t_submit_source')}")

    pod, pod_err = pick_pod(args.context, args.namespace, args.pod, args.pod_selector)
    if pod_err:
        notes.append(pod_err)
    pod_info = pod_facts(pod) if pod else {}
    t_pod_ready = parse_ts(args.t_pod_ready) or pod_info.get("t_pod_ready")
    t_container_exit = pod_info.get("t_container_exit")
    if t_container_exit and t_last:
        notes.append(f"container_exit-last_step={secs(t_last, t_container_exit)}s(交叉核对)")

    wt_info = {}
    if args.warmup_task:
        wt, wt_err = run_kubectl(args.context, args.namespace, "warmuptask",
                                 name=args.warmup_task)
        if wt_err:
            notes.append(wt_err)
        elif wt:
            wt_info = warmup_facts(wt, args.chunk_bytes)
    t_gate_release = parse_ts(args.t_gate_release) or wt_info.get("t_gate_release")
    t_startup_ready = parse_ts(args.t_startup_ready) or wt_info.get("t_startup_ready")

    startup_end = t_gate_release or t_first
    steady_start = t_gate_release or t_first
    startup_makespan = secs(t_submit, t_first)
    full_makespan = secs(t_submit, t_last)
    post_pod = secs(t_pod_ready, t_last)
    gate_hold = secs(t_pod_ready, t_gate_release)

    dataset = args.dataset or run.get("dataset") or "track-d"
    chunk_bytes = args.chunk_bytes
    fetch = {"startup": None, "sustained": None, "switch": None, "total": None}
    fetch_err = None
    fetch_recon = None
    fetch_assumed_zero = False
    hit = {}
    gauge = {}
    boundary_error = None
    switch_count = len(run.get("switch_events", []) or [])
    switch_censored = sum(1 for e in (run.get("switch_events", []) or [])
                          if e.get("censored"))

    if args.prometheus_url and t_submit and t_last and t_last > t_submit:
        base = args.prometheus_url
        q = f'sum by (instance) (traincache_fetch_bytes_total{{dataset="{dataset}"}})'
        try:
            result = prom_query_range(base, q, t_submit - timedelta(seconds=60),
                                      t_last + timedelta(seconds=60), args.step_s)
            errs = []
            if startup_end:
                fetch["startup"], e, _, assumed, _ = counter_delta(
                    result, t_submit, startup_end)
                errs.append(e)
                fetch_assumed_zero = fetch_assumed_zero or assumed
            fetch["total"], e, _, assumed, _ = counter_delta(result, t_submit, t_last)
            errs.append(e)
            fetch_assumed_zero = fetch_assumed_zero or assumed
            switch_sum = 0.0
            for ev in run.get("switch_events", []) or []:
                start = parse_ts(ev.get("at"))
                end = parse_ts(ev.get("ready_at") or ev.get("epoch_end_at"))
                if not start or not end or end <= start:
                    continue
                inc, e, _, assumed, _ = counter_delta(result, start, end)
                switch_sum += inc
                errs.append(e)
                fetch_assumed_zero = fetch_assumed_zero or assumed
            fetch["switch"] = switch_sum
            if steady_start and t_last and t_last > steady_start:
                steady_total, e, _, assumed, _ = counter_delta(result, steady_start, t_last)
                errs.append(e)
                fetch_assumed_zero = fetch_assumed_zero or assumed
                fetch["sustained"] = max(0.0, steady_total - switch_sum)
            if fetch["startup"] is not None and fetch["sustained"] is not None:
                recon = fetch["startup"] + fetch["sustained"] + switch_sum
                fetch_recon = recon
                if fetch["total"] and fetch["total"] > 0:
                    fetch_recon = round(
                        (recon - fetch["total"]) / fetch["total"] * 100, 2)
            boundary_error = max(errs) if errs else None
            if fetch_assumed_zero:
                notes.append("fetch counter 区间前无样本,首个样本按 0 基线起算")
        except (urllib.error.URLError, TimeoutError, RuntimeError) as exc:
            fetch_err = str(exc)
            notes.append(f"Prometheus fetch 查询失败: {exc}")

        hits_q = (f'sum by (phase) (traincache_cache_hits_total'
                  f'{{dataset="{dataset}"}})')
        misses_q = (f'sum by (phase) (traincache_cache_misses_total'
                    f'{{dataset="{dataset}"}})')
        try:
            hits_res = prom_query_range(base, hits_q, t_submit - timedelta(seconds=60),
                                        t_last + timedelta(seconds=60), args.step_s)
            miss_res = prom_query_range(base, misses_q, t_submit - timedelta(seconds=60),
                                        t_last + timedelta(seconds=60), args.step_s)
            _, _, n_hits, _, hits_by = counter_delta(hits_res, t_submit, t_last,
                                                     label="phase")
            _, _, n_miss, _, miss_by = counter_delta(miss_res, t_submit, t_last,
                                                     label="phase")
            for phase in ("startup", "steady", "switch", "unknown"):
                h = hits_by.get(phase, 0.0)
                m = miss_by.get(phase, 0.0)
                hit[phase] = {
                    "hits": h, "misses": m,
                    "ratio": (h / (h + m)) if (h + m) > 0 else None,
                }
            if n_hits == 0 and n_miss == 0:
                notes.append("Prometheus 无 hits/misses 序列(可能未开监控或数据集名不符)")
            gauge_res = prom_query(
                base, f'traincache_cache_hit_ratio{{dataset="{dataset}"}}', t_last)
            gauge = gauge_at(gauge_res, t_last)
        except (urllib.error.URLError, TimeoutError, RuntimeError) as exc:
            notes.append(f"Prometheus 命中率查询失败: {exc}")

    row = {
        "run_id": run.get("run_id"),
        "cond": run.get("cond"),
        "group": run.get("group") or args.group,
        "dataset": dataset,
        "track": run.get("track"),
        "void": "true" if args.void else "false",
        "t_submit": fmt_ts(t_submit),
        "t_submit_source": run.get("t_submit_source"),
        "t_pod_ready": fmt_ts(t_pod_ready),
        "t_first_step": fmt_ts(t_first),
        "t_last_step": fmt_ts(t_last),
        "t_startup_ready": fmt_ts(t_startup_ready),
        "t_gate_release": fmt_ts(t_gate_release),
        "gate_state": wt_info.get("gate_state") or "",
        "gate_reason": wt_info.get("gate_reason") or "",
        "startup_makespan_s": startup_makespan,
        "full_makespan_s": full_makespan,
        "post_pod_s": post_pod,
        "gate_hold_s": gate_hold,
        "startup_fetch_bytes": fetch["startup"],
        "sustained_fetch_bytes": fetch["sustained"],
        "switch_fetch_bytes": fetch["switch"],
        "total_fetch_bytes": fetch["total"],
        "fetch_recon_diff_pct": fetch_recon,
        "fetch_boundary_error_s": boundary_error,
        "fanout_actual": wt_info.get("fanout_actual"),
        "fanout_max": wt_info.get("fanout_max"),
        "startup_bytes_status_est": wt_info.get("startup_bytes_status_est"),
        "hit_hits_startup": hit.get("startup", {}).get("hits"),
        "hit_misses_startup": hit.get("startup", {}).get("misses"),
        "hit_ratio_startup": hit.get("startup", {}).get("ratio"),
        "hit_hits_steady": hit.get("steady", {}).get("hits"),
        "hit_misses_steady": hit.get("steady", {}).get("misses"),
        "hit_ratio_steady": hit.get("steady", {}).get("ratio"),
        "hit_hits_switch": hit.get("switch", {}).get("hits"),
        "hit_misses_switch": hit.get("switch", {}).get("misses"),
        "hit_ratio_switch": hit.get("switch", {}).get("ratio"),
        "hit_ratio_gauge_startup": gauge.get("startup"),
        "hit_ratio_gauge_steady": gauge.get("steady"),
        "hit_ratio_gauge_switch": gauge.get("switch"),
        "switch_count": switch_count,
        "switch_censored": switch_censored,
        "pod_name": pod_info.get("pod_name") or "",
        "t_container_exit": fmt_ts(t_container_exit),
        "notes": "; ".join(notes),
    }
    return row, run, wt_info, pod_info


CSV_FIELDS = [
    "run_id", "cond", "group", "dataset", "track", "void",
    "t_submit", "t_submit_source", "t_pod_ready", "t_first_step", "t_last_step",
    "t_startup_ready", "t_gate_release", "gate_state", "gate_reason",
    "startup_makespan_s", "full_makespan_s", "post_pod_s", "gate_hold_s",
    "startup_fetch_bytes", "sustained_fetch_bytes", "switch_fetch_bytes",
    "total_fetch_bytes", "fetch_recon_diff_pct", "fetch_boundary_error_s",
    "fanout_actual", "fanout_max", "startup_bytes_status_est",
    "hit_hits_startup", "hit_misses_startup", "hit_ratio_startup",
    "hit_hits_steady", "hit_misses_steady", "hit_ratio_steady",
    "hit_hits_switch", "hit_misses_switch", "hit_ratio_switch",
    "hit_ratio_gauge_startup", "hit_ratio_gauge_steady",
    "hit_ratio_gauge_switch", "switch_count", "switch_censored",
    "pod_name", "t_container_exit", "notes",
]


def selftest():
    """无网络的纯函数自检:counter reset、边界误差、按 label 聚合。"""
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(seconds=15)
    base = start.timestamp()
    result = [
        {"metric": {"phase": "steady"},
         "values": [[base, "100"], [base + 5, "150"], [base + 10, "20"],
                    [base + 15, "70"]]},
        {"metric": {"phase": "switch"},
         "values": [[base, "5"], [base + 15, "9"]]},
    ]
    total, err, n, assumed, by = counter_delta(result, start, end, label="phase")
    assert n == 2 and not assumed, (n, assumed)
    assert total == 124.0, total  # 50 + 20(reset) + 50 + 4
    assert by["steady"] == 120.0 and by["switch"] == 4.0, by
    assert err == 0.0, err
    # 区间前无样本:首个样本按 0 起算,并标记 assumed_zero。
    total2, _, _, assumed2, _ = counter_delta(
        [{"metric": {}, "values": [[base + 5, "30"], [base + 10, "50"]]}],
        start, end)
    assert assumed2 and total2 == 20.0, (assumed2, total2)
    # 区间内无样本:增量为 0。
    total3, _, _, _, _ = counter_delta(
        [{"metric": {}, "values": [[base + 30, "1"], [base + 35, "2"]]}], start, end)
    assert total3 == 0.0, total3
    print("collect selftest: PASS")


def main():
    ap = argparse.ArgumentParser(description="M1 离线采集:run.json + K8s + Prometheus → metrics.csv")
    ap.add_argument("--run-dir", type=Path, help="run 产物目录(默认读 run.json)")
    ap.add_argument("--run-json", type=Path, help="显式 run.json 路径")
    ap.add_argument("--out", type=Path, help="metrics.csv 路径(缺省 run 目录下)")
    ap.add_argument("--prometheus-url", default="http://127.0.0.1:9090")
    ap.add_argument("--namespace", default="traincache")
    ap.add_argument("--context", help="kubectl context;缺省用当前 context")
    ap.add_argument("--pod", help="训练 Pod 名;与 --pod-selector 二选一")
    ap.add_argument("--pod-selector", help="训练 Pod 标签选择器,例如 job-name=train-xxx")
    ap.add_argument("--warmup-task", help="WarmupTask 名;非 traincache 组可省略")
    ap.add_argument("--dataset", help="指标里的 dataset 标签(缺省取 run.json)")
    ap.add_argument("--chunk-bytes", type=int, default=64 << 20,
                    help="块大小,用于 startup bytes 状态估算(缺省 64Mi)")
    ap.add_argument("--t-submit", help="人工覆盖 Job 提交时刻(RFC3339)")
    ap.add_argument("--t-pod-ready", help="人工覆盖 Pod Ready 时刻(RFC3339)")
    ap.add_argument("--t-gate-release", help="人工覆盖门控放行时刻(RFC3339)")
    ap.add_argument("--t-startup-ready", help="人工覆盖启动窗口就绪时刻(RFC3339)")
    ap.add_argument("--step-s", type=int, default=5, help="Prometheus range step 秒")
    ap.add_argument("--group", default="traincache", help="无 run.json group 时的回退组名")
    ap.add_argument("--void", action="store_true", help="整轮作废标记")
    ap.add_argument("--selftest", action="store_true", help="运行无网络自检后退出")
    args = ap.parse_args()

    if args.selftest:
        selftest()
        return

    if not args.run_dir and not args.run_json:
        ap.error("需要 --run-dir 或 --run-json")
    run_dir = Path(args.run_dir) if args.run_dir else Path(args.run_json).parent
    out = Path(args.out) if args.out else run_dir / "metrics.csv"

    row, run, wt_info, pod_info = collect(args)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="") as fout:
        writer = csv.DictWriter(fout, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerow(row)
    detail = {
        "schema_version": "1",
        "run_id": run.get("run_id"),
        "metrics_csv": str(out),
        "row": row,
        "warmup_task": {k: (fmt_ts(v) if isinstance(v, datetime) else v)
                        for k, v in wt_info.items()},
        "pod": {k: (fmt_ts(v) if isinstance(v, datetime) else v)
                for k, v in pod_info.items()},
        "switch_events": run.get("switch_events", []),
    }
    (run_dir / "collect.json").write_text(
        json.dumps(detail, ensure_ascii=False, indent=2) + "\n")
    print(f"collect: {out}")
    print(f"  makespan startup={row['startup_makespan_s']}s "
          f"full={row['full_makespan_s']}s "
          f"post_pod={row['post_pod_s'] if row['post_pod_s'] is not None else 'n/a'}s")
    print(f"  fetch startup={row['startup_fetch_bytes']} "
          f"steady={row['sustained_fetch_bytes']} switch={row['switch_fetch_bytes']} "
          f"recon={row['fetch_recon_diff_pct']}%")
    if row["notes"]:
        print(f"  notes: {row['notes']}")


if __name__ == "__main__":
    main()
