#!/usr/bin/env python3
"""traintrace 探针编排:负载旁路拉起,负载结束 SIGINT 收尾。

开关与过滤集合走运行参数(环境变量),不进 COND。同一个 COND 跑两遍,
一遍设 TRAINTRACE_PROBE=1 挂探针,一遍不设,两遍产物供 probe_overhead.py 对账。
traintrace 的 metrics.json/manifest.json 直接落进本次 run 目录,与负载侧指标同目录。

两处起法:
  * 手动宿主机负载:探针在宿主上起(Probe 类),过滤集合走 --pid。train 负载
    已改走集群内 e2e Pod,run.py 不再自动挂宿主探针,这起法供手动负载与自检。
  * dataset 形态:测量 Pod 在集群里跑,探针进 Pod 所在节点容器起(NodeProbe 类),
    过滤集合走 Pod 的 cgroup。kind 节点容器的 cgroup 命名空间是私有的,宿主上
    看不到 Pod 的 cgroup 目录树,探针与被观测进程必须同处一个命名空间,
    --pid 解析出的 cgroup id 才与内核上报的 cgroup id 对得上。
"""
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

BENCH_ROOT = Path(__file__).resolve().parent.parent
REPO_ROOT = BENCH_ROOT.parent
DEFAULT_BIN = REPO_ROOT / "ebpfobs" / "build" / "traintrace"
SUDO_PASS = BENCH_ROOT / ".sudo-pass"


def _enabled(env: dict) -> bool:
    return str(env.get("TRAINTRACE_PROBE", "")).strip().lower() in ("1", "true", "yes")


def _split(env: dict, key: str) -> list[str]:
    raw = env.get(key, "")
    return [x.strip() for x in raw.split(",") if x.strip()]


class Probe:
    """一个在跑的 traintrace 进程。stop() 发 SIGINT,等它落盘后退出。"""

    def __init__(self, proc: subprocess.Popen, out_dir: Path):
        self.proc = proc
        self.out_dir = out_dir

    def stop(self, timeout: float = 30.0) -> int:
        # traintrace 收到 SIGINT 才做最后一次全量拉取与配对落盘,不能 SIGKILL。
        # 独立进程组里可能有 sudo 父进程,整组发信号确保 traintrace 收到。
        try:
            os.killpg(os.getpgid(self.proc.pid), signal.SIGINT)
        except ProcessLookupError:
            pass
        try:
            return self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
            return self.proc.wait()


def start_probe(run_dir: Path, env: dict | None = None) -> Probe | None:
    """按运行参数拉起 traintrace;未开探针或参数不全时返回 None(不影响负载)。"""
    env = os.environ if env is None else env
    if not _enabled(env):
        return None

    bin_path = Path(env.get("TRAINTRACE_BIN") or DEFAULT_BIN)
    if not bin_path.is_file():
        print(f"[probe] 跳过:找不到 traintrace 二进制 {bin_path}", file=sys.stderr)
        return None
    dev = env.get("TRAINTRACE_DEV", "").strip()
    cgroups = _split(env, "TRAINTRACE_CGROUP")
    pids = _split(env, "TRAINTRACE_PID")
    if not dev or not (cgroups or pids):
        print("[probe] 跳过:TRAINTRACE_DEV 与 TRAINTRACE_CGROUP/TRAINTRACE_PID 不全",
              file=sys.stderr)
        return None

    run_dir = Path(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    args = [str(bin_path), "--dev", dev,
            "--duration", env.get("TRAINTRACE_DURATION", "0"),
            "--out-dir", str(run_dir)]
    for cg in cgroups:
        args += ["--cgroup", cg]
    for pid in pids:
        args += ["--pid", pid]

    # traintrace 要 root 或 CAP_BPF/CAP_PERFMON。非 root 且备有 sudo 口令时经 sudo -S 拉起。
    stdin = subprocess.DEVNULL
    cmd = args
    password = None
    if os.geteuid() != 0 and SUDO_PASS.is_file():
        cmd = ["sudo", "-S", "-p", ""] + args
        password = SUDO_PASS.read_text().strip()
        stdin = subprocess.PIPE

    log = (run_dir / "traintrace.log").open("w")
    proc = subprocess.Popen(cmd, stdin=stdin, stdout=log, stderr=subprocess.STDOUT,
                            start_new_session=True)
    log.close()  # 子进程已持有 dup 出的 fd,父进程这份关掉
    if password is not None:
        proc.stdin.write((password + "\n").encode())
        proc.stdin.close()
    print(f"[probe] traintrace 已拉起 pid={proc.pid} dev={dev} out={run_dir}")
    return Probe(proc, run_dir)


# ---------------------------------------------------------------- 节点内探针

# 节点容器里 /tmp 与 /run 是 noexec 的 tmpfs,docker cp 写进去的文件在容器内也
# 看不见,二进制没法执行。走 /var/tmp(overlay 可写层,可执行)。
NODE_BIN = "/var/tmp/traintrace-bin"
NODE_OUT = "/var/tmp/traintrace-out"
NODE_LOG = "/var/tmp/traintrace.log"
NODE_PID = "/var/tmp/traintrace.pid"


def _run(cmd: list[str], timeout: int = 120) -> tuple[int, str, str]:
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    return proc.returncode, proc.stdout, proc.stderr


def _docker(args: list[str], timeout: int = 120) -> tuple[int, str, str]:
    return _run(["docker", *args], timeout=timeout)


def _kubectl(args: list[str], namespace: str, timeout: int = 60) -> tuple[int, str, str]:
    cmd = ["kubectl"]
    if namespace:
        cmd += ["-n", namespace]
    cmd += args
    return _run(cmd, timeout=timeout)


def pod_probe_target(namespace: str, name: str) -> dict | None:
    """测量 Pod 的落点:节点名、容器 ID、Pod UID、当前阶段。Pod 不存在返回 None。"""
    rc, out, _ = _kubectl(["get", "pod", name, "-o", "json"], namespace)
    if rc != 0:
        return None
    try:
        pod = json.loads(out)
    except ValueError:
        return None
    container_id = ""
    for cs in pod.get("status", {}).get("containerStatuses") or []:
        container_id = cs.get("containerID") or ""
        if container_id:
            break
    return {
        "node": pod.get("spec", {}).get("nodeName") or "",
        "container_id": container_id.split("://")[-1],
        "uid": pod.get("metadata", {}).get("uid") or "",
        "phase": pod.get("status", {}).get("phase") or "",
    }


def node_cgroup_dir(node: str, container_id: str, pod_uid: str) -> str | None:
    """节点内 Pod 的 cgroup 目录。先按容器 ID 找容器级 scope,找不到退回 Pod 级。"""
    for token in (container_id[:12], pod_uid):
        if not token:
            continue
        rc, out, _ = _docker([
            "exec", node, "sh", "-c",
            f"find /sys/fs/cgroup -maxdepth 5 -type d -name '*{token}*' 2>/dev/null | head -1",
        ])
        if rc == 0:
            hit = out.strip().splitlines()[0].strip() if out.strip() else ""
            if hit:
                return hit
    return None


def node_disk_dev(node: str) -> str | None:
    """节点上承载容器可写层与 hostPath 缓存的块设备节点路径。取不到返回 None。"""
    rc, out, _ = _docker([
        "exec", node, "sh", "-c",
        "df --output=source /var/lib/containerd 2>/dev/null | tail -1",
    ])
    dev = out.strip().splitlines()[-1].strip() if out.strip() else ""
    if rc == 0 and dev.startswith("/dev/"):
        return dev
    return None


class NodeProbe:
    """跑在 kind 节点容器内的 traintrace。stop() 发 SIGINT,等它落盘后取回产物。"""

    def __init__(self, node: str, cgroup: str, dev: str, out_dir: Path,
                 bin_path: Path, tag: str):
        self.node = node
        self.cgroup = cgroup
        self.dev = dev
        self.out_dir = Path(out_dir)
        self.bin_path = Path(bin_path)
        self.tag = tag
        self.pid = ""
        self.node_out = f"{NODE_OUT}-{tag}"

    def start(self) -> None:
        self.out_dir.mkdir(parents=True, exist_ok=True)
        rc, _, err = _docker(["exec", self.node, "mkdir", "-p", self.node_out])
        if rc != 0:
            raise RuntimeError(f"节点 {self.node} 建产物目录失败: {err[:200]}")
        rc, _, err = _docker(["cp", str(self.bin_path), f"{self.node}:{NODE_BIN}"])
        if rc != 0:
            raise RuntimeError(f"二进制进节点 {self.node} 失败: {err[:200]}")
        cmd = (f"echo $$ > {NODE_PID}; exec {NODE_BIN} --dev {self.dev} "
               f"--cgroup {self.cgroup} --duration 0 --out-dir {self.node_out} "
               f"> {NODE_LOG}.{self.tag} 2>&1")
        rc, _, err = _docker(["exec", "-d", self.node, "sh", "-c", cmd])
        if rc != 0:
            raise RuntimeError(f"节点 {self.node} 起探针失败: {err[:200]}")
        for _ in range(20):
            rc, out, _ = _docker(["exec", self.node, "cat", NODE_PID])
            if rc == 0 and out.strip():
                self.pid = out.strip()
                break
            time.sleep(0.2)
        if not self.pid:
            raise RuntimeError(f"节点 {self.node} 探针 pid 没写出来")
        print(f"[probe] 节点 {self.node} 探针已起 pid={self.pid} "
              f"dev={self.dev} cgroup={self.cgroup}")

    def _alive(self) -> bool:
        rc, _, _ = _docker(["exec", self.node, "sh", "-c",
                            f"kill -0 {self.pid} 2>/dev/null"])
        return rc == 0

    def stop(self, timeout: float = 60.0) -> int:
        """SIGINT 收尾:traintrace 收到信号才做最后一次拉取与配对落盘。"""
        if not self.pid:
            return -1
        _docker(["exec", self.node, "kill", "-INT", self.pid])
        deadline = time.time() + timeout
        while time.time() < deadline:
            if not self._alive():
                break
            time.sleep(0.5)
        else:
            _docker(["exec", self.node, "kill", "-KILL", self.pid])
            time.sleep(1.0)
        rc, _, err = _docker(["cp", f"{self.node}:{NODE_LOG}.{self.tag}",
                              str(self.out_dir / "traintrace.log")])
        if rc != 0:
            print(f"[probe] 取回日志失败: {err[:200]}", file=sys.stderr)
        rc, _, err = _docker(["cp", f"{self.node}:{self.node_out}/.",
                              str(self.out_dir)])
        if rc != 0:
            print(f"[probe] 取回产物失败: {err[:200]}", file=sys.stderr)
        _docker(["exec", self.node, "rm", "-rf", self.node_out,
                 f"{NODE_LOG}.{self.tag}", NODE_BIN, NODE_PID])
        print(f"[probe] 节点 {self.node} 探针已收尾,产物 → {self.out_dir}")
        return 0


def watch_pod(namespace: str, name: str, out_dir: Path, bin_path: Path,
              reps: int, timeout_s: float = 5400.0, poll_s: float = 3.0) -> list[dict]:
    """盯测量 Pod 的多轮 incarnation:每轮 Pod 起来起一次探针,结束收尾取回产物。

    一轮 COND 里 Pod 名字不变(train-job-0 每轮重建),按 Pod UID 分轮。
    """
    out_root = Path(out_dir)
    out_root.mkdir(parents=True, exist_ok=True)
    seen: set[str] = set()
    done = 0
    deadline = time.time() + timeout_s
    records: list[dict] = []
    while done < reps and time.time() < deadline:
        target = pod_probe_target(namespace, name)
        if target is None or not target["uid"] or target["uid"] in seen:
            time.sleep(poll_s)
            continue
        seen.add(target["uid"])
        # 等 Pod 真正 Running,此时才有容器 ID 与 cgroup
        while target["phase"] != "Running" and time.time() < deadline:
            time.sleep(poll_s)
            target = pod_probe_target(namespace, name)
            if target is None:
                break
        if target is None or target["phase"] != "Running":
            continue
        node = target["node"]
        cgroup = node_cgroup_dir(node, target["container_id"], target["uid"])
        dev = node_disk_dev(node)
        if not cgroup or not dev:
            print(f"[probe] 节点 {node} 解析 cgroup/dev 失败(cgroup={cgroup}, dev={dev}),"
                  f"本轮不挂探针", file=sys.stderr)
            records.append({"rep": done, "node": node, "skipped": True})
            done += 1
            continue
        probe = NodeProbe(node, cgroup, dev, out_root / f"rep-{done:02d}",
                          bin_path, f"r{done:02d}")
        probe.start()
        end_reason = "timeout"
        while time.time() < deadline:
            cur = pod_probe_target(namespace, name)
            if cur is None:
                end_reason = "pod_deleted"
                break
            if cur["uid"] != target["uid"]:
                end_reason = "recreated"
                break
            if cur["phase"] in ("Succeeded", "Failed"):
                end_reason = cur["phase"]
                break
            time.sleep(poll_s)
        probe.stop()
        records.append({"rep": done, "node": node, "cgroup": cgroup,
                        "dev": dev, "end": end_reason})
        done += 1
    return records


def selftest() -> None:
    """用假二进制验证拉起、SIGINT 收尾与进程组信号,不需要 root 也不碰集群。"""
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        marker = root / "stopped"
        started = root / "started"
        fake = root / "fake-traintrace"
        fake.write_text(
            "#!/bin/sh\n"
            f"trap 'echo stopped > {marker}; exit 0' INT\n"
            f"echo started > {started}\n"
            "while true; do sleep 0.1; done\n")
        fake.chmod(0o755)

        env = os.environ.copy()
        env.update({"TRAINTRACE_PROBE": "1", "TRAINTRACE_BIN": str(fake),
                    "TRAINTRACE_DEV": "/dev/null", "TRAINTRACE_PID": str(os.getpid())})
        run_dir = root / "run"
        probe = start_probe(run_dir, env)
        assert probe is not None, "探针未拉起"
        for _ in range(50):
            if marker.exists():
                break
            time.sleep(0.1)
        rc = probe.stop()
        assert rc == 0, f"退出码 {rc}"
        assert started.exists(), "假二进制没起来"
        assert marker.exists(), "没收到 SIGINT"
        assert (run_dir / "traintrace.log").exists(), "没落探针日志"

    print("probe selftest: PASS")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    elif "--watch-ns" in sys.argv:
        import argparse

        ap = argparse.ArgumentParser(
            description="盯集群里的测量 Pod,Pod 起来起探针、结束收尾取回产物")
        ap.add_argument("--watch-ns", required=True, help="测量 Pod 所在命名空间")
        ap.add_argument("--watch-pod", default="train-job-0", help="测量 Pod 名")
        ap.add_argument("--watch-out", required=True, help="产物落点(每轮一个子目录)")
        ap.add_argument("--watch-reps", type=int, default=1, help="盯几轮")
        ap.add_argument("--watch-timeout", type=float, default=5400.0,
                        help="总超时秒数")
        ap.add_argument("--watch-bin", default=str(DEFAULT_BIN), help="traintrace 二进制")
        args = ap.parse_args()
        recs = watch_pod(args.watch_ns, args.watch_pod, Path(args.watch_out),
                         Path(args.watch_bin), args.watch_reps, args.watch_timeout)
        for rec in recs:
            print(f"[probe] {rec}")
    else:
        print("用法: probe.py --selftest | probe.py --watch-ns NS --watch-out DIR")