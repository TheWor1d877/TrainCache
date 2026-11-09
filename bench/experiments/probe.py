#!/usr/bin/env python3
"""traintrace 探针编排:load 步骤旁路拉起,load 结束 SIGINT 收尾。

开关与过滤集合走运行参数(环境变量),不进 COND。同一个 COND 跑两遍,
一遍设 TRAINTRACE_PROBE=1 挂探针,一遍不设,两遍产物供 probe_overhead.py 对账。
traintrace 的 metrics.json/manifest.json 直接落进本次 run 目录,与负载侧指标同目录。
"""
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
    else:
        print("用法: probe.py --selftest(其余调用由 run.py 走 probe_mod)")