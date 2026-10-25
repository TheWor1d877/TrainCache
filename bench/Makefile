# TrainCache bench 装置编排:make bench-up 一键拉起整套装置
# 常用目标:
#   make bench-up      一键:集群→镜像→k8s→数据(A/B/C/D 轨)→上传→控制面+agentd→tc→冷态→验证
#   make bench-data    只生成四轨数据(B 轨 8GB 约 25 分钟)
#   make bench-tc RATE=300mbit DELAY=10ms   换限速档位
#   make bench-cold    冷态规程
#   make bench-verify  两次冷读验证(过关标准)
#   make bench-down    拆 tc(集群与数据保留)
#   make bench-down-full  拆 tc + 删控制面(集群与数据保留)
#   make bench-status  只读查看集群/节点/MinIO/tc/数据目录/最近产物
#   make bench-doctor  只读体检:逐项 PASS/FAIL/WARN,有 FAIL 时退出码非零
#   make bench-quick TIER=S RATE=1000 REPS=3 [TAG=...]
#                      只跑 traincache-cold + traincache-warm 的快速跑分
#   make bench-m7 [FORCE=1]  M7 五格串行跑分(约 6.3 h;FORCE=1 全量重跑)
#   make track-d-data  D 轨数据(16×~128MB≈2GiB,落宿主机,不上传 MinIO)
#   make track-d-up    D 轨:搭源站容器+欠配 50%+tc+连续 10 epoch(训练由本目标单独触发)
#   make track-d-verify  D 轨:两轮冷读对账
#   make track-d-down  D 轨:拆 tc、恢复内存限制(128GiB 等效)、停容器
ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
# ---- python 解释器统一 ----
# 所有 python 调用统一走项目根 .venv,依赖清单见 $(ROOT)/requirements.txt。
# .venv/bin/python 自带依赖目录,无需额外路径配置。
PY := $(abspath $(ROOT)/../.venv/bin/python)

.PHONY: bench-up bench-down bench-down-full ctrl-up ctrl-down bench-status bench-doctor bench-quick bench-m7 kind-up k8s-up data-up upload tc-up tc-down cold-reset bench-cold bench-verify bench-data bench-tc track-d-data track-d-up track-d-verify track-d-down e2e-local e2e-k8s selftest

# D 轨数据已挂在 data-up 之下(track-d-data);bench-up 显式列出,使全链路依赖面直读含 D 轨(幂等,不会重复执行)
bench-up: kind-up k8s-up data-up track-d-data upload ctrl-up tc-up bench-verify
	@echo "== bench-up 完成:装置已拉起并通过冷读验证 =="

kind-up:
	@if kind get clusters 2>/dev/null | grep -q traincache; then \
		echo "== kind 集群 traincache 已存在,复用 =="; \
	else \
		echo "== 创建 kind 集群 =="; \
		kind create cluster --config $(ROOT)/k8s/kind-config.yaml; \
		for img in minio/minio:latest busybox:latest; do \
			kind load docker-image $$img --name traincache; \
		done; \
	fi
	@kubectl get nodes

k8s-up:
	@kubectl apply -f $(ROOT)/k8s/minio.yaml
	@kubectl -n minio rollout status deployment/minio --timeout=180s
	@echo "== MinIO 就绪 =="

# 数据与产物一律落运行时数据层,路径口径只认 config.py 一处
DATA_DIR      := $(shell $(PY) $(ROOT)/config.py --sh DATA_DIR)
TRACK_A_DIR   := $(shell $(PY) $(ROOT)/config.py --sh TRACK_A_DIR)
TRACK_B_DIR   := $(shell $(PY) $(ROOT)/config.py --sh TRACK_B_DIR)
RESULTS_DIR   := $(shell $(PY) $(ROOT)/config.py --sh RESULTS_DIR)

data-up: track-d-data
	@if [ -z "$(TRACK_A_DIR)" ] || [ -z "$(TRACK_B_DIR)" ]; then \
		echo "错误:config.py 取值失败(TRACK_A_DIR/TRACK_B_DIR 为空)"; exit 1; fi
	@if [ ! -d $(TRACK_A_DIR) ] || [ -z "$$(ls $(TRACK_A_DIR) 2>/dev/null)" ]; then \
		echo "== 生成 A 轨(100 万×4KB,约 3-8 分钟)=="; $(PY) $(ROOT)/scripts/synth_track_a.py; \
	else echo "== A 轨已存在,跳过 =="; fi
	@if [ -z "$$(ls $(TRACK_B_DIR) 2>/dev/null)" ]; then \
		echo "== 生成 B 轨(32×256MB≈8GB,约 25 分钟)=="; $(PY) $(ROOT)/scripts/synth_track_b.py; \
	else echo "== B 轨已存在,跳过 =="; fi
	@echo "== 生成 C 轨清单 =="; $(PY) $(ROOT)/scripts/synth_track_c.py

upload:
	@$(PY) -c "import boto3" >/dev/null 2>&1 || { \
		echo "错误: $(PY) 缺 boto3。请运行 $(PY) -m pip install -r $(ROOT)/requirements.txt"; exit 1; \
	}
	@echo "== 上传 B 轨数据到 MinIO($(shell $(PY) $(ROOT)/config.py --sh MINIO_BUCKET) 桶)=="
	@$(PY) $(ROOT)/scripts/upload_bench.py

ctrl-up:
	@bash $(ROOT)/scripts/ctrl_up.sh

ctrl-down:
	@bash $(ROOT)/scripts/ctrl_down.sh

tc-up:
	@bash $(ROOT)/scripts/tc_apply.sh $(RATE) $(DELAY)

tc-down:
	@bash $(ROOT)/scripts/tc_teardown.sh

bench-cold cold-reset:
	@bash $(ROOT)/scripts/cold_reset.sh

bench-verify:
	@bash $(ROOT)/scripts/verify_cold.sh $(QUICK)

# ---- 装置自检(M9 9.4:全本地、不碰集群、不改数据) ----
# 9.1 §11 记的「make verify 没把 bench 的 Python 自检串起来」由本目标补齐:
# 跑正式轮前后各跑一遍,任何一项红即整轮不该开跑。
selftest:
	@echo "== 装置自检(纯本地) =="
	@$(PY) -m py_compile $(ROOT)/config.py $(wildcard $(ROOT)/experiments/*.py) $(ROOT)/compare/run_bench.py $(ROOT)/compare/trace.py
	@$(PY) $(ROOT)/compare/run_bench.py --selftest
	@$(PY) $(ROOT)/experiments/cond.py --selftest
	@$(PY) $(ROOT)/experiments/lock.py --selftest
	@$(PY) $(ROOT)/experiments/collect.py --selftest
	@$(PY) -c "import json,sys,glob,os; sys.path.insert(0,'$(ROOT)/experiments'); import cond; bad=[(os.path.basename(f),e) for f in sorted(glob.glob('$(ROOT)/experiments/conds/*.json')) for e in cond.validate_cond(json.load(open(f)))]; print('COND 全量校验: %d 个文件, %d 条错误' % (len(glob.glob('$(ROOT)/experiments/conds/*.json')), len(bad))); [print(' ', b) for b in bad[:5]]; sys.exit(1 if bad else 0)"
	@bash $(ROOT)/scripts/tc_apply.sh --self-test
	@$(MAKE) --no-print-directory -C $(ROOT)/compare check
	@echo "== 装置自检通过 =="

bench-data: data-up

bench-tc: tc-up

bench-down: tc-down
	@echo "== bench-down:tc 已拆;集群与数据保留(删集群用 kind delete cluster --name traincache)=="

# 全拆:先 tc-down,再删控制面 Deployment/DS/CRD/Secret;kind 集群与 ~/tcd 数据保留
bench-down-full: tc-down
	@bash $(ROOT)/scripts/ctrl_down.sh
	@echo "== bench-down-full:tc 与控制面已拆;kind 集群与 ~/tcd 数据保留 =="

# ---- 只读状态与体检 ----
# 二者都不改集群、不动数据、不挂/拆 tc;doctor 的 FAIL 决定退出码。
bench-status:
	@bash $(ROOT)/scripts/doctor.sh --status

bench-doctor:
	@bash $(ROOT)/scripts/doctor.sh

# ---- 快速跑分:只跑 TrainCache 自身(cold + warm),不跑前三家对照 ----
# 产物落 $(RESULTS_DIR)/quick/<run_id>/;run_id 由脚本按 UTC 时间戳 + git short sha 生成。
# RATE/REPS 取自命令行;未显式给出时 TIER=S、RATE=1000、REPS=3(与 bench 的 REPS=7 默认区分)。
TIER ?= S
QUICK_RATE = $(if $(strip $(RATE)),$(strip $(RATE)),1000)
QUICK_REPS = $(if $(filter command line environment,$(origin REPS)),$(strip $(REPS)),3)

bench-quick:
	@bash $(ROOT)/scripts/bench_quick.sh --tier "$(TIER)" --rate "$(QUICK_RATE)" --reps "$(QUICK_REPS)" $(if $(strip $(TAG)),--tag "$(strip $(TAG))")

# ---- M7 五格串行跑分 ----
# 五格顺序、rate/groups/reps 与产物目录由 run_m7.sh 单点定义。
# 已完成格自动跳过;FORCE=1 忽略已完成格全量重跑。
bench-m7:
	@bash $(ROOT)/compare/run_m7.sh $(if $(strip $(FORCE)),--force)

# ---- D 轨(v4 全程收益专用):单 Job 连续 10 epoch、每 epoch 换 seed、缓存欠配 50% ----
# 数据 16 个 shard-*.tar 落宿主机(路径取 config.py 的 TRACK_D_DIR),经独立 nginx
# 容器 traincache-source-d(端口 9100,挂 /usr/share/nginx/html/track-d)供训练进程
# 读取,不上传 MinIO。track_d_up/verify/down.sh 由 T2a 实现(接口冻结:脚本名与调用
# 形态即下述 recipe;脚本自读 config.py 的 TRACK_D_* 参数,风格同既有 tc-up/bench-verify)。
TRACK_D_DIR := $(shell $(PY) $(ROOT)/config.py --sh TRACK_D_DIR)

track-d-data:
	@if [ -z "$$(ls $(TRACK_D_DIR)/shard-*.tar 2>/dev/null)" ]; then \
		echo "== 生成 D 轨(16×~128MB≈2GiB,欠配 50% 专用)=="; \
		$(PY) $(ROOT)/scripts/synth_track_d.py; \
	else echo "== D 轨已存在,跳过 =="; fi

track-d-up:
	@bash $(ROOT)/scripts/track_d_up.sh

track-d-verify:
	@bash $(ROOT)/scripts/track_d_verify.sh

track-d-down:
	@bash $(ROOT)/scripts/track_d_down.sh

# ---- e2e 演练(M7c):Agent 服务化端到端 ----
# e2e-local:本机跑真实 agentd(TRAINCACHE_CACHE_ROOT 指向可写临时目录)+ CLI 三命令
#            + /metrics 七线指标;自清理进程与临时目录,可重复执行。
# e2e-k8s:  复用 kind 集群(traincache,幂等 kubectl apply),演练验收 2/3/8/10。
# 两者证据落 $(DATA_ROOT)/agent/results/*.raw.log;验收 1/5/9 需对象存储 + 测试数据,另行覆盖。
e2e-local:
	@bash $(ROOT)/../agent/e2e/run-local.sh

e2e-k8s:
	@bash $(ROOT)/../agent/e2e/run-k8s.sh

# ---- M2/M5 实验 runner:条件化入口 ----
# make bench COND=<name> [REPS=n]   跑指定 COND 的重复采样
#   COND 以 policy- 开头 → experiments/run_policy.py(C 轨淘汰×预取),
#     不传 REPS 时取 COND 的 sampling.reps
#   其余 → experiments/run.py,不传 REPS 时默认 7 次
# make bench-list                   列出 conds/ 下所有 COND 名
.PHONY: bench bench-list

bench:
	@if [ -z "$(strip $(COND))" ]; then \
		echo "错误:需要 COND=<name>(make bench-list 查看)"; exit 2; \
	fi
	@$(PY) experiments/$(if $(filter policy-%,$(strip $(COND))),run_policy.py,run.py) --cond $(strip $(COND)) $(if $(strip $(REPS)),--reps $(strip $(REPS)))

bench-list:
	@if ls experiments/conds/*.json >/dev/null 2>&1; then \
		ls experiments/conds/*.json | xargs -n1 basename | sed 's/.json//'; \
	else \
		echo "no conds found"; \
	fi
