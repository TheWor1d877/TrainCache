# TrainCache bench 装置编排:make bench-up 一键拉起整套装置
# 常用目标:
#   make bench-up      一键:集群→镜像→k8s→数据(A/B/C/D 轨)→上传→tc→冷态→验证
#   make bench-data    只生成四轨数据(B 轨 8GB 约 25 分钟)
#   make bench-tc RATE=300mbit DELAY=10ms   换限速档位
#   make bench-cold    冷态规程
#   make bench-verify  两次冷读验证(过关标准)
#   make bench-down    拆 tc(集群与数据保留)
#   make track-d-data  D 轨数据(16×~128MB≈2GiB,落宿主机,不上传 MinIO)
#   make track-d-up    D 轨:搭源站容器+欠配 50%+tc+连续 10 epoch(训练由本目标单独触发)
#   make track-d-verify  D 轨:两轮冷读对账
#   make track-d-down  D 轨:拆 tc、恢复内存限制(128GiB 等效)、停容器
ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
# ---- python 解释器统一 ----
# .venv/bin/python 链到 /usr/bin/python3(现为 3.14),与 .venv 里 python3.12 的
# site-packages 不匹配,已不可执行;故全部 python 调用统一走 $(PY)(系统 python3),
# 第三方依赖(boto3 1.43.x)经 PYTHONPATH 指向 .venv 的 python3.12 site-packages。
# 注:make 中 $$PYTHONPATH 展开后是字面量 "$PYTHONPATH"(不会追加旧值),故这里写 $(PYTHONPATH)。
PY := /usr/bin/python3
VENV_SITE := $(ROOT)/.venv/lib/python3.12/site-packages
export PYTHONPATH := $(VENV_SITE):$(PYTHONPATH)

.PHONY: bench-up bench-down kind-up k8s-up data-up upload tc-up tc-down cold-reset bench-cold bench-verify bench-data bench-tc track-d-data track-d-up track-d-verify track-d-down e2e-local e2e-k8s

# D 轨数据已挂在 data-up 之下(track-d-data);bench-up 显式列出,使全链路依赖面直读含 D 轨(幂等,不会重复执行)
bench-up: kind-up k8s-up data-up track-d-data upload tc-up bench-verify
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

data-up: track-d-data
	@if [ ! -d $(ROOT)/data/track_a/files ] || [ -z "$$(ls $(ROOT)/data/track_a/files 2>/dev/null)" ]; then \
		echo "== 生成 A 轨(100 万×4KB,约 3-8 分钟)=="; $(PY) $(ROOT)/scripts/synth_track_a.py; \
	else echo "== A 轨已存在,跳过 =="; fi
	@if [ -z "$$(ls $(ROOT)/data/track_b/tars 2>/dev/null)" ]; then \
		echo "== 生成 B 轨(32×256MB≈8GB,约 25 分钟)=="; $(PY) $(ROOT)/scripts/synth_track_b.py; \
	else echo "== B 轨已存在,跳过 =="; fi
	@echo "== 生成 C 轨清单 =="; $(PY) $(ROOT)/scripts/synth_track_c.py

# .venv 仅作 boto3 的安装目的地(经顶部 PYTHONPATH 使用),不再执行其 bin/python
upload:
	@[ -e $(VENV_SITE)/boto3 ] || ($(PY) -m venv $(ROOT)/.venv && $(ROOT)/.venv/bin/python -m pip install boto3 -i https://pypi.tuna.tsinghua.edu.cn/simple)
	@echo "== 上传 B 轨数据到 MinIO($(shell $(PY) $(ROOT)/config.py --sh MINIO_BUCKET) 桶)=="
	@$(PY) $(ROOT)/scripts/upload_bench.py

tc-up:
	@bash $(ROOT)/scripts/tc_apply.sh $(RATE) $(DELAY)

tc-down:
	@bash $(ROOT)/scripts/tc_teardown.sh

bench-cold cold-reset:
	@bash $(ROOT)/scripts/cold_reset.sh

bench-verify:
	@bash $(ROOT)/scripts/verify_cold.sh $(QUICK)

bench-data: data-up

bench-tc: tc-up

bench-down: tc-down
	@echo "== bench-down:tc 已拆;集群与数据保留(删集群用 kind delete cluster --name traincache)=="

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
# 两者证据落 agent/results/*.raw.log;验收 1/5/9 需对象存储 + 测试数据,另行覆盖。
e2e-local:
	@bash $(ROOT)/../agent/e2e/run-local.sh

e2e-k8s:
	@bash $(ROOT)/../agent/e2e/run-k8s.sh

# ---- M2 实验 runner:条件化入口 ----
# make bench COND=<name> [REPS=7]   跑指定 COND 的重复采样
# make bench-list                   列出 conds/ 下所有 COND 名
REPS ?= 7
.PHONY: bench bench-list

bench:
	$(PY) experiments/run.py --cond $(COND) --reps $(REPS)

bench-list:
	@if ls experiments/conds/*.json >/dev/null 2>&1; then \
		ls experiments/conds/*.json | xargs -n1 basename | sed 's/.json//'; \
	else \
		echo "no conds found"; \
	fi
