# TrainCache bench 装置编排:make bench-up 一键拉起整套装置
# 常用目标:
#   make bench-up      一键:集群→镜像→k8s→数据→上传→tc→冷态→验证
#   make bench-data    只生成三轨数据(B 轨 8GB 约 25 分钟)
#   make bench-tc RATE=300mbit DELAY=10ms   换限速档位
#   make bench-cold    冷态规程
#   make bench-verify  两次冷读验证(过关标准)
#   make bench-down    拆 tc(集群与数据保留)
ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
PY := python3
PY_VENV := $(ROOT)/.venv/bin/python
PIP_VENV := $(ROOT)/.venv/bin/pip

.PHONY: bench-up bench-down kind-up k8s-up data-up upload tc-up tc-down cold-reset bench-cold bench-verify bench-data bench-tc

bench-up: kind-up k8s-up data-up upload tc-up bench-verify
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

data-up:
	@if [ ! -d $(ROOT)/data/track_a/files ] || [ -z "$$(ls $(ROOT)/data/track_a/files 2>/dev/null)" ]; then \
		echo "== 生成 A 轨(100 万×4KB,约 3-8 分钟)=="; $(PY) $(ROOT)/scripts/synth_track_a.py; \
	else echo "== A 轨已存在,跳过 =="; fi
	@if [ -z "$$(ls $(ROOT)/data/track_b/tars 2>/dev/null)" ]; then \
		echo "== 生成 B 轨(32×256MB≈8GB,约 25 分钟)=="; $(PY) $(ROOT)/scripts/synth_track_b.py; \
	else echo "== B 轨已存在,跳过 =="; fi
	@echo "== 生成 C 轨清单 =="; $(PY) $(ROOT)/scripts/synth_track_c.py

upload:
	@[ -x $(PY_VENV) ] || (python3 -m venv $(ROOT)/.venv && $(PIP_VENV) install boto3 -i https://pypi.tuna.tsinghua.edu.cn/simple)
	@echo "== 上传 B 轨数据到 MinIO($(shell $(PY) $(ROOT)/config.py --sh MINIO_BUCKET) 桶)=="
	@$(PY_VENV) $(ROOT)/scripts/upload_bench.py

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
