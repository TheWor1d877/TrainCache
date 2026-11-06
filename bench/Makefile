# TrainCache bench 装置:只保留装置生命周期三个目标。
#
#   make bench-up      拉起整套装置:kind → MinIO → 数据集/倾斜/训练数据 → 上传
#                      → 控制面 → tc → 冷态 → 两次冷读验证
#   make bench-doctor  只读体检:先本地 py_compile,再跑 doctor.sh(有 FAIL 退出码非零)
#   make bench-down    拆装置:拆 tc + 删控制面(集群与数据保留,删集群用 kind delete)
#
# 跑实验一律走 python3 bench/run.py <族> [选项];单格执行体由它按族分派。
ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))

# ---- python 解释器统一 ----
# 所有 python 调用统一走项目根 .venv,依赖清单见 $(ROOT)/requirements.txt。
PY := $(abspath $(ROOT)/../.venv/bin/python)
# 只给不依赖第三方库的目标用:.venv 没装时退回系统 python3,避免计划都打不开。
PY_FALLBACK := $(if $(wildcard $(PY)),$(PY),python3)

# 数据与产物路径只认 config.py 一处,Makefile 不重复写死目录名。
CFG = $(shell $(PY_FALLBACK) $(ROOT)/config.py --sh $(1))
DATASET_DIR := $(call CFG,DATASET_DIR)
TILT_DIR    := $(call CFG,TILT_DIR)
TRAIN_DIR   := $(call CFG,TRAIN_DIR)

# py_compile 清单:装置侧全部 python 入口与执行体。
PY_FILES = $(ROOT)/run.py $(ROOT)/config.py \
           $(wildcard $(ROOT)/experiments/*.py) \
           $(ROOT)/compare/run_bench.py $(ROOT)/compare/gateway_multi.py \
           $(ROOT)/compare/summarize.py $(ROOT)/compare/trace.py \
           $(wildcard $(ROOT)/scripts/*.py)

.PHONY: bench-up bench-doctor bench-down

bench-up:
	@if kind get clusters 2>/dev/null | grep -qx traincache; then \
		echo "== kind 集群 traincache 已存在,复用 =="; \
	else \
		echo "== 创建 kind 集群 =="; \
		kind create cluster --config $(ROOT)/k8s/kind-config.yaml; \
		for img in pgsty/minio:RELEASE.2026-08-04T00-00-00Z busybox:latest; do \
			kind load docker-image $$img --name traincache; \
		done; \
	fi
	@kubectl apply -f $(ROOT)/k8s/minio.yaml
	@kubectl -n minio rollout status deployment/minio --timeout=180s
	@if [ -z "$$(ls $(DATASET_DIR)/shard-*.tar 2>/dev/null)" ]; then \
		echo "== 生成数据集(16×256 MiB) =="; $(PY) $(ROOT)/scripts/synth_dataset.py; \
	else echo "== 数据集已存在,跳过 =="; fi
	@echo "== 生成倾斜清单 =="
	@$(PY) $(ROOT)/scripts/synth_tilt.py
	@if [ -z "$$(ls $(TRAIN_DIR)/shard-*.tar 2>/dev/null)" ]; then \
		echo "== 生成训练数据(16×128 MiB) =="; $(PY) $(ROOT)/scripts/synth_train.py; \
	else echo "== 训练数据已存在,跳过 =="; fi
	@echo "== 上传数据集到 MinIO =="
	@$(PY) $(ROOT)/scripts/upload_bench.py
	@bash $(ROOT)/scripts/ctrl_up.sh
	@bash $(ROOT)/scripts/tc_apply.sh
	@bash $(ROOT)/scripts/cold_reset.sh
	@bash $(ROOT)/scripts/verify_cold.sh
	@echo "== bench-up 完成:装置已拉起并通过冷读验证 =="
	@echo "   跑实验:python3 bench/run.py <族> [选项](python3 bench/run.py status 看进度)"

bench-doctor:
	@echo "== 本地语法检查 =="
	@$(PY_FALLBACK) -m py_compile $(PY_FILES)
	@bash $(ROOT)/scripts/doctor.sh

bench-down:
	@bash $(ROOT)/scripts/tc_teardown.sh
	@bash $(ROOT)/scripts/ctrl_down.sh
	@echo "== bench-down:tc 与控制面已拆;kind 集群与数据保留(删集群用 kind delete cluster --name traincache)=="
