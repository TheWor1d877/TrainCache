# ============================================================================
# bench/compare Makefile — 五组对照一键起拆(M6)
#
# 用法:
#   make compare-up GROUP=direct      起 direct 组
#   make compare-down GROUP=direct    拆 direct 组
#   make compare-all-up               依次起 direct juicefs fluid traincache
#   make compare-all-down             依次拆所有组
#   make juicefs-warm                 对 juicefs 组做预热(等价 DataLoad)
#   make trace                        生成统一 trace.json
#   make check                        语法检查 + trace selftest
#
# 组名:direct / juicefs / fluid / traincache
# (juicefs-cold 与 juicefs-warm 共用 juicefs 命名空间,warm.sh 负责预热)
#
# 环境变量(可选,覆盖 config.py 缺省):
#   TC_RATE / TC_DELAY / TC_BURST     tc 档位(五组共用)
#   MINIO_ACCESS_KEY / MINIO_SECRET_KEY  MinIO 凭证(不硬编码)
#   JFS_CACHE_MB / FLUID_CACHE_GI     缓存容量(与其他组对齐)
# ============================================================================
ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
PY := /usr/bin/python3
export PYTHONPATH := $(ROOT)/../.venv/lib/python3.12/site-packages:$(PYTHONPATH)

GROUPS := direct juicefs fluid traincache

.PHONY: compare-up compare-down compare-all-up compare-all-down \
        juicefs-warm trace check help

# ---- 组起拆入口 ----
compare-up:
	@if [ -z "$(GROUP)" ]; then \
		echo "错误:需指定 GROUP=$(GROUPS)"; exit 2; \
	fi
	@case "$(GROUP)" in \
		direct|juicefs|fluid|traincache) ;; \
		*) echo "错误:GROUP 必须是 $(GROUPS) 之一(收到 $(GROUP))"; exit 2 ;; \
	esac
	@echo "== compare-up GROUP=$(GROUP) =="
	@bash $(ROOT)/$(GROUP)/up.sh

compare-down:
	@if [ -z "$(GROUP)" ]; then \
		echo "错误:需指定 GROUP=$(GROUPS)"; exit 2; \
	fi
	@case "$(GROUP)" in \
		direct|juicefs|fluid|traincache) ;; \
		*) echo "错误:GROUP 必须是 $(GROUPS) 之一(收到 $(GROUP))"; exit 2 ;; \
	esac
	@echo "== compare-down GROUP=$(GROUP) =="
	@bash $(ROOT)/$(GROUP)/down.sh

# ---- 全量起拆 ----
compare-all-up:
	@for g in $(GROUPS); do \
		echo ""; \
		echo "############################################################"; \
		echo "## 起 $$g 组"; \
		echo "############################################################"; \
		$(MAKE) compare-up GROUP=$$g || { echo "$$g up 失败,终止"; exit 1; }; \
	done
	@echo "== 所有对照组已启动 =="

compare-all-down:
	@for g in $(GROUPS); do \
		echo ""; \
		echo "############################################################"; \
		echo "## 拆 $$g 组"; \
		echo "############################################################"; \
		$(MAKE) compare-down GROUP=$$g || true; \
	done
	@echo "== 所有对照组已拆除 =="

# ---- JuiceFS 预热(等价 Fluid DataLoad)----
juicefs-warm:
	@echo "== juicefs warmup(等价 DataLoad) =="
	@bash $(ROOT)/juicefs/warm.sh

# ---- 统一轨迹生成 ----
TRACE_OUT ?= $(ROOT)/traces/default.json
trace:
	@mkdir -p $(ROOT)/traces
	@$(PY) $(ROOT)/trace.py --out $(TRACE_OUT)
	@echo "trace.json → $(TRACE_OUT)"

# ---- 验收检查(语法 + selftest)----
check:
	@echo "== bash -n 语法检查 =="
	@for f in $(ROOT)/_common.sh \
	           $(ROOT)/direct/up.sh $(ROOT)/direct/down.sh \
	           $(ROOT)/juicefs/up.sh $(ROOT)/juicefs/warm.sh $(ROOT)/juicefs/down.sh \
	           $(ROOT)/fluid/up.sh $(ROOT)/fluid/down.sh \
	           $(ROOT)/traincache/up.sh $(ROOT)/traincache/down.sh; do \
		bash -n "$$f" && echo "  OK  $$f" || { echo "  FAIL $$f"; exit 1; }; \
	done
	@echo "== trace.py selftest =="
	@$(PY) -m py_compile $(ROOT)/trace.py && echo "  OK  py_compile"
	@$(PY) $(ROOT)/trace.py --selftest

help:
	@cat $(ROOT)/Makefile | head -20
