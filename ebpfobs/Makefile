# traintrace 构建：vmlinux.h → bpf.o → skeleton → 静态二进制
ARCH    ?= x86
CLANG   ?= clang
CC      ?= gcc
BPFTOOL ?= bpftool

SRC     := src
BUILD   := build
VENDOR  := vendor/libbpf
LB_DEST := $(VENDOR)/output

LIBBPF_A   := $(LB_DEST)/usr/lib64/libbpf.a
LIBBPF_INC := $(LB_DEST)/usr/include
VMLINUX_H := $(BUILD)/vmlinux.h
BPF_OBJ   := $(BUILD)/traintrace.bpf.o
SKEL_H    := $(BUILD)/traintrace.skel.h
OUT       := $(BUILD)/traintrace

BPFCFLAGS := -target bpf -D__TARGET_ARCH_$(ARCH) -I$(BUILD) -I$(LIBBPF_INC) -I$(SRC) -g -O2 -Wno-missing-declarations
APP_INC   := -I$(BUILD) -I$(LIBBPF_INC) -I$(SRC)
APP_LIBS  := $(LIBBPF_A) -lelf -lz -lzstd -lm
APP_CFLAGS := -g -O2 -Wall -static

all: $(OUT)

$(BUILD):
	@mkdir -p $(BUILD)

$(VMLINUX_H): | $(BUILD)
	$(BPFTOOL) btf dump file /sys/kernel/btf/vmlinux format c > $@

$(LIBBPF_A):
	@echo "  LIBBPF static"
	$(MAKE) -C $(VENDOR)/src BUILD_STATIC_ONLY=1 EXTRA_CFLAGS=-Wno-error \
	    DESTDIR=$(abspath $(LB_DEST)) install

$(BPF_OBJ): $(SRC)/traintrace.bpf.c $(SRC)/shared.h $(VMLINUX_H) $(LIBBPF_A)
	@echo "  BPF    $@"
	$(CLANG) $(BPFCFLAGS) -c $< -o $@

$(SKEL_H): $(BPF_OBJ)
	@echo "  SKEL   $@"
	$(BPFTOOL) gen skeleton $< > $@

$(OUT): $(SRC)/main.c $(SKEL_H) $(LIBBPF_A)
	@echo "  LD     $@"
	$(CC) $(APP_CFLAGS) $(APP_INC) $(SRC)/main.c $(APP_LIBS) -o $@

clean:
	rm -f $(BUILD)/*.o $(BUILD)/vmlinux.h $(BUILD)/traintrace.skel.h $(BUILD)/traintrace

distclean: clean
	rm -rf $(LB_DEST)
	$(MAKE) -C $(VENDOR)/src clean

.PHONY: all clean distclean
