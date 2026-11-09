// SPDX-License-Identifier: GPL-2.0
/* 内核侧：块 I/O 延迟探针族（M2） */
#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include "shared.h"

/* block_rq_issue / block_rq_complete 的展平记录，按 tracepoint format 偏移定义 */
struct blk_issue_raw {
	__u16 type;
	__u8  flags;
	__u8  preempt_count;
	int   pid;
	__u32 dev;          /* offset 8  内核 dev_t */
	__u32 pad0;         /* offset 12 */
	__u64 sector;       /* offset 16 */
	__u32 nr_sector;    /* offset 24 */
	__u32 bytes;        /* offset 28 */
	__u16 ioprio;       /* offset 32 */
	char  rwbs[10];     /* offset 34 */
	char  comm[16];     /* offset 44 */
};

struct blk_complete_raw {
	__u16 type;
	__u8  flags;
	__u8  preempt_count;
	int   pid;
	__u32 dev;          /* offset 8  内核 dev_t */
	__u32 pad0;         /* offset 12 */
	__u64 sector;       /* offset 16 */
	__u32 nr_sector;    /* offset 24 */
	int   error;        /* offset 28 */
	__u16 ioprio;       /* offset 32 */
	char  rwbs[10];     /* offset 34 */
};

struct {
	__uint(type, BPF_MAP_TYPE_LRU_HASH);
	__type(key, struct rq_key);
	__type(value, __u64);
	__uint(max_entries, 1048576);
} rq_start SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_PERCPU_HASH);
	__type(key, struct blk_key);
	__type(value, __u64);
	__uint(max_entries, 4096);
} blk_hist SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_ARRAY);
	__type(key, __u32);
	__type(value, struct dev_val);
	__uint(max_entries, 1);
} watch_dev SEC(".maps");

static __always_inline int dev_is_watched(__u32 dev)
{
	__u32 zero = 0;
	struct dev_val *v = bpf_map_lookup_elem(&watch_dev, &zero);
	return v && v->encoded_dev == dev;
}

/* rwbs 首字母映射成读写维度：R=0 W=1 其它=2 */
static __always_inline __u8 rw_dim(char c)
{
	if (c == 'R')
		return 0;
	if (c == 'W')
		return 1;
	return 2;
}

/* 纳秒延迟取 log2 桶下标，上限 31 */
static __always_inline __u8 lat_bucket(__u64 dt_ns)
{
	if (dt_ns == 0)
		return 0;
	return 63 - __builtin_clzll(dt_ns);
}

SEC("tracepoint/block/block_rq_issue")
int do_blk_issue(struct blk_issue_raw *ctx)
{
	if (!dev_is_watched(ctx->dev))
		return 0;

	struct rq_key k = { .dev = ctx->dev, .sector = ctx->sector };
	__u64 ts = bpf_ktime_get_ns();
	bpf_map_update_elem(&rq_start, &k, &ts, BPF_ANY);
	return 0;
}

SEC("tracepoint/block/block_rq_complete")
int do_blk_complete(struct blk_complete_raw *ctx)
{
	if (!dev_is_watched(ctx->dev))
		return 0;

	struct rq_key k = { .dev = ctx->dev, .sector = ctx->sector };
	__u64 *start = bpf_map_lookup_elem(&rq_start, &k);
	if (!start)
		return 0;

	__u64 dt = bpf_ktime_get_ns() - *start;
	bpf_map_delete_elem(&rq_start, &k);

	struct blk_key hk = {
		.dev    = ctx->dev,
		.rw     = rw_dim(ctx->rwbs[0]),
		.bucket = lat_bucket(dt),
	};
	__u64 *cnt = bpf_map_lookup_elem(&blk_hist, &hk);
	if (cnt)
		*cnt += 1;
	else
		bpf_map_update_elem(&blk_hist, &hk, &(__u64){ 1 }, BPF_ANY);
	return 0;
}

char LICENSE[] SEC("license") = "GPL";
