// SPDX-License-Identifier: GPL-2.0
/* 内核侧：块 I/O 延迟（M2）、major fault（M3）、filemap 预读（M4） */
#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include "shared.h"

/* ---- 块 tracepoint 展平记录（按 format 偏移定义） ---- */
struct blk_issue_raw {
	__u16 type; __u8 flags; __u8 preempt_count; int pid;
	__u32 dev; __u32 pad0; __u64 sector;
	__u32 nr_sector; __u32 bytes; __u16 ioprio; char rwbs[10]; char comm[16];
};
struct blk_complete_raw {
	__u16 type; __u8 flags; __u8 preempt_count; int pid;
	__u32 dev; __u32 pad0; __u64 sector;
	__u32 nr_sector; int error; __u16 ioprio; char rwbs[10];
};

/* ---- filemap tracepoint 展平记录 ---- */
struct fm_add_raw {
	__u16 type; __u8 flags; __u8 preempt_count; int pid;
	__u64 pfn; __u64 i_ino; __u64 index; __u32 s_dev; __u8 order;
};
struct fm_fault_raw {
	__u16 type; __u8 flags; __u8 preempt_count; int pid;
	__u64 i_ino; __u32 s_dev; __u32 pad; __u64 index;
};
struct fm_getp_raw {
	__u16 type; __u8 flags; __u8 preempt_count; int pid;
	__u64 i_ino; __u32 s_dev; __u32 pad; __u64 index; __u64 last_index;
};

/* ---- M2 maps ---- */
struct {
	__uint(type, BPF_MAP_TYPE_LRU_HASH);
	__type(key, struct rq_key); __type(value, __u64);
	__uint(max_entries, 1048576);
} rq_start SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_PERCPU_HASH);
	__type(key, struct blk_key); __type(value, __u64);
	__uint(max_entries, 4096);
} blk_hist SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_ARRAY);
	__type(key, __u32); __type(value, struct dev_val);
	__uint(max_entries, 1);
} watch_dev SEC(".maps");

/* ---- M3 maps ---- */
struct {
	__uint(type, BPF_MAP_TYPE_TASK_STORAGE);
	__uint(map_flags, BPF_F_NO_PREALLOC);
	__type(key, int); __type(value, __u64);
} mmf_ts SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__type(key, struct fault_key); __type(value, struct fault_val);
	__uint(max_entries, 4096);
} fault_agg SEC(".maps");

/* ---- M4 maps ---- */
struct {
	__uint(type, BPF_MAP_TYPE_RINGBUF);
	__uint(max_entries, 33554432);
} rb SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
	__type(key, __u32); __type(value, __u64);
	__uint(max_entries, 1);
} rb_drop SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__type(key, __u64); __type(value, __u8);
	__uint(max_entries, 256);
} watch_cgroup SEC(".maps");

/* ---- M2 块延迟 ---- */
static __always_inline int dev_is_watched(__u32 dev)
{
	__u32 zero = 0;
	struct dev_val *v = bpf_map_lookup_elem(&watch_dev, &zero);
	return v && v->encoded_dev == dev;
}
static __always_inline __u8 rw_dim(char c)
{
	return c == 'R' ? 0 : c == 'W' ? 1 : 2;
}
static __always_inline __u8 lat_bucket(__u64 dt)
{
	return dt == 0 ? 0 : 63 - __builtin_clzll(dt);
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
	struct blk_key hk = { .dev = ctx->dev, .rw = rw_dim(ctx->rwbs[0]), .bucket = lat_bucket(dt) };
	__u64 *cnt = bpf_map_lookup_elem(&blk_hist, &hk);
	if (cnt)
		*cnt += 1;
	else
		bpf_map_update_elem(&blk_hist, &hk, &(__u64){ 1 }, BPF_ANY);
	return 0;
}

/* ---- M3 major fault ---- */
SEC("kprobe/handle_mm_fault")
int BPF_KPROBE(do_fault_enter)
{
	struct task_struct *t = (struct task_struct *)bpf_get_current_task_btf();
	__u64 *ts = bpf_task_storage_get(&mmf_ts, t, NULL, BPF_LOCAL_STORAGE_GET_F_CREATE);
	if (ts)
		*ts = bpf_ktime_get_ns();
	return 0;
}

SEC("kretprobe/handle_mm_fault")
int BPF_KRETPROBE(do_fault_return, unsigned int ret)
{
	if (!(ret & VM_FAULT_MAJOR))
		return 0;
	struct task_struct *t = (struct task_struct *)bpf_get_current_task_btf();
	__u64 *ts = bpf_task_storage_get(&mmf_ts, t, NULL, 0);
	__u64 dt = ts ? bpf_ktime_get_ns() - *ts : 0;
	if (ts)
		bpf_task_storage_delete(&mmf_ts, t);

	struct fault_key k = {
		.cgroup_id = bpf_get_current_cgroup_id(),
		.pid = bpf_get_current_pid_tgid() >> 32,
	};
	struct fault_val *v = bpf_map_lookup_elem(&fault_agg, &k);
	if (v) {
		v->major_cnt += 1;
		v->major_ns += dt;
	} else {
		struct fault_val nv = { .major_cnt = 1, .major_ns = dt };
		bpf_map_update_elem(&fault_agg, &k, &nv, BPF_ANY);
	}
	return 0;
}

/* ---- M4 filemap 预读 ---- */
static __always_inline int cgroup_watched(void)
{
	__u64 id = bpf_get_current_cgroup_id();
	return bpf_map_lookup_elem(&watch_cgroup, &id) != NULL;
}
static __always_inline void bump_drop(void)
{
	__u32 z = 0;
	__u64 *p = bpf_map_lookup_elem(&rb_drop, &z);
	if (p)
		__sync_fetch_and_add(p, 1);
}
static __always_inline void emit_tev(__u8 kind, __u64 ino, __u64 index,
				     __u64 last_index, __u32 dev, __u8 order)
{
	struct tev *e = bpf_ringbuf_reserve(&rb, sizeof(*e), 0);
	if (!e) {
		bump_drop();
		return;
	}
	e->nsecs = bpf_ktime_get_ns();
	e->cgroup_id = bpf_get_current_cgroup_id();
	e->ino = ino;
	e->index = index;
	e->last_index = last_index;
	e->dev = dev;
	e->pid = bpf_get_current_pid_tgid() >> 32;
	e->kind = kind;
	e->order = order;
	bpf_ringbuf_submit(e, 0);
}

SEC("tracepoint/filemap/mm_filemap_add_to_page_cache")
int do_add_to_cache(struct fm_add_raw *ctx)
{
	if (!cgroup_watched())
		return 0;
	emit_tev(EV_ADD, ctx->i_ino, ctx->index, 0, ctx->s_dev, ctx->order);
	return 0;
}

SEC("tracepoint/filemap/mm_filemap_fault")
int do_filemap_fault(struct fm_fault_raw *ctx)
{
	if (!cgroup_watched())
		return 0;
	emit_tev(EV_FAULT, ctx->i_ino, ctx->index, 0, ctx->s_dev, 0);
	return 0;
}

SEC("tracepoint/filemap/mm_filemap_get_pages")
int do_get_pages(struct fm_getp_raw *ctx)
{
	if (!cgroup_watched())
		return 0;
	emit_tev(EV_GETP, ctx->i_ino, ctx->index, ctx->last_index, ctx->s_dev, 0);
	return 0;
}

char LICENSE[] SEC("license") = "GPL";
