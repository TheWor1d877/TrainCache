/* SPDX-License-Identifier: GPL-2.0 */
/* 内核侧与用户态共用的 map 键值结构、事件结构与枚举 */
#ifndef __SHARED_H
#define __SHARED_H

/* 类型 __u32/__u64 由包含方提供：内核侧经 vmlinux.h，用户态经 <linux/types.h> */

struct rq_key    { __u32 dev; __u64 sector; };
struct blk_key   { __u32 dev; __u8 rw; __u8 bucket; };
struct fault_key { __u64 cgroup_id; __u32 pid; };
struct fault_val { __u64 major_cnt; __u64 major_ns; };
struct dev_val   { __u32 encoded_dev; };

enum tev_kind { EV_ADD = 1, EV_FAULT = 2, EV_GETP = 3 };

struct tev {
	__u64 nsecs;      /* bpf_ktime_get_ns，配对与时间窗对齐用 */
	__u64 cgroup_id;  /* 发起进程 cgroup，归因用 */
	__u64 ino;        /* filemap 展平字段 ino，文件标识 */
	__u64 index;      /* 起始页索引（folio 首页） */
	__u64 last_index; /* 仅 EV_GETP 有效，消费覆盖的末页索引 */
	__u32 dev;        /* s_dev 编码设备号 */
	__u32 pid;        /* 发起进程 tgid */
	__u8  kind;       /* EV_ADD / EV_FAULT / EV_GETP */
	__u8  order;      /* 仅 EV_ADD 有效，本次进缓存是 2^order 个连续页 */
};

#endif /* __SHARED_H */
