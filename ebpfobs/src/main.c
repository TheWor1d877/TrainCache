// SPDX-License-Identifier: GPL-2.0
/* traintrace 用户态：块 I/O 延迟（M2）、major fault（M3）、filemap 预读配对（M4） */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <time.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/sysmacros.h>
#include <linux/limits.h>
#include <linux/types.h>

#include <bpf/libbpf.h>
#include <bpf/bpf.h>

#include "traintrace.skel.h"
#include "shared.h"

static volatile sig_atomic_t g_stop;
static void on_sigint(int sig) { (void)sig; g_stop = 1; }

#define MAX_CGROUPS 64
#define MAX_PIDS 64

struct config {
	const char *dev;
	const char *out_dir;
	const char *json_name;
	int duration;
	int poll_interval;
	int verbose;
	const char *cgroups[MAX_CGROUPS];
	int n_cgroups;
	int pids[MAX_PIDS];
	int n_pids;
};

/* ---- M2 直方图累计 ---- */
struct hist_entry { __u32 dev; __u8 rw; __u8 bucket; __u64 count; };
static struct hist_entry *g_hist;
static int g_hist_nr, g_hist_cap;

static void hist_add(__u32 dev, __u8 rw, __u8 bucket, __u64 n)
{
	for (int i = 0; i < g_hist_nr; i++)
		if (g_hist[i].dev == dev && g_hist[i].rw == rw && g_hist[i].bucket == bucket) {
			g_hist[i].count += n; return;
		}
	if (g_hist_nr == g_hist_cap) {
		g_hist_cap = g_hist_cap ? g_hist_cap * 2 : 64;
		g_hist = realloc(g_hist, g_hist_cap * sizeof(*g_hist));
	}
	g_hist[g_hist_nr++] = (struct hist_entry){ dev, rw, bucket, n };
}

/* ---- M3 fault 累计 ---- */
struct fault_entry { __u64 cgroup; __u32 pid; __u64 cnt; __u64 ns; };
static struct fault_entry *g_fault;
static int g_fault_nr, g_fault_cap;

static void fault_add(__u64 cg, __u32 pid, __u64 cnt, __u64 ns)
{
	for (int i = 0; i < g_fault_nr; i++)
		if (g_fault[i].cgroup == cg && g_fault[i].pid == pid) {
			g_fault[i].cnt += cnt; g_fault[i].ns += ns; return;
		}
	if (g_fault_nr == g_fault_cap) {
		g_fault_cap = g_fault_cap ? g_fault_cap * 2 : 64;
		g_fault = realloc(g_fault, g_fault_cap * sizeof(*g_fault));
	}
	g_fault[g_fault_nr++] = (struct fault_entry){ cg, pid, cnt, ns };
}

/* ---- M4 事件缓冲与区间集合 ---- */
static struct tev *g_events;
static int g_events_nr, g_events_cap;

static int handle_event(void *ctx, void *data, size_t len)
{
	(void)ctx;
	if (len < sizeof(struct tev))
		return 0;
	if (g_events_nr == g_events_cap) {
		g_events_cap = g_events_cap ? g_events_cap * 2 : 4096;
		g_events = realloc(g_events, g_events_cap * sizeof(*g_events));
	}
	g_events[g_events_nr++] = *(struct tev *)data;
	return 0;
}

struct interval { __u64 lo, hi; };
struct iset { struct interval *v; int n, cap; };

static void iset_add(struct iset *s, __u64 lo, __u64 hi)
{
	if (lo > hi)
		return;
	/* 找插入点并合并重叠/相邻区间 */
	int i = 0;
	while (i < s->n && s->v[i].hi + 1 < lo)
		i++;
	int j = i;
	while (j < s->n && s->v[j].lo <= hi + 1) {
		if (s->v[j].lo < lo) lo = s->v[j].lo;
		if (s->v[j].hi > hi) hi = s->v[j].hi;
		j++;
	}
	int removed = j - i;
	if (removed > 0) {
		memmove(&s->v[i + 1], &s->v[j], (s->n - j) * sizeof(*s->v));
		s->n -= removed - 1;
	} else {
		if (s->n == s->cap) {
			s->cap = s->cap ? s->cap * 2 : 64;
			s->v = realloc(s->v, s->cap * sizeof(*s->v));
		}
		memmove(&s->v[i + 1], &s->v[i], (s->n - i) * sizeof(*s->v));
		s->n++;
	}
	s->v[i] = (struct interval){ lo, hi };
}

static __u64 iset_total(const struct iset *s)
{
	__u64 t = 0;
	for (int i = 0; i < s->n; i++)
		t += s->v[i].hi - s->v[i].lo + 1;
	return t;
}

/* a 与 b 交集的总页数 */
static __u64 iset_intersect(const struct iset *a, const struct iset *b)
{
	__u64 t = 0;
	int i = 0, j = 0;
	while (i < a->n && j < b->n) {
		__u64 lo = a->v[i].lo > b->v[j].lo ? a->v[i].lo : b->v[j].lo;
		__u64 hi = a->v[i].hi < b->v[j].hi ? a->v[i].hi : b->v[j].hi;
		if (lo <= hi)
			t += hi - lo + 1;
		if (a->v[i].hi < b->v[j].hi) i++; else j++;
	}
	return t;
}

struct ra_group { __u32 dev; __u64 ino; struct iset added, consumed; };
static struct ra_group *g_groups;
static int g_groups_nr, g_groups_cap;

static struct ra_group *group_get(__u32 dev, __u64 ino)
{
	for (int i = 0; i < g_groups_nr; i++)
		if (g_groups[i].dev == dev && g_groups[i].ino == ino)
			return &g_groups[i];
	if (g_groups_nr == g_groups_cap) {
		g_groups_cap = g_groups_cap ? g_groups_cap * 2 : 16;
		g_groups = realloc(g_groups, g_groups_cap * sizeof(*g_groups));
	}
	struct ra_group *g = &g_groups[g_groups_nr++];
	memset(g, 0, sizeof(*g));
	g->dev = dev; g->ino = ino;
	return g;
}

static void pair_readahead(void)
{
	for (int i = 0; i < g_events_nr; i++) {
		struct tev *e = &g_events[i];
		struct ra_group *g = group_get(e->dev, e->ino);
		if (e->kind == EV_ADD) {
			__u64 pages = (__u64)1 << e->order;
			iset_add(&g->added, e->index, e->index + pages - 1);
		} else if (e->kind == EV_FAULT) {
			iset_add(&g->consumed, e->index, e->index);
		} else if (e->kind == EV_GETP) {
			iset_add(&g->consumed, e->index, e->last_index);
		}
	}
}

/* ---- 参数 ---- */
static void usage(const char *prog)
{
	fprintf(stderr,
		"用法: %s --dev PATH [--cgroup PATH ...] [--pid PID ...]\n"
		"        [--duration SEC] [--poll-interval SEC] [--out-dir DIR] [--json NAME] [--verbose]\n", prog);
}

static int parse_args(int argc, char **argv, struct config *cfg)
{
	memset(cfg, 0, sizeof(*cfg));
	cfg->duration = 60; cfg->poll_interval = 1; cfg->json_name = "metrics.json";
	for (int i = 1; i < argc; i++) {
		if (!strcmp(argv[i], "--dev") && i + 1 < argc) cfg->dev = argv[++i];
		else if (!strcmp(argv[i], "--out-dir") && i + 1 < argc) cfg->out_dir = argv[++i];
		else if (!strcmp(argv[i], "--json") && i + 1 < argc) cfg->json_name = argv[++i];
		else if (!strcmp(argv[i], "--duration") && i + 1 < argc) cfg->duration = atoi(argv[++i]);
		else if (!strcmp(argv[i], "--poll-interval") && i + 1 < argc) cfg->poll_interval = atoi(argv[++i]);
		else if (!strcmp(argv[i], "--verbose")) cfg->verbose = 1;
		else if (!strcmp(argv[i], "--cgroup") && i + 1 < argc) {
			if (cfg->n_cgroups < MAX_CGROUPS) cfg->cgroups[cfg->n_cgroups++] = argv[++i];
		} else if (!strcmp(argv[i], "--pid") && i + 1 < argc) {
			if (cfg->n_pids < MAX_PIDS) cfg->pids[cfg->n_pids++] = atoi(argv[++i]);
		} else { usage(argv[0]); return -1; }
	}
	if (!cfg->dev) { fprintf(stderr, "错误: --dev 必填\n"); usage(argv[0]); return -1; }
	return 0;
}

static int resolve_dev(const char *dev, __u32 *raw, char *name, size_t name_sz)
{
	struct stat st;
	int major, minor;
	const char *base = strrchr(dev, '/');
	base = base ? base + 1 : dev;
	if (!strncmp(dev, "/dev/", 5) || !strchr(dev, '/')) {
		char syspath[PATH_MAX];
		snprintf(syspath, sizeof(syspath), "/sys/class/block/%s/dev", base);
		FILE *f = fopen(syspath, "r");
		if (!f) { fprintf(stderr, "无法读 %s: %s\n", syspath, strerror(errno)); return -1; }
		if (fscanf(f, "%d:%d", &major, &minor) != 2) { fclose(f); return -1; }
		fclose(f);
		*raw = ((__u32)major << 20) | (__u32)minor;
		snprintf(name, name_sz, "%s", base);
		return 0;
	}
	if (stat(dev, &st)) { fprintf(stderr, "stat %s 失败: %s\n", dev, strerror(errno)); return -1; }
	major = major(st.st_dev); minor = minor(st.st_dev);
	*raw = ((__u32)major << 20) | (__u32)minor;
	snprintf(name, name_sz, "%s", base);
	return 0;
}

/* 由内核 dev_t 反查块设备名（扫 /sys/class/block），查不到回退十六进制 */
static void dev_name_from_raw(__u32 raw, char *out, size_t n)
{
	int major = raw >> 20, minor = raw & 0xfffff;
	DIR *d = opendir("/sys/class/block");
	if (d) {
		struct dirent *de;
		while ((de = readdir(d))) {
			if (de->d_name[0] == '.') continue;
			char p[PATH_MAX];
			snprintf(p, sizeof(p), "/sys/class/block/%s/dev", de->d_name);
			FILE *f = fopen(p, "r");
			if (!f) continue;
			int mj = -1, mn = -1;
			if (fscanf(f, "%d:%d", &mj, &mn) == 2 && mj == major && mn == minor) {
				snprintf(out, n, "%.*s", (int)(n - 1), de->d_name);
				fclose(f); closedir(d); return;
			}
			fclose(f);
		}
		closedir(d);
	}
	snprintf(out, n, "0x%x", raw);
}

static const char *rw_name(__u8 rw) { return rw == 0 ? "read" : rw == 1 ? "write" : "other"; }

/* ---- cgroup 过滤集合 ---- */
static int cgroup_id_from_path(const char *path, __u64 *id)
{
	struct stat st;
	if (stat(path, &st)) { fprintf(stderr, "stat cgroup %s 失败: %s\n", path, strerror(errno)); return -1; }
	*id = st.st_ino;
	return 0;
}

static int cgroup_id_from_pid(int pid, __u64 *id)
{
	char p[64], line[512];
	snprintf(p, sizeof(p), "/proc/%d/cgroup", pid);
	FILE *f = fopen(p, "r");
	if (!f) { fprintf(stderr, "无法读 %s: %s\n", p, strerror(errno)); return -1; }
	char rel[PATH_MAX] = "";
	while (fgets(line, sizeof(line), f)) {
		if (!strncmp(line, "0::", 3)) {
			snprintf(rel, sizeof(rel), "%s", line + 3);
			rel[strcspn(rel, "\n")] = 0;
			break;
		}
	}
	fclose(f);
	if (!rel[0]) { fprintf(stderr, "pid %d 无 cgroup v2 路径\n", pid); return -1; }
	char full[PATH_MAX];
	snprintf(full, sizeof(full), "/sys/fs/cgroup%s", rel);
	return cgroup_id_from_path(full, id);
}

static void populate_watch_cgroup(struct traintrace_bpf *skel, struct config *cfg)
{
	int fd = bpf_map__fd(skel->maps.watch_cgroup);
	__u8 one = 1;
	for (int i = 0; i < cfg->n_cgroups; i++) {
		__u64 id;
		if (!cgroup_id_from_path(cfg->cgroups[i], &id))
			bpf_map_update_elem(fd, &id, &one, BPF_ANY);
	}
	for (int i = 0; i < cfg->n_pids; i++) {
		__u64 id;
		if (!cgroup_id_from_pid(cfg->pids[i], &id))
			bpf_map_update_elem(fd, &id, &one, BPF_ANY);
	}
}

/* ---- 拉取并清零聚合 map ---- */
static void drain_blk_hist(struct traintrace_bpf *skel, int ncpu)
{
	int fd = bpf_map__fd(skel->maps.blk_hist);
	__u64 *vals = calloc(ncpu, sizeof(__u64));
	struct blk_key key, next;
	bool have = bpf_map_get_next_key(fd, NULL, &key) == 0;
	while (have) {
		bool hn = bpf_map_get_next_key(fd, &key, &next) == 0;
		if (bpf_map_lookup_elem(fd, &key, vals) == 0) {
			__u64 sum = 0;
			for (int c = 0; c < ncpu; c++) sum += vals[c];
			if (sum) hist_add(key.dev, key.rw, key.bucket, sum);
		}
		bpf_map_delete_elem(fd, &key);
		if (!hn) break;
		key = next;
	}
	free(vals);
}

static void drain_fault_agg(struct traintrace_bpf *skel)
{
	int fd = bpf_map__fd(skel->maps.fault_agg);
	struct fault_key key, next;
	struct fault_val val;
	bool have = bpf_map_get_next_key(fd, NULL, &key) == 0;
	while (have) {
		bool hn = bpf_map_get_next_key(fd, &key, &next) == 0;
		if (bpf_map_lookup_elem(fd, &key, &val) == 0)
			fault_add(key.cgroup_id, key.pid, val.major_cnt, val.major_ns);
		bpf_map_delete_elem(fd, &key);
		if (!hn) break;
		key = next;
	}
}

static __u64 read_rb_drop(struct traintrace_bpf *skel, int ncpu)
{
	int fd = bpf_map__fd(skel->maps.rb_drop);
	__u32 z = 0;
	__u64 *vals = calloc(ncpu, sizeof(__u64));
	__u64 sum = 0;
	if (bpf_map_lookup_elem(fd, &z, vals) == 0)
		for (int c = 0; c < ncpu; c++) sum += vals[c];
	free(vals);
	return sum;
}

static void read_comm(__u32 pid, char *out, size_t n)
{
	char p[64];
	snprintf(p, sizeof(p), "/proc/%u/comm", pid);
	FILE *f = fopen(p, "r");
	out[0] = 0;
	if (f) {
		if (fgets(out, n, f)) out[strcspn(out, "\n")] = 0;
		fclose(f);
	}
	if (!out[0]) snprintf(out, n, "?");
}

/* ---- 输出 ---- */
static void write_metrics(const char *dir, const char *json_name, const char *dev_name,
			  struct config *cfg, __u64 start_epoch_ns, __u64 offset_ns, __u64 dropped)
{
	char path[512];
	snprintf(path, sizeof(path), "%s/%s", dir, json_name);
	FILE *f = fopen(path, "w");
	if (!f) { fprintf(stderr, "无法写 %s: %s\n", path, strerror(errno)); return; }

	fprintf(f, "{\n");
	fprintf(f, "  \"run\": { \"start_epoch_ns\": %llu, \"uptime_epoch_offset_ns\": %llu, \"dev\": \"%s\", \"duration_s\": %d, \"ringbuf_dropped\": %llu },\n",
		(unsigned long long)start_epoch_ns, (unsigned long long)offset_ns, dev_name, cfg->duration, (unsigned long long)dropped);

	/* block_io */
	fprintf(f, "  \"block_io\": { \"by_dev_rw\": {\n");
	int gp = 0;
	for (int i = 0; i < g_hist_nr; i++) {
		int first = 1;
		for (int j = 0; j < i; j++)
			if (g_hist[j].dev == g_hist[i].dev && g_hist[j].rw == g_hist[i].rw) { first = 0; break; }
		if (!first) continue;
		char nm[64], key[128];
		dev_name_from_raw(g_hist[i].dev, nm, sizeof(nm));
		snprintf(key, sizeof(key), "%s.%s", nm, rw_name(g_hist[i].rw));
		fprintf(f, "%s \"%s\": [\n", gp++ ? "    ," : "    ", key);
		int fb = 1;
		for (int b = 0; b <= 31; b++)
			for (int k = 0; k < g_hist_nr; k++) {
				if (g_hist[k].dev != g_hist[i].dev || g_hist[k].rw != g_hist[i].rw ||
				    g_hist[k].bucket != b || g_hist[k].count == 0) continue;
				fprintf(f, "      %s{ \"bucket\": %d, \"bucket_ns_lo\": %llu, \"bucket_ns_hi\": %llu, \"count\": %llu }\n",
					fb ? "" : ", ", b, (unsigned long long)((__u64)1 << b),
					(unsigned long long)((__u64)1 << (b + 1)), (unsigned long long)g_hist[k].count);
				fb = 0;
			}
		fprintf(f, "    ]");
	}
	fprintf(f, "\n  } },\n");

	/* major_fault */
	fprintf(f, "  \"major_fault\": { \"readahead_agnostic\": true, \"by_cgroup_pid\": {\n");
	for (int i = 0; i < g_fault_nr; i++) {
		char comm[32];
		read_comm(g_fault[i].pid, comm, sizeof(comm));
		double mean_us = g_fault[i].cnt ? (double)g_fault[i].ns / g_fault[i].cnt / 1000.0 : 0;
		fprintf(f, "    %s\"%llu/%u\": { \"pid\": %u, \"comm\": \"%s\", \"count\": %llu, \"mean_us\": %.3f }\n",
			i ? "," : "", (unsigned long long)g_fault[i].cgroup, g_fault[i].pid, g_fault[i].pid, comm,
			(unsigned long long)g_fault[i].cnt, mean_us);
	}
	fprintf(f, "  } },\n");

	/* readahead */
	fprintf(f, "  \"readahead\": { \"by_dev_ino\": {\n");
	for (int i = 0; i < g_groups_nr; i++) {
		struct ra_group *g = &g_groups[i];
		__u64 added = iset_total(&g->added);
		__u64 consumed = iset_intersect(&g->consumed, &g->added);
		__u64 invalid = added > consumed ? added - consumed : 0;
		double ratio = added ? (double)invalid / added * 100.0 : 0;
		char nm[64];
		dev_name_from_raw(g->dev, nm, sizeof(nm));
		fprintf(f, "    %s\"%s:%llu\": { \"added_pages\": %llu, \"consumed_pages\": %llu, \"invalid_pages\": %llu, \"invalid_ratio_pct\": %.2f }\n",
			i ? "," : "", nm, (unsigned long long)g->ino,
			(unsigned long long)added, (unsigned long long)consumed, (unsigned long long)invalid, ratio);
	}
	fprintf(f, "  } }\n");
	fprintf(f, "}\n");
	fclose(f);
	printf("指标已写出: %s\n", path);
}

static void print_summary(struct config *cfg, __u64 dropped)
{
	printf("\n=== 摘要 (%ds, filemap 事件 %d, 丢事件 %llu) ===\n",
	       cfg->duration, g_events_nr, (unsigned long long)dropped);
	for (int i = 0; i < g_fault_nr; i++) {
		char comm[32]; read_comm(g_fault[i].pid, comm, sizeof(comm));
		printf("  major fault pid=%u comm=%s count=%llu mean=%.1fus\n",
		       g_fault[i].pid, comm, (unsigned long long)g_fault[i].cnt,
		       g_fault[i].cnt ? (double)g_fault[i].ns / g_fault[i].cnt / 1000.0 : 0);
	}
	for (int i = 0; i < g_groups_nr; i++) {
		struct ra_group *g = &g_groups[i];
		__u64 added = iset_total(&g->added);
		__u64 consumed = iset_intersect(&g->consumed, &g->added);
		__u64 invalid = added > consumed ? added - consumed : 0;
		printf("  readahead ino=%llu added=%llu consumed=%llu invalid=%llu (%.1f%%)\n",
		       (unsigned long long)g->ino, (unsigned long long)added,
		       (unsigned long long)consumed, (unsigned long long)invalid,
		       added ? (double)invalid / added * 100.0 : 0);
	}
}

int main(int argc, char **argv)
{
	struct config cfg;
	struct traintrace_bpf *skel = NULL;
	struct ring_buffer *rb = NULL;
	char dir_buf[256], dev_name[64];
	const char *out_dir;
	__u32 raw_dev = 0;
	__u64 start_epoch_ns = 0, offset_ns = 0;

	if (parse_args(argc, argv, &cfg)) return 1;
	if (resolve_dev(cfg.dev, &raw_dev, dev_name, sizeof(dev_name))) return 1;

	out_dir = cfg.out_dir;
	if (!out_dir) {
		snprintf(dir_buf, sizeof(dir_buf), "traintrace-%lld-%d", (long long)time(NULL), (int)getpid());
		out_dir = dir_buf;
	}
	if (mkdir(out_dir, 0755) && errno != EEXIST) {
		fprintf(stderr, "无法创建目录 %s: %s\n", out_dir, strerror(errno)); return 1;
	}

	{
		struct timespec rt;
		FILE *up = fopen("/proc/uptime", "r");
		double secs = 0;
		if (up) { if (fscanf(up, "%lf", &secs) != 1) secs = 0; fclose(up); }
		clock_gettime(CLOCK_REALTIME, &rt);
		__u64 epoch_ns = (__u64)rt.tv_sec * 1000000000ULL + rt.tv_nsec;
		offset_ns = epoch_ns - (__u64)(secs * 1e9);
		start_epoch_ns = epoch_ns;
	}

	skel = traintrace_bpf__open();
	if (!skel) { fprintf(stderr, "skeleton open 失败\n"); return 1; }
	if (traintrace_bpf__load(skel)) { fprintf(stderr, "skeleton load 失败\n"); goto out; }

	{
		__u32 zero = 0;
		struct dev_val dv = { .encoded_dev = raw_dev };
		if (bpf_map_update_elem(bpf_map__fd(skel->maps.watch_dev), &zero, &dv, BPF_ANY)) {
			fprintf(stderr, "写 watch_dev 失败\n"); goto out;
		}
	}
	populate_watch_cgroup(skel, &cfg);

	if (traintrace_bpf__attach(skel)) { fprintf(stderr, "skeleton attach 失败\n"); goto out; }

	int ncpu = libbpf_num_possible_cpus();
	printf("探针装载成功 (dev=%s raw=0x%x, ncpu=%d, cgroup 过滤项=%d)\n",
	       dev_name, raw_dev, ncpu, cfg.n_cgroups + cfg.n_pids);

	rb = ring_buffer__new(bpf_map__fd(skel->maps.rb), handle_event, NULL, NULL);
	if (!rb) { fprintf(stderr, "ring_buffer 创建失败\n"); goto out; }

	signal(SIGINT, on_sigint);
	time_t end = time(NULL) + cfg.duration;
	time_t last_drain = time(NULL);
	while (!g_stop && (cfg.duration == 0 || time(NULL) < end)) {
		ring_buffer__poll(rb, 200);
		time_t now = time(NULL);
		if (now - last_drain >= cfg.poll_interval) {
			drain_blk_hist(skel, ncpu);
			drain_fault_agg(skel);
			last_drain = now;
		}
	}
	ring_buffer__poll(rb, 0);
	drain_blk_hist(skel, ncpu);
	drain_fault_agg(skel);

	pair_readahead();
	__u64 dropped = read_rb_drop(skel, ncpu);
	write_metrics(out_dir, cfg.json_name, dev_name, &cfg, start_epoch_ns, offset_ns, dropped);
	print_summary(&cfg, dropped);

out:
	ring_buffer__free(rb);
	traintrace_bpf__destroy(skel);
	free(g_hist); free(g_fault); free(g_events);
	for (int i = 0; i < g_groups_nr; i++) { free(g_groups[i].added.v); free(g_groups[i].consumed.v); }
	free(g_groups);
	return 0;
}
