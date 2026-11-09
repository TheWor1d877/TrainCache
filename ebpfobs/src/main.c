// SPDX-License-Identifier: GPL-2.0
/* traintrace 用户态：M2 块 I/O 延迟聚合与落盘 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <time.h>
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

struct config {
	const char *dev;
	const char *out_dir;
	int duration;
	int poll_interval;
};

/* 用户态累计的直方图条目 */
struct hist_entry {
	__u32 dev;
	__u8  rw;
	__u8  bucket;
	__u64 count;
};
static struct hist_entry *g_hist;
static int g_hist_nr, g_hist_cap;

static void hist_add(__u32 dev, __u8 rw, __u8 bucket, __u64 n)
{
	for (int i = 0; i < g_hist_nr; i++) {
		if (g_hist[i].dev == dev && g_hist[i].rw == rw && g_hist[i].bucket == bucket) {
			g_hist[i].count += n;
			return;
		}
	}
	if (g_hist_nr == g_hist_cap) {
		g_hist_cap = g_hist_cap ? g_hist_cap * 2 : 64;
		g_hist = realloc(g_hist, g_hist_cap * sizeof(*g_hist));
	}
	g_hist[g_hist_nr++] = (struct hist_entry){ .dev = dev, .rw = rw, .bucket = bucket, .count = n };
}

static void usage(const char *prog)
{
	fprintf(stderr,
		"用法: %s --dev PATH [--out-dir DIR] [--duration SEC] [--poll-interval SEC]\n"
		"  M2：只出块 I/O 延迟直方图\n", prog);
}

static int parse_args(int argc, char **argv, struct config *cfg)
{
	memset(cfg, 0, sizeof(*cfg));
	cfg->duration = 60;
	cfg->poll_interval = 1;
	for (int i = 1; i < argc; i++) {
		if (!strcmp(argv[i], "--dev") && i + 1 < argc) cfg->dev = argv[++i];
		else if (!strcmp(argv[i], "--out-dir") && i + 1 < argc) cfg->out_dir = argv[++i];
		else if (!strcmp(argv[i], "--duration") && i + 1 < argc) cfg->duration = atoi(argv[++i]);
		else if (!strcmp(argv[i], "--poll-interval") && i + 1 < argc) cfg->poll_interval = atoi(argv[++i]);
		else { usage(argv[0]); return -1; }
	}
	if (!cfg->dev) { fprintf(stderr, "错误: --dev 必填\n"); usage(argv[0]); return -1; }
	return 0;
}

/* 把 --dev 解析成内核 dev_t（major<<20|minor），同时给出展示用名字 */
static int resolve_dev(const char *dev, __u32 *raw, char *name, size_t name_sz)
{
	struct stat st;
	int major, minor;
	const char *base = strrchr(dev, '/');
	base = base ? base + 1 : dev;

	/* 块设备节点或纯设备名 */
	if (!strncmp(dev, "/dev/", 5) || !strchr(dev, '/')) {
		char syspath[PATH_MAX];
		const char *n = base;
		/* /dev/nvme1n1p2 这类分区名去掉尾部 pN 求整盘名 */
		snprintf(syspath, sizeof(syspath), "/sys/class/block/%s/dev", n);
		FILE *f = fopen(syspath, "r");
		if (!f) {
			fprintf(stderr, "无法读 %s: %s\n", syspath, strerror(errno));
			return -1;
		}
		if (fscanf(f, "%d:%d", &major, &minor) != 2) { fclose(f); return -1; }
		fclose(f);
		*raw = ((__u32)major << 20) | (__u32)minor;
		snprintf(name, name_sz, "%s", n);
		return 0;
	}

	/* 挂载点：stat 取 st_dev */
	if (stat(dev, &st)) { fprintf(stderr, "stat %s 失败: %s\n", dev, strerror(errno)); return -1; }
	major = major(st.st_dev);
	minor = minor(st.st_dev);
	*raw = ((__u32)major << 20) | (__u32)minor;
	snprintf(name, name_sz, "%s", base);
	return 0;
}

static const char *rw_name(__u8 rw)
{
	return rw == 0 ? "read" : rw == 1 ? "write" : "other";
}

/* 迭代并清空 blk_hist，把每桶计数累进 g_hist */
static void drain_blk_hist(struct traintrace_bpf *skel, int ncpu)
{
	int fd = bpf_map__fd(skel->maps.blk_hist);
	__u64 *vals = calloc(ncpu, sizeof(__u64));
	struct blk_key key, next;
	bool have = bpf_map_get_next_key(fd, NULL, &key) == 0;

	while (have) {
		bool have_next = bpf_map_get_next_key(fd, &key, &next) == 0;
		if (bpf_map_lookup_elem(fd, &key, vals) == 0) {
			__u64 sum = 0;
			for (int c = 0; c < ncpu; c++)
				sum += vals[c];
			if (sum)
				hist_add(key.dev, key.rw, key.bucket, sum);
		}
		bpf_map_delete_elem(fd, &key);
		if (!have_next)
			break;
		key = next;
	}
	free(vals);
}

static void write_metrics(const char *dir, const char *dev_name, int duration,
			  __u64 start_epoch_ns, __u64 offset_ns)
{
	char path[512];
	snprintf(path, sizeof(path), "%s/metrics.json", dir);
	FILE *f = fopen(path, "w");
	if (!f) { fprintf(stderr, "无法写 %s: %s\n", path, strerror(errno)); return; }

	fprintf(f, "{\n");
	fprintf(f, "  \"run\": { \"start_epoch_ns\": %llu, \"uptime_epoch_offset_ns\": %llu, \"dev\": \"%s\", \"duration_s\": %d, \"stage\": \"M2\" },\n",
		(unsigned long long)start_epoch_ns, (unsigned long long)offset_ns, dev_name, duration);
	fprintf(f, "  \"block_io\": { \"by_dev_rw\": {\n");

	int groups_printed = 0;
	for (int i = 0; i < g_hist_nr; i++) {
		/* 只在每个 dev+rw 组的首条进入 */
		int first = 1;
		for (int j = 0; j < i; j++)
			if (g_hist[j].dev == g_hist[i].dev && g_hist[j].rw == g_hist[i].rw) { first = 0; break; }
		if (!first)
			continue;

		char key[128];
		snprintf(key, sizeof(key), "%s.%s", dev_name, rw_name(g_hist[i].rw));
		fprintf(f, "%s \"%s\": [\n", groups_printed++ ? "    ," : "    ", key);

		int first_bucket = 1;
		for (int b = 0; b <= 31; b++) {
			for (int k = 0; k < g_hist_nr; k++) {
				if (g_hist[k].dev != g_hist[i].dev || g_hist[k].rw != g_hist[i].rw ||
				    g_hist[k].bucket != b || g_hist[k].count == 0)
					continue;
				__u64 lo = (__u64)1 << b;
				__u64 hi = (__u64)1 << (b + 1);
				fprintf(f, "      %s{ \"bucket\": %d, \"bucket_ns_lo\": %llu, \"bucket_ns_hi\": %llu, \"count\": %llu }\n",
					first_bucket ? "" : ", ", b,
					(unsigned long long)lo, (unsigned long long)hi,
					(unsigned long long)g_hist[k].count);
				first_bucket = 0;
			}
		}
		fprintf(f, "    ]");
	}

	fprintf(f, "\n  } },\n");
	fprintf(f, "  \"major_fault\": {},\n");
	fprintf(f, "  \"readahead\": {}\n");
	fprintf(f, "}\n");
	fclose(f);
	printf("指标已写出: %s\n", path);
}

static void print_summary(int duration)
{
	printf("\n=== 块 I/O 延迟摘要 (%s, %ds) ===\n", "M2", duration);
	for (int i = 0; i < g_hist_nr; i++) {
		__u64 lo = (__u64)1 << g_hist[i].bucket;
		__u64 hi = (__u64)1 << (g_hist[i].bucket + 1);
		printf("  bucket %2u [%8llu, %8llu) ns  count %llu\n",
		       g_hist[i].bucket, (unsigned long long)lo, (unsigned long long)hi,
		       (unsigned long long)g_hist[i].count);
	}
}

int main(int argc, char **argv)
{
	struct config cfg;
	struct traintrace_bpf *skel;
	char dir_buf[256], dev_name[64];
	const char *out_dir;
	__u32 raw_dev = 0;
	__u64 start_epoch_ns = 0, offset_ns = 0;

	if (parse_args(argc, argv, &cfg))
		return 1;

	if (resolve_dev(cfg.dev, &raw_dev, dev_name, sizeof(dev_name)))
		return 1;

	out_dir = cfg.out_dir;
	if (!out_dir) {
		snprintf(dir_buf, sizeof(dir_buf), "traintrace-%lld-%d", (long long)time(NULL), (int)getpid());
		out_dir = dir_buf;
	}
	if (mkdir(out_dir, 0755) && errno != EEXIST) {
		fprintf(stderr, "无法创建目录 %s: %s\n", out_dir, strerror(errno));
		return 1;
	}

	/* 采样时钟偏移：epoch_ns - uptime_ns */
	{
		struct timespec rt, mono;
		FILE *up = fopen("/proc/uptime", "r");
		double secs = 0;
		if (up) { if (fscanf(up, "%lf", &secs) != 1) secs = 0; fclose(up); }
		__u64 uptime_ns = (__u64)(secs * 1e9);
		clock_gettime(CLOCK_REALTIME, &rt);
		__u64 epoch_ns = (__u64)rt.tv_sec * 1000000000ULL + rt.tv_nsec;
		offset_ns = epoch_ns - uptime_ns;
		start_epoch_ns = epoch_ns;
		(void)mono;
	}

	skel = traintrace_bpf__open();
	if (!skel) { fprintf(stderr, "skeleton open 失败\n"); return 1; }
	if (traintrace_bpf__load(skel)) { fprintf(stderr, "skeleton load 失败\n"); goto out; }

	/* 回填 watch_dev */
	{
		__u32 zero = 0;
		struct dev_val dv = { .encoded_dev = raw_dev };
		if (bpf_map_update_elem(bpf_map__fd(skel->maps.watch_dev), &zero, &dv, BPF_ANY)) {
			fprintf(stderr, "写 watch_dev 失败\n");
			goto out;
		}
	}

	if (traintrace_bpf__attach(skel)) { fprintf(stderr, "skeleton attach 失败\n"); goto out; }

	int ncpu = libbpf_num_possible_cpus();
	printf("探针装载成功 (dev=%s raw=0x%x, ncpu=%d)\n", dev_name, raw_dev, ncpu);

	signal(SIGINT, on_sigint);
	time_t end = time(NULL) + cfg.duration;
	while (!g_stop && (cfg.duration == 0 || time(NULL) < end)) {
		sleep(cfg.poll_interval);
		drain_blk_hist(skel, ncpu);
	}
	/* 收尾再做一次全量拉取 */
	drain_blk_hist(skel, ncpu);

	write_metrics(out_dir, dev_name, cfg.duration, start_epoch_ns, offset_ns);
	print_summary(cfg.duration);

out:
	traintrace_bpf__destroy(skel);
	free(g_hist);
	return 0;
}
