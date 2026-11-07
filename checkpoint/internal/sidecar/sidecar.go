// Package sidecar 实现 checkpointd 的保存与恢复编排（design 10.1「关键流程」）。
//
// 本包只做编排：扫描训练侧目录、维护保存队列、调用插件子进程、回填与
// 淘汰 NVMe 缓存、维护探针。插件协议、manifest、配置与日志分别由
// internal/plugin、internal/manifest、internal/config、internal/obs 负责。
package sidecar

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"traincache/checkpoint/internal/obs"
	"traincache/checkpoint/internal/plugin"
)

// 默认值来自 design 10.1「准入契约」、「部署与配置」与任务约定。
const (
	DefaultStateDir   = "/run/checkpoint/state"
	DefaultProbeFile  = "/run/checkpoint/ready"
	DefaultSocketPath = "/run/checkpoint/plugin.sock"
	DefaultPlugin     = "s3"

	defaultPluginRoot = "/opt/traincache/plugins"
	queueLimit        = 4

	defaultPollInterval    = 200 * time.Millisecond
	defaultScanInterval    = 20 * time.Second
	defaultScanStable      = 3
	defaultSaveDeadline    = 15 * time.Minute
	defaultRestoreDeadline = 10 * time.Minute
	defaultSlowRestore     = 5 * time.Minute
	defaultRetention       = 1

	// DefaultRestoreGiveUpAfter 是恢复放弃期限（10.2 D14）。cmd 的装配序把
	// RESTORE_DEADLINE 合进 Options 后才调 New，所以「必须大于恢复期限」
	// 在 New 里判定，非法值走退出码 2 的配置错误路径。
	DefaultRestoreGiveUpAfter = 60 * time.Minute
)

var (
	defaultSaveRetryIntervals    = []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 30 * time.Second, 30 * time.Second}
	defaultRestoreRetryIntervals = []time.Duration{5 * time.Second, 10 * time.Second, 15 * time.Second, 20 * time.Second, 25 * time.Second}
)

// Options 是 sidecar 的全部运行参数。路径、插件与后端来自环境变量；
// 时间参数由 cmd/checkpointd 从 config.Config 合并进来，测试可注入毫秒级值。
type Options struct {
	TaskID    string
	Rank      int
	WorldSize int
	Job       string // JOB，owner Job 名；裸 Pod 时为空串（design 10.2 D26）

	TaskDir   string // TRAINCACHE_CHECKPOINT_DIR，指向 <work>/<task_id>
	NVMEDir   string // NVME_DIR，缓存根目录；为空表示关闭缓存
	StateDir  string // STATE_DIR，续传状态根目录
	ProbeFile string // PROBE_FILE，探针标记文件

	Plugin     string
	PluginPath string
	SocketPath string

	Backend           json.RawMessage
	RequireCheckpoint bool
	NVMERetention     int
	CheckpointStep    *int64

	PollInterval          time.Duration
	ScanInterval          time.Duration
	ScanStable            int
	SaveDeadline          time.Duration
	RestoreDeadline       time.Duration
	RestoreGiveUpAfter    time.Duration // 到点仍未恢复成功则放弃并退出（D14）
	SaveRetryIntervals    []time.Duration
	RestoreRetryIntervals []time.Duration
	RestoreSlowInterval   time.Duration

	Logger *slog.Logger
}

// Plugin 是 sidecar 对插件子进程的最小依赖。生产实现是 plugin.Client，
// 测试注入内存假插件，不编译也不启动子进程。
type Plugin interface {
	Resolve(ctx context.Context, params plugin.ResolveParams) (plugin.ResolveResult, error)
	Save(ctx context.Context, params plugin.SaveParams) (plugin.TransferResult, error)
	Restore(ctx context.Context, params plugin.RestoreParams) (plugin.TransferResult, error)
	Close() error
}

// Factory 为一次保存或恢复操作启动一个插件子进程。onProgress 只对该次
// 操作有效；每次重试都重新 Start。
type Factory interface {
	Start(ctx context.Context, onProgress func(done, total int64)) (Plugin, error)
}

// Loader 把进程环境解析为 Options。生产实现是 EnvLoader。
type Loader interface {
	Load() (Options, error)
}

// EnvLoader 从环境变量加载 Options。
type EnvLoader struct{}

// Load 实现 Loader。
func (EnvLoader) Load() (Options, error) { return FromEnv() }

// PluginFactory 用 internal/plugin 启动真实插件进程。
type PluginFactory struct {
	Path       string
	SocketPath string
	Logger     *slog.Logger
}

// NewPluginFactory 构造生产用 Factory。
func NewPluginFactory(path, socketPath string, logger *slog.Logger) *PluginFactory {
	return &PluginFactory{Path: path, SocketPath: socketPath, Logger: logger}
}

// Start 启动插件并完成握手。
func (f *PluginFactory) Start(ctx context.Context, onProgress func(done, total int64)) (Plugin, error) {
	return plugin.Start(ctx, plugin.Options{
		Path:       f.Path,
		SocketPath: f.SocketPath,
		Logger:     f.Logger,
		OnProgress: onProgress,
	})
}

// Sidecar 是 checkpointd 的保存与恢复状态机。
type Sidecar struct {
	opts    Options
	log     *slog.Logger
	factory Factory
	probe   *Probe

	nvmeTaskDir string

	mu        sync.Mutex
	cache     map[int64]string // step -> NVMe 目录（已写 .complete）
	links     map[int64]string // step -> 本进程创建的软链目标
	queued    map[int64]bool   // 已入队或正在处理
	completed map[int64]bool   // 本进程已完成（保存成功或恢复成功）
	queue     []saveJob
	lastPoll  map[int64]int64 // 轮询看到的字节数
	scans     map[int64]scanState
	accepting bool
	failed    bool
	stopped   bool
	wake      chan struct{}
	cancelOp  context.CancelFunc

	lastQueueWarn time.Time

	runWG sync.WaitGroup
	bg    sync.WaitGroup
}

// New 构造 Sidecar 并应用默认值。目录的创建与可写性检查在 Run 的启动
// 序里做；这里只校验与文件系统无关的必填项。
func New(opts Options, factory Factory) (*Sidecar, error) {
	opts = normalize(opts)
	if strings.TrimSpace(opts.TaskID) == "" {
		return nil, fmt.Errorf("sidecar: task_id 不能为空")
	}
	if opts.TaskDir == "" {
		return nil, fmt.Errorf("sidecar: 工作目录不能为空")
	}
	if opts.WorldSize <= 0 {
		return nil, fmt.Errorf("sidecar: world_size=%d 需 > 0", opts.WorldSize)
	}
	if opts.Rank < 0 || opts.Rank >= opts.WorldSize {
		return nil, fmt.Errorf("sidecar: rank=%d 需在 [0,%d) 内", opts.Rank, opts.WorldSize)
	}
	if opts.CheckpointStep != nil && *opts.CheckpointStep < 0 {
		return nil, fmt.Errorf("sidecar: checkpoint step=%d 不能为负", *opts.CheckpointStep)
	}
	// 放弃期限必须大于恢复期限：慢速重试段的存在以恢复期限到点为起点，
	// 前者不大于后者时慢速段长度为零，恢复一失败就退出，等于绕过了
	// 「保留现场让人排障」的设计（design 10.2 D14）。这里判非法而不是取大。
	if opts.RestoreGiveUpAfter <= opts.RestoreDeadline {
		return nil, fmt.Errorf("sidecar: 恢复放弃期限 %s 需大于恢复期限 %s",
			opts.RestoreGiveUpAfter, opts.RestoreDeadline)
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	if factory == nil {
		factory = NewPluginFactory(opts.PluginPath, opts.SocketPath, log)
	}
	s := &Sidecar{
		opts:      opts,
		log:       log,
		factory:   factory,
		probe:     NewProbe(opts.ProbeFile, log),
		cache:     make(map[int64]string),
		links:     make(map[int64]string),
		queued:    make(map[int64]bool),
		completed: make(map[int64]bool),
		lastPoll:  make(map[int64]int64),
		scans:     make(map[int64]scanState),
		wake:      make(chan struct{}, 1),
	}
	if opts.NVMEDir != "" {
		s.nvmeTaskDir = filepath.Join(opts.NVMEDir, opts.TaskID)
	}
	return s, nil
}

// normalize 补齐默认值并复制重试间隔，避免调用方切片被后续修改影响。
func normalize(o Options) Options {
	if o.StateDir == "" {
		o.StateDir = DefaultStateDir
	}
	if o.ProbeFile == "" {
		o.ProbeFile = DefaultProbeFile
	}
	if o.Plugin == "" {
		o.Plugin = DefaultPlugin
	}
	if o.PluginPath == "" {
		o.PluginPath = filepath.Join(defaultPluginRoot, o.Plugin, o.Plugin)
	}
	if o.SocketPath == "" {
		o.SocketPath = DefaultSocketPath
	}
	if o.PollInterval <= 0 {
		o.PollInterval = defaultPollInterval
	}
	if o.ScanInterval <= 0 {
		o.ScanInterval = defaultScanInterval
	}
	if o.ScanStable <= 0 {
		o.ScanStable = defaultScanStable
	}
	if o.SaveDeadline <= 0 {
		o.SaveDeadline = defaultSaveDeadline
	}
	if o.RestoreDeadline <= 0 {
		o.RestoreDeadline = defaultRestoreDeadline
	}
	if o.RestoreSlowInterval <= 0 {
		o.RestoreSlowInterval = defaultSlowRestore
	}
	if o.RestoreGiveUpAfter <= 0 {
		o.RestoreGiveUpAfter = DefaultRestoreGiveUpAfter
	}
	if o.NVMERetention <= 0 {
		o.NVMERetention = defaultRetention
	}
	if len(o.Backend) == 0 {
		o.Backend = json.RawMessage("{}")
	}
	o.SaveRetryIntervals = append([]time.Duration(nil), o.SaveRetryIntervals...)
	if len(o.SaveRetryIntervals) == 0 {
		o.SaveRetryIntervals = append([]time.Duration(nil), defaultSaveRetryIntervals...)
	}
	o.RestoreRetryIntervals = append([]time.Duration(nil), o.RestoreRetryIntervals...)
	if len(o.RestoreRetryIntervals) == 0 {
		o.RestoreRetryIntervals = append([]time.Duration(nil), defaultRestoreRetryIntervals...)
	}
	return o
}

// Run 按 design 10.1「关键流程」的顺序启动：校验目录、登记缓存、探针 restoring、
// 恢复、探针 ready、进入保存监听。ctx 取消时做收尾并返回 nil。
func (s *Sidecar) Run(ctx context.Context) error {
	s.mu.Lock()
	s.accepting = false
	s.mu.Unlock()
	if err := s.prepare(); err != nil {
		return err
	}
	if err := s.registerCache(); err != nil {
		s.log.Warn("缓存登记失败，关闭缓存",
			append([]any{obs.Task(s.opts.TaskID), obs.Plugin(s.opts.Plugin), slog.String("reason", "cache_scan_failed")}, obs.Err(err)...)...)
		s.nvmeTaskDir = ""
	}
	if err := s.probe.Set(obs.StateRestoring); err != nil {
		return fmt.Errorf("sidecar: 写探针 restoring: %w", err)
	}

	if err := s.restore(ctx); err != nil {
		if ctx.Err() == nil {
			s.shutdown()
			return err
		}
		s.shutdown()
		return nil
	}
	if ctx.Err() != nil {
		s.shutdown()
		return nil
	}
	if err := s.probe.Set(obs.StateReady); err != nil {
		s.shutdown()
		return fmt.Errorf("sidecar: 写探针 ready: %w", err)
	}
	s.log.Info("checkpointd 恢复完成，进入保存监听",
		obs.Task(s.opts.TaskID), obs.StateAttr(obs.StateReady))

	s.mu.Lock()
	s.accepting = true
	s.mu.Unlock()

	s.runWG.Add(3)
	go func() { defer s.runWG.Done(); s.pollLoop(ctx) }()
	go func() { defer s.runWG.Done(); s.scanLoop(ctx) }()
	go func() { defer s.runWG.Done(); s.saveLoop(ctx) }()

	<-ctx.Done()
	s.shutdown()
	return nil
}

// prepare 创建并校验工作卷、NVMe 卷、状态目录、探针与 socket 的父目录。
// NVMe 不可写时关闭缓存并记 WARN，不阻断启动（design 10.1「关键流程」）。
func (s *Sidecar) prepare() error {
	// 训练容器在恢复完成后才启动，<task_id> 目录必须由 sidecar 先建出来。
	// 目录按组可写建：训练程序可能不是 65532，只有 fsGroup 组能写进去。
	if err := os.MkdirAll(s.opts.TaskDir, 0o775); err != nil {
		return fmt.Errorf("sidecar: 创建工作目录 %s: %w", s.opts.TaskDir, err)
	}
	info, err := os.Stat(s.opts.TaskDir)
	if err != nil {
		return fmt.Errorf("sidecar: 工作目录 %s: %w", s.opts.TaskDir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("sidecar: 工作目录 %s 不是目录", s.opts.TaskDir)
	}
	// 目录已存在时按 umask 建出来的模式不带组写权限，这里补上。
	if err := os.Chmod(s.opts.TaskDir, 0o775); err != nil {
		return fmt.Errorf("sidecar: 工作目录 %s 不能设为组可写: %w", s.opts.TaskDir, err)
	}
	if err := ensureWritable(s.opts.TaskDir); err != nil {
		return fmt.Errorf("sidecar: 工作目录 %s 不可写: %w", s.opts.TaskDir, err)
	}
	if err := ensureWritable(s.opts.StateDir); err != nil {
		return fmt.Errorf("sidecar: 状态目录 %s 不可写: %w", s.opts.StateDir, err)
	}
	if dir := filepath.Dir(s.opts.SocketPath); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("sidecar: 创建 socket 目录 %s: %w", dir, err)
		}
	}
	if dir := filepath.Dir(s.opts.ProbeFile); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("sidecar: 创建探针目录 %s: %w", dir, err)
		}
	}

	if s.opts.NVMEDir == "" {
		s.nvmeTaskDir = ""
		return nil
	}
	root := filepath.Join(s.opts.NVMEDir, s.opts.TaskID)
	if err := os.MkdirAll(root, 0o755); err != nil {
		s.disableCache(err)
		return nil
	}
	if err := ensureWritable(root); err != nil {
		s.disableCache(err)
		return nil
	}
	s.nvmeTaskDir = root
	return nil
}

func (s *Sidecar) disableCache(err error) {
	s.nvmeTaskDir = ""
	s.log.Warn("NVMe 不可写，关闭缓存",
		append([]any{obs.Task(s.opts.TaskID), slog.String("reason", "nvme_unwritable")}, obs.Err(err)...)...)
}

func ensureWritable(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".checkpointd-write-*")
	if err != nil {
		return err
	}
	name := f.Name()
	if err := f.Close(); err != nil {
		_ = os.Remove(name)
		return err
	}
	return os.Remove(name)
}

// registerCache 扫描 NVMe 上本任务的 step-* 目录，把带 .complete 的登记
// 进内存索引（design 10.1「数据格式」、「关键流程」）。
func (s *Sidecar) registerCache() error {
	if s.nvmeTaskDir == "" {
		return nil
	}
	entries, err := os.ReadDir(s.nvmeTaskDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	n := 0
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		step, ok := parseStepName(e.Name())
		if !ok {
			continue
		}
		dir := filepath.Join(s.nvmeTaskDir, e.Name())
		if !pathExists(filepath.Join(dir, completeMarker)) {
			continue
		}
		s.mu.Lock()
		s.cache[step] = dir
		s.mu.Unlock()
		n++
	}
	s.log.Info("缓存登记完成",
		obs.Task(s.opts.TaskID), obs.Plugin(s.opts.Plugin),
		slog.Int("steps", n), slog.Bool("nvme", true))
	return nil
}

// shutdown 停止接收新 step、取消在途插件调用、最多等 5 秒，然后删除
// socket 并把探针置为 failed（design 10.1「关键流程」）。
func (s *Sidecar) shutdown() {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	s.accepting = false
	cancel := s.cancelOp
	inflight := len(s.queued)
	s.mu.Unlock()

	s.log.Info("checkpointd 停止接收新 step", obs.Task(s.opts.TaskID), slog.Int("inflight", inflight))
	if cancel != nil {
		cancel()
	}

	done := make(chan struct{})
	go func() {
		s.runWG.Wait()
		s.bg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		s.log.Warn("等待在途操作超时", obs.Task(s.opts.TaskID), slog.Int("inflight", 1))
	}

	if err := s.probe.Set(obs.StateFailed); err != nil {
		s.log.Warn("写探针 failed 失败", append([]any{obs.Task(s.opts.TaskID)}, obs.Err(err)...)...)
	}
	if err := os.Remove(s.opts.SocketPath); err != nil && !os.IsNotExist(err) {
		s.log.Warn("删除 socket 失败",
			append([]any{obs.Task(s.opts.TaskID), slog.String("path", s.opts.SocketPath)}, obs.Err(err)...)...)
	}
}

// setCancel 登记当前插件操作的取消函数，shutdown 用它取消在途调用。
func (s *Sidecar) setCancel(cancel context.CancelFunc) {
	s.mu.Lock()
	s.cancelOp = cancel
	s.mu.Unlock()
}

func (s *Sidecar) clearCancel() {
	s.mu.Lock()
	s.cancelOp = nil
	s.mu.Unlock()
}

func (s *Sidecar) statePath(step int64) string {
	return filepath.Join(s.opts.StateDir, s.opts.TaskID, fmt.Sprintf("step-%d", step))
}

func (s *Sidecar) stepDir(step int64) string {
	return filepath.Join(s.opts.TaskDir, fmt.Sprintf("step-%d", step))
}

func parseStepName(name string) (int64, bool) {
	const prefix = "step-"
	if !strings.HasPrefix(name, prefix) {
		return 0, false
	}
	step, err := strconv.ParseInt(strings.TrimPrefix(name, prefix), 10, 64)
	if err != nil || step < 0 {
		return 0, false
	}
	return step, true
}

func pathExists(path string) bool {
	_, err := os.Lstat(path)
	return err == nil
}
