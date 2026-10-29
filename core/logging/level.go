package logging

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"
)

// LevelPath 是动态级别端点的固定路径。
const LevelPath = "/debug/loglevel"

// processLevel 是进程级日志级别。各组件构造 handler 时把它作为
// HandlerOptions.Level；运行中修改它对所有已构造的 logger 立即生效。
var processLevel = new(slog.LevelVar)

// Level 返回进程级 LevelVar，读写都直接作用在它上面。
func Level() *slog.LevelVar {
	return processLevel
}

// ParseLevel 把 debug/info/warn/error（大小写不敏感，允许前后空白）解析成
// slog.Level。ok 为 false 时返回值是 LevelInfo，调用方可以据此退化为 info。
func ParseLevel(s string) (slog.Level, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug, true
	case "info":
		return slog.LevelInfo, true
	case "warn":
		return slog.LevelWarn, true
	case "error":
		return slog.LevelError, true
	}
	return slog.LevelInfo, false
}

// levelName 把级别格式化成可写入响应与审计日志的小写形式。
func levelName(l slog.Level) string {
	return strings.ToLower(l.String())
}

// NewLevelHandler 返回 /debug/loglevel 的 HTTP handler：
//
//	GET -> 200 {"level":"<当前>"}
//	PUT {"level":"debug"} -> 合法则修改并 200，非法值 400
//	其余方法 -> 405
//
// 每次成功修改用 slog.Default() 写一条审计 Info，字段为 component、old、
// new、remote_addr。端点不鉴权，只能挂在回环地址上。
func NewLevelHandler(component string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(LevelPath, func(w http.ResponseWriter, r *http.Request) {
		serveLevel(w, r, component)
	})
	return mux
}

func serveLevel(w http.ResponseWriter, r *http.Request, component string) {
	switch r.Method {
	case http.MethodGet:
		writeLevel(w)
	case http.MethodPut:
		var req struct {
			Level string `json:"level"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<10)).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "body must be a JSON object with a level field")
			return
		}
		level, ok := ParseLevel(req.Level)
		if !ok {
			writeError(w, http.StatusBadRequest, "level must be one of debug/info/warn/error")
			return
		}
		old := Level().Level()
		Level().Set(level)
		slog.Default().Info("log level changed",
			"component", component,
			"old", levelName(old),
			"new", levelName(level),
			"remote_addr", r.RemoteAddr,
		)
		writeLevel(w)
	default:
		w.Header().Set("Allow", "GET, PUT")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func writeLevel(w http.ResponseWriter) {
	writeJSON(w, http.StatusOK, map[string]string{"level": levelName(Level().Level())})
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// ServeLevel 在 addr 上提供 /debug/loglevel，阻塞到 ctx 取消或监听失败。
// addr 为空表示关闭端点，立即返回。监听失败只记 Error，不返回错误：
// 动态级别是运维便利，不能成为业务依赖。
func ServeLevel(ctx context.Context, addr, component string, log *slog.Logger) {
	if addr == "" {
		return
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Error("debug level listener failed", "addr", addr, "err", err)
		return
	}
	srv := &http.Server{
		Handler:           NewLevelHandler(component),
		ReadHeaderTimeout: 5 * time.Second,
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("debug level server stopped", "addr", addr, "err", err)
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Warn("debug level server shutdown", "addr", addr, "err", err)
		}
	}
}
