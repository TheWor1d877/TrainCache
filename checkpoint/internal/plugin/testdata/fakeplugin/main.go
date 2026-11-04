// Command fakeplugin 是插件协议测试用的最小实现，行为由 FAKEPLUGIN_MODE
// 控制。它不属于发布产物，放在 testdata 下因此不会被 ./... 编译。
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"traincache/checkpoint/internal/plugin"
)

func main() {
	mode := os.Getenv("FAKEPLUGIN_MODE")
	if mode == "" {
		mode = "normal"
	}
	if pidPath := os.Getenv("FAKEPLUGIN_PIDFILE"); pidPath != "" {
		_ = os.WriteFile(pidPath, []byte(strconv.Itoa(os.Getpid())), 0o644)
	}
	// no_connect 用来验证握手超时：连接都不建立，客户端只能靠超时收场。
	if mode == "no_connect" {
		time.Sleep(time.Hour)
		return
	}

	err := plugin.Serve("fake", "0.1", []string{"resolve", "save", "restore"},
		func(ctx context.Context, method string, params json.RawMessage, sess *plugin.Session) (any, error) {
			switch mode {
			case "no_result_exit":
				os.Exit(3)
			case "hang":
				time.Sleep(time.Hour)
			case "cancel":
				if err := sess.Progress(0, 10); err != nil {
					return nil, err
				}
				<-sess.Canceled()
				os.Exit(0)
			case "progress":
				if err := sess.Progress(40, 100); err != nil {
					return nil, err
				}
				if err := sess.Progress(100, 100); err != nil {
					return nil, err
				}
				return plugin.TransferResult{Bytes: 100, Files: 1}, nil
			}

			switch method {
			case "resolve":
				return plugin.ResolveResult{Found: true, Step: 7, Complete: true, Epoch: 2}, nil
			case "save", "restore":
				return plugin.TransferResult{Bytes: 1234, Files: 2}, nil
			default:
				return nil, &plugin.Error{Code: plugin.CodeInvalidArgument, Message: "未知方法 " + method}
			}
		})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
