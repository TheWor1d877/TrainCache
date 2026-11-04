// Command plugin-local 是本地目录后端插件的入口。
//
// checkpointd 每次保存或恢复启动一个子进程，argv[1] 是本次连接的
// UNIX 域套接字路径（design 10.1 §4.1）。
package main

import (
	"fmt"
	"os"

	"traincache/checkpoint/internal/plugin"
	"traincache/checkpoint/internal/plugin/local"
)

func main() {
	err := plugin.Serve(local.Name, local.Version,
		[]string{"resolve", "save", "restore"}, local.Handler())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
