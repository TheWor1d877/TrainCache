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

// version 由构建脚本用 -X main.version 注入；源码构建时保持 dev。
// 必须是变量：-X 对常量静默无效，写成常量会让镜像里的握手版本永远是 dev。
var version = "dev"

func main() {
	err := plugin.Serve(local.Name, version,
		[]string{"resolve", "save", "restore"}, local.Handler())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
