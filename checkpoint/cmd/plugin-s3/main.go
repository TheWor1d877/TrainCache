// Command plugin-s3 是 S3 兼容后端的 checkpoint 插件进程。
package main

import (
	"fmt"
	"os"

	"traincache/checkpoint/internal/plugin"
	"traincache/checkpoint/internal/plugin/s3"
)

// version 由构建脚本用 -X main.version 注入；源码构建时保持 dev。
// 必须是变量：-X 对常量静默无效，写成常量会让镜像里的握手版本永远是 dev。
var version = "dev"

func main() {
	err := plugin.Serve("s3", version, []string{"resolve", "save", "restore"}, s3.Handler)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
