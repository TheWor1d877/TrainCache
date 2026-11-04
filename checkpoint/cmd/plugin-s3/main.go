// Command plugin-s3 是 S3 兼容后端的 checkpoint 插件进程。
package main

import (
	"fmt"
	"os"

	"traincache/checkpoint/internal/plugin"
	"traincache/checkpoint/internal/plugin/s3"
)

// version 由构建脚本注入；源码构建时保持 dev，与插件握手的 version 字段一致。
const version = "dev"

func main() {
	err := plugin.Serve("s3", version, []string{"resolve", "save", "restore"}, s3.Handler)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
