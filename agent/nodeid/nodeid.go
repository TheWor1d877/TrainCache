// Package nodeid 解析节点标识:env NODE_NAME(spec.nodeName 经 fieldRef 注入)为权威来源
// (design §4 目录树;decisions D8——启动校验非空,状态与指标均携带节点标识)。
package nodeid

import (
	"errors"
	"os"
	"strings"
)

// ID 是节点标识,取值来自 NODE_NAME。
type ID string

// FromEnv 读取 NODE_NAME 并解析为 ID;TrimSpace 后为空 → 错误(D8 启动校验)。
func FromEnv() (ID, error) {
	name := strings.TrimSpace(os.Getenv("NODE_NAME"))
	if name == "" {
		return "", errors.New("nodeid: 环境变量 NODE_NAME 缺失或为空(decisions D8:启动校验必填)")
	}
	return ID(name), nil
}

// String 返回节点名。
func (id ID) String() string { return string(id) }
