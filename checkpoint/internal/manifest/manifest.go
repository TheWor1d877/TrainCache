// Package manifest 定义 checkpoint 的 manifest 结构与校验规则
// （design 10.1「数据格式」）。
//
// manifest 是后端上的提交标记：它存在且校验通过，该 rank 的该 step 才算
// 完整。写入方必须按 path 排序后再写，读取方据此得到稳定的字节表示。
package manifest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Version 是当前 manifest 结构版本（design 10.1「数据格式」）。
const Version = 1

// File 是 manifest 里的单个文件条目。
type File struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// Manifest 是一个 rank 在一个 step 上的完整文件清单。
type Manifest struct {
	Version   int       `json:"version"`
	TaskID    string    `json:"task_id"`
	Step      int64     `json:"step"`
	Rank      int       `json:"rank"`
	WorldSize int       `json:"world_size"`
	Epoch     int       `json:"epoch"`
	CreatedAt time.Time `json:"created_at"`
	Files     []File    `json:"files"`
}

// Validate 校验 manifest 的结构约束。路径只允许相对路径且不能含 ..，
// 否则读取方在 root 之外拼路径，会造成目录穿越。
func (m *Manifest) Validate() error {
	if m == nil {
		return fmt.Errorf("manifest: 为空")
	}
	if m.Version != Version {
		return fmt.Errorf("manifest: version=%d 不受支持(需 %d)", m.Version, Version)
	}
	if m.TaskID == "" {
		return fmt.Errorf("manifest: task_id 不能为空")
	}
	if m.Step < 0 {
		return fmt.Errorf("manifest: step=%d 不能为负", m.Step)
	}
	if m.Rank < 0 {
		return fmt.Errorf("manifest: rank=%d 不能为负", m.Rank)
	}
	if m.WorldSize <= 0 {
		return fmt.Errorf("manifest: world_size=%d 需 > 0", m.WorldSize)
	}
	if m.Rank >= m.WorldSize {
		return fmt.Errorf("manifest: rank=%d 需小于 world_size=%d", m.Rank, m.WorldSize)
	}
	if len(m.Files) == 0 {
		return fmt.Errorf("manifest: files 不能为空")
	}

	seen := make(map[string]struct{}, len(m.Files))
	for i, f := range m.Files {
		if err := validateFile(f); err != nil {
			return fmt.Errorf("manifest: files[%d] %q: %w", i, f.Path, err)
		}
		if _, dup := seen[f.Path]; dup {
			return fmt.Errorf("manifest: 文件 %q 重复", f.Path)
		}
		seen[f.Path] = struct{}{}
	}
	return nil
}

func validateFile(f File) error {
	if f.Path == "" {
		return fmt.Errorf("path 不能为空")
	}
	if filepath.IsAbs(f.Path) || strings.HasPrefix(f.Path, "/") {
		return fmt.Errorf("path 必须是相对路径")
	}
	for _, seg := range strings.Split(f.Path, "/") {
		if seg == ".." {
			return fmt.Errorf("path 不能包含 ..")
		}
	}
	if f.Size < 0 {
		return fmt.Errorf("size=%d 不能为负", f.Size)
	}
	if !validSHA256(f.SHA256) {
		return fmt.Errorf("sha256 必须是 64 位小写十六进制")
	}
	return nil
}

func validSHA256(s string) bool {
	if len(s) != sha256.Size*2 {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// Marshal 按 path 排序后编码，保证同一个文件集合总能得到相同的字节。
// 排序在副本上做，不修改调用方的切片。
func (m *Manifest) Marshal() ([]byte, error) {
	cp := *m
	cp.Files = append([]File(nil), m.Files...)
	sort.Slice(cp.Files, func(i, j int) bool { return cp.Files[i].Path < cp.Files[j].Path })
	b, err := json.Marshal(&cp)
	if err != nil {
		return nil, fmt.Errorf("manifest: 编码: %w", err)
	}
	return b, nil
}

// Unmarshal 解码并校验。校验放在这里，调用方拿到的 Manifest 一定满足
// design 10.1「数据格式」的约束。
func Unmarshal(data []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("manifest: 解码: %w", err)
	}
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return &m, nil
}

// VerifyFiles 按 manifest 逐文件核对大小与 sha256。先 Validate 是为了
// 复用重复路径与路径穿越检查，避免这里出现另一套口径。
func (m *Manifest) VerifyFiles(root string) error {
	if err := m.Validate(); err != nil {
		return err
	}
	for _, f := range m.Files {
		path := filepath.Join(root, f.Path)
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("manifest: 核对 %q: %w", f.Path, err)
		}
		if info.IsDir() {
			return fmt.Errorf("manifest: 核对 %q: 是目录", f.Path)
		}
		if info.Size() != f.Size {
			return fmt.Errorf("manifest: 核对 %q: 大小 %d，期望 %d", f.Path, info.Size(), f.Size)
		}
		sum, err := fileSHA256(path)
		if err != nil {
			return fmt.Errorf("manifest: 核对 %q: %w", f.Path, err)
		}
		if sum != f.SHA256 {
			return fmt.Errorf("manifest: 核对 %q: sha256 %s，期望 %s", f.Path, sum, f.SHA256)
		}
	}
	return nil
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
