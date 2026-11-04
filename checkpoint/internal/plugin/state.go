package plugin

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
)

// PartRecord 是一条续传记录（design 10.1 §5.2）。前半段字段是分片记录；
// 多文件上传还需要知道上传 id 与对象键，因此增加可选的头字段，旧文件仍可读。
type PartRecord struct {
	Kind     string `json:"kind,omitempty"`      // upload 表示头记录，空表示分片
	UploadID string `json:"upload_id,omitempty"` // S3 multipart 上传 id
	Key      string `json:"key,omitempty"`       // 对象键
	File     string `json:"file,omitempty"`      // manifest 里的相对路径
	Part     int    `json:"part"`
	ETag     string `json:"etag"`
	Bytes    int64  `json:"bytes"`
}

// AppendPart 追加一条分片记录并 fsync。先落盘再传下一片，进程被杀时最多
// 丢掉正在写的末行，已确认的分片仍然可信。
func AppendPart(path string, rec PartRecord) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("plugin: 编码分片记录 %s: %w", path, err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("plugin: 打开续传状态 %s: %w", path, err)
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		_ = f.Close()
		return fmt.Errorf("plugin: 写续传状态 %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("plugin: fsync 续传状态 %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("plugin: 关闭续传状态 %s: %w", path, err)
	}
	return nil
}

// LoadParts 读取续传状态。末行没有换行说明上一次写没有完成，按 design
// 10.1 §5.2 丢弃；中间行损坏则说明文件本身不可信，返回错误而不是猜。
func LoadParts(path string) ([]PartRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("plugin: 打开续传状态 %s: %w", path, err)
	}
	defer f.Close()

	r := bufio.NewReader(f)
	var parts []PartRecord
	for lineNo := 1; ; lineNo++ {
		line, err := r.ReadBytes('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				// ReadBytes 返回的残留字节就是没有换行的末行，丢弃。
				break
			}
			return nil, fmt.Errorf("plugin: 读取续传状态 %s 第 %d 行: %w", path, lineNo, err)
		}
		line = bytes.TrimSuffix(line, []byte{'\n'})
		if len(line) == 0 {
			return nil, fmt.Errorf("plugin: 续传状态 %s 第 %d 行损坏: 空行", path, lineNo)
		}
		var rec PartRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return nil, fmt.Errorf("plugin: 续传状态 %s 第 %d 行损坏: %w", path, lineNo, err)
		}
		parts = append(parts, rec)
	}
	return parts, nil
}
