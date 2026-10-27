// 桶级格式声明与 DataSet 文件校验。
//
// 切块粒度是桶级不变量：同一桶内所有 dataset 共用同一个 chunkSize，块名与
// 索引里的偏移都按它计算。首次发布把 {"version":1,"chunkSize":N} 写到
// _traincache/format.json（条件写 If-None-Match: *，并发发布只有一个能成功），
// 之后的发布只读这份声明，不再接受改变大小。
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"go.yaml.in/yaml/v3"

	"traincache/core/objectstore"
)

const (
	// formatKey 是桶级格式声明的对象键；不放在 dataset/ 前缀下，避免被
	// 快照枚举当成数据对象。
	formatKey = "_traincache/format.json"
	// formatVersion 是当前声明格式的版本号；只认这一个值。
	formatVersion = 1
)

// bucketFormat 是 _traincache/format.json 的内容。
type bucketFormat struct {
	Version   int   `json:"version"`
	ChunkSize int64 `json:"chunkSize"`
}

// ensureBucketFormat 返回本次发布实际生效的 chunkSize。
//
//   - 桶里还没有声明：用 requested 写入声明，并返回 requested；
//   - 已有声明：校验 version/chunkSize，explicit 为真且 requested 与之不同时
//     返回错误（块大小已固定）；
//   - explicit 为假（未显式传 --chunk-size）时忽略 requested，沿用已有值。
//
// 写入走条件写：并发发布时只有一个进程能创建成功，其余按已有声明处理。
// 后端不支持条件写时报错，不做“覆盖式初始化”的静默降级。
func ensureBucketFormat(ctx context.Context, store objectstore.Store, requested int64, explicit bool) (int64, error) {
	existing, err := readBucketFormat(ctx, store)
	if err == nil {
		return applyExistingFormat(existing, requested, explicit)
	}
	if !errors.Is(err, objectstore.ErrNotFound) {
		return 0, err
	}

	if requested <= 0 {
		return 0, fmt.Errorf("块大小必须大于 0，得到 %d", requested)
	}
	payload, err := json.Marshal(bucketFormat{Version: formatVersion, ChunkSize: requested})
	if err != nil {
		return 0, fmt.Errorf("编码桶格式声明 %s: %w", formatKey, err)
	}

	_, err = store.PutIfMatch(ctx, formatKey, bytes.NewReader(payload), int64(len(payload)), "")
	switch {
	case err == nil:
		return requested, nil
	case errors.Is(err, objectstore.ErrConditionalWriteUnsupported):
		return 0, fmt.Errorf("写入桶格式声明 %s 失败: %w：后端不支持条件写，无法保证只初始化一次，请改用支持 If-None-Match 的对象存储", formatKey, err)
	case errors.Is(err, objectstore.ErrConflict):
		// 并发发布：别的进程已经写了声明，退回按已有声明处理。
		existing, getErr := readBucketFormat(ctx, store)
		if getErr != nil {
			return 0, fmt.Errorf("重新读取桶格式声明 %s: %w", formatKey, getErr)
		}
		return applyExistingFormat(existing, requested, explicit)
	default:
		return 0, fmt.Errorf("写入桶格式声明 %s: %w", formatKey, err)
	}
}

// readBucketFormat 读取并校验桶格式声明；桶里没有声明时返回 objectstore.ErrNotFound。
func readBucketFormat(ctx context.Context, store objectstore.Store) (bucketFormat, error) {
	var buf bytes.Buffer
	if err := store.Get(ctx, formatKey, &buf); err != nil {
		if errors.Is(err, objectstore.ErrNotFound) {
			return bucketFormat{}, err
		}
		return bucketFormat{}, fmt.Errorf("读取桶格式声明 %s: %w", formatKey, err)
	}

	var f bucketFormat
	if err := json.Unmarshal(buf.Bytes(), &f); err != nil {
		return bucketFormat{}, fmt.Errorf("解析桶格式声明 %s: %w", formatKey, err)
	}
	if f.Version != formatVersion {
		return bucketFormat{}, fmt.Errorf("桶格式声明 %s 的 version=%d，只支持 %d", formatKey, f.Version, formatVersion)
	}
	if f.ChunkSize <= 0 {
		return bucketFormat{}, fmt.Errorf("桶格式声明 %s 的 chunkSize=%d，必须大于 0", formatKey, f.ChunkSize)
	}
	return f, nil
}

// applyExistingFormat 用一条已有声明决定本次生效的 chunkSize。
func applyExistingFormat(existing bucketFormat, requested int64, explicit bool) (int64, error) {
	if explicit && requested != existing.ChunkSize {
		return 0, fmt.Errorf("显式指定的块大小 %d 与桶声明 %s 中的 %d 不一致：桶的块大小已固定，改大小需要清空桶或换桶后重新发布", requested, formatKey, existing.ChunkSize)
	}
	return existing.ChunkSize, nil
}

// datasetFile 是 DataSet YAML 中本命令关心的字段子集。
type datasetFile struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec struct {
		DatasetName string `yaml:"datasetName"`
		ChunkSize   int64  `yaml:"chunkSize"`
	} `yaml:"spec"`
}

// checkDatasetFile 校验 DataSet 文件与本次发布的 dataset 名、桶声明块大小一致。
func checkDatasetFile(path string, wantDataset string, marker int64) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("读取 DataSet 文件: %w", err)
	}

	var f datasetFile
	if err := yaml.Unmarshal(data, &f); err != nil {
		return fmt.Errorf("解析 DataSet 文件 %s: %w", path, err)
	}
	if f.Kind != "DataSet" {
		return fmt.Errorf("DataSet 文件 %s 的 kind=%q，期望 DataSet", path, f.Kind)
	}
	if strings.TrimSpace(f.Metadata.Name) == "" {
		return fmt.Errorf("DataSet 文件 %s 缺少 metadata.name", path)
	}
	if f.Spec.DatasetName != wantDataset {
		return fmt.Errorf("DataSet 文件 %s 的 spec.datasetName=%q 与 --dataset %q 不一致", path, f.Spec.DatasetName, wantDataset)
	}
	if f.Spec.ChunkSize != marker {
		return fmt.Errorf("DataSet 文件 %s 的 spec.chunkSize=%d 与桶声明的块大小 %d 不一致：块大小已固定，改大小需要清空桶或换桶后重新发布", path, f.Spec.ChunkSize, marker)
	}
	return nil
}
