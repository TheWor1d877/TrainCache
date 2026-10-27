package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"google.golang.org/grpc"

	agentpb "traincache/agent/api/agentpb"
)

// fakeSnapshotLister 替代真实 Agent,记录请求并返回预置响应。
type fakeSnapshotLister struct {
	req  *agentpb.ListSnapshotRequest
	resp *agentpb.ListSnapshotResponse
	err  error
}

func (f *fakeSnapshotLister) ListSnapshot(_ context.Context, in *agentpb.ListSnapshotRequest, _ ...grpc.CallOption) (*agentpb.ListSnapshotResponse, error) {
	f.req = in
	return f.resp, f.err
}

func entryRef(path string, chunk, size int64) *agentpb.EntryRef {
	return &agentpb.EntryRef{Path: path, Chunk: chunk, Size: size}
}

// rowsOf 把表格拆成单元格,忽略列宽,只校验内容与顺序。
func rowsOf(t *testing.T, out string) [][]string {
	t.Helper()
	text := strings.TrimRight(out, "\n")
	if text == "" {
		return nil
	}
	var rows [][]string
	for _, line := range strings.Split(text, "\n") {
		rows = append(rows, strings.Fields(line))
	}
	return rows
}

func assertRows(t *testing.T, got [][]string, want ...[]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("行数 = %d, 期望 %d(got=%q)", len(got), len(want), got)
	}
	for i := range want {
		if strings.Join(got[i], "|") != strings.Join(want[i], "|") {
			t.Fatalf("第 %d 行 = %v, 期望 %v", i, got[i], want[i])
		}
	}
}

func TestWriteListSnapshotEmpty(t *testing.T) {
	var buf bytes.Buffer
	if err := writeListSnapshot(&buf, nil, false); err != nil {
		t.Fatalf("writeListSnapshot: %v", err)
	}
	if got, want := buf.String(), "FILE  CHUNKS  BYTES\n"; got != want {
		t.Fatalf("输出 = %q, 期望 %q", got, want)
	}

	buf.Reset()
	if err := writeListSnapshot(&buf, nil, true); err != nil {
		t.Fatalf("writeListSnapshot: %v", err)
	}
	if got, want := buf.String(), "FILE  CHUNK  BYTES\n"; got != want {
		t.Fatalf("输出 = %q, 期望 %q", got, want)
	}
}

func TestWriteListSnapshotSingleFileMultiChunk(t *testing.T) {
	entries := []*agentpb.EntryRef{
		entryRef("shard-000.tar", 0, 10),
		entryRef("shard-000.tar", 1, 20),
		entryRef("shard-000.tar", 2, 30),
	}
	var buf bytes.Buffer
	if err := writeListSnapshot(&buf, entries, false); err != nil {
		t.Fatalf("writeListSnapshot: %v", err)
	}
	assertRows(t, rowsOf(t, buf.String()),
		[]string{"FILE", "CHUNKS", "BYTES"},
		[]string{"shard-000.tar", "3", "60"},
	)
}

func TestWriteListSnapshotMultiFileSorted(t *testing.T) {
	entries := []*agentpb.EntryRef{
		entryRef("coco-002.tar", 1, 5),
		entryRef("coco-000.tar", 0, 7),
		entryRef("coco-002.tar", 0, 5),
		entryRef("coco-000.tar", 1, 8),
		entryRef("coco-001.tar", 0, 9),
	}
	var buf bytes.Buffer
	if err := writeListSnapshot(&buf, entries, false); err != nil {
		t.Fatalf("writeListSnapshot: %v", err)
	}
	assertRows(t, rowsOf(t, buf.String()),
		[]string{"FILE", "CHUNKS", "BYTES"},
		[]string{"coco-000.tar", "2", "15"},
		[]string{"coco-001.tar", "1", "9"},
		[]string{"coco-002.tar", "2", "10"},
	)
}

func TestWriteListSnapshotChunksExpanded(t *testing.T) {
	entries := []*agentpb.EntryRef{
		entryRef("coco-001.tar", 2, 3),
		entryRef("coco-000.tar", 1, 2),
		entryRef("coco-000.tar", 0, 1),
		entryRef("coco-001.tar", 1, 3),
	}
	var buf bytes.Buffer
	if err := writeListSnapshot(&buf, entries, true); err != nil {
		t.Fatalf("writeListSnapshot: %v", err)
	}
	assertRows(t, rowsOf(t, buf.String()),
		[]string{"FILE", "CHUNK", "BYTES"},
		[]string{"coco-000.tar", "0", "1"},
		[]string{"coco-000.tar", "1", "2"},
		[]string{"coco-001.tar", "1", "3"},
		[]string{"coco-001.tar", "2", "3"},
	)
}

func TestSummarizeFilesKeepsSumAndCount(t *testing.T) {
	got := summarizeFiles([]*agentpb.EntryRef{
		entryRef("b.tar", 0, 1),
		entryRef("a.tar", 0, 2),
		entryRef("a.tar", 1, 4),
	})
	if len(got) != 2 || got[0].path != "a.tar" || got[0].chunks != 2 || got[0].bytes != 6 {
		t.Fatalf("summarizeFiles = %+v", got)
	}
}

func TestLSSnapshotEmptyResultExitsZero(t *testing.T) {
	var stdout, stderr bytes.Buffer
	fake := &fakeSnapshotLister{resp: &agentpb.ListSnapshotResponse{}}
	code := lsSnapshot([]string{"--dataset", "coco"}, &stdout, &stderr, func(string) (snapshotLister, func(), error) {
		return fake, func() {}, nil
	})
	if code != exitOK {
		t.Fatalf("退出码 = %d, 期望 %d(stderr=%q)", code, exitOK, stderr.String())
	}
	if got, want := stdout.String(), "FILE  CHUNKS  BYTES\n"; got != want {
		t.Fatalf("输出 = %q, 期望 %q", got, want)
	}
}

func TestLSSnapshotRequestFields(t *testing.T) {
	var stdout, stderr bytes.Buffer
	fake := &fakeSnapshotLister{resp: &agentpb.ListSnapshotResponse{
		Entries: []*agentpb.EntryRef{entryRef("coco-000.tar", 0, 8)},
	}}
	code := lsSnapshot(
		[]string{"--dataset", "coco", "--snapshot-id", "snap-1", "--prefix", "coco-", "--chunks"},
		&stdout, &stderr,
		func(string) (snapshotLister, func(), error) { return fake, func() {}, nil },
	)
	if code != exitOK {
		t.Fatalf("退出码 = %d, 期望 %d(stderr=%q)", code, exitOK, stderr.String())
	}
	if fake.req.GetDataset() != "coco" || fake.req.GetSnapshotId() != "snap-1" || fake.req.GetPrefix() != "coco-" {
		t.Fatalf("请求字段 = %+v", fake.req)
	}
	assertRows(t, rowsOf(t, stdout.String()),
		[]string{"FILE", "CHUNK", "BYTES"},
		[]string{"coco-000.tar", "0", "8"},
	)
}

func TestLSSnapshotRPCErrorExitsOne(t *testing.T) {
	var stdout, stderr bytes.Buffer
	fake := &fakeSnapshotLister{err: errors.New("boom")}
	code := lsSnapshot([]string{"--dataset", "coco"}, &stdout, &stderr, func(string) (snapshotLister, func(), error) {
		return fake, func() {}, nil
	})
	if code != exitError {
		t.Fatalf("退出码 = %d, 期望 %d", code, exitError)
	}
	if !strings.Contains(stderr.String(), "ListSnapshot 失败") {
		t.Fatalf("stderr = %q, 期望含 ListSnapshot 失败", stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, 期望为空", stdout.String())
	}
}

func TestLSSnapshotDialErrorExitsOne(t *testing.T) {
	code := lsSnapshot([]string{"--dataset", "coco"}, io.Discard, io.Discard, func(string) (snapshotLister, func(), error) {
		return nil, nil, errors.New("no route")
	})
	if code != exitError {
		t.Fatalf("退出码 = %d, 期望 %d", code, exitError)
	}
}

func TestLSSnapshotUsageErrorsExitTwo(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"缺 dataset", []string{}},
		{"位置参数", []string{"--dataset", "coco", "extra"}},
		{"未知 flag", []string{"--dataset", "coco", "--nope"}},
		{"缺 flag 值", []string{"--dataset"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			code := lsSnapshot(tc.args, io.Discard, io.Discard, func(string) (snapshotLister, func(), error) {
				called = true
				return &fakeSnapshotLister{resp: &agentpb.ListSnapshotResponse{}}, func() {}, nil
			})
			if code != exitUsage {
				t.Fatalf("退出码 = %d, 期望 %d", code, exitUsage)
			}
			if called {
				t.Fatal("参数错误时不应建立连接")
			}
		})
	}
}

func TestExpandFileChunksOrderAndMissing(t *testing.T) {
	entries := []*agentpb.EntryRef{
		{Path: "b.tar", Chunk: 1},
		{Path: "a.tar", Chunk: 2},
		{Path: "a.tar", Chunk: 0},
		{Path: "c.tar", Chunk: 7},
	}
	refs, missing := expandFileChunks(entries, []string{"a.tar", "z.tar"})
	if len(missing) != 1 || missing[0] != "z.tar" {
		t.Fatalf("missing = %v", missing)
	}
	if len(refs) != 2 || refs[0].GetChunk() != 0 || refs[1].GetChunk() != 2 || refs[0].GetPath() != "a.tar" {
		t.Fatalf("refs = %+v", refs)
	}

	refs, missing = expandFileChunks(entries, []string{"b.tar", "a.tar"})
	if len(missing) != 0 || len(refs) != 3 {
		t.Fatalf("refs=%+v missing=%v", refs, missing)
	}
	if refs[0].GetPath() != "b.tar" || refs[1].GetPath() != "a.tar" {
		t.Fatalf("file order = %+v", refs)
	}
}

func TestParseChunkSpecs(t *testing.T) {
	refs, err := parseChunkSpecs([]string{"shard-000.tar:3", "a.tar:0"})
	if err != nil {
		t.Fatalf("parseChunkSpecs: %v", err)
	}
	if len(refs) != 2 || refs[0].GetPath() != "shard-000.tar" || refs[0].GetChunk() != 3 {
		t.Fatalf("refs = %+v", refs)
	}

	for _, bad := range []string{"a.tar", ":1", "a.tar:", "a.tar:-1", "a.tar:x"} {
		if _, err := parseChunkSpecs([]string{bad}); err == nil {
			t.Fatalf("%q accepted", bad)
		}
	}
}
