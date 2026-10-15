package svc

// 本文件是 ListSnapshot 的用例(§5.6 CLI --chunks 名字解析;D29):直接调 Server 方法
// (bufconn 链路已由 server_test 覆盖),装置用 svcFixture(Mock 源站 + 临时 CAS)。

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	agentpb "traincache/agent/api/agentpb"
)

// newSnapshotServer 构造带预置数据集的 Server(不经过 bufconn)。
func newSnapshotServer(t *testing.T) (*Server, *svcFixture) {
	t.Helper()
	fx := newSvcFixture(t)
	return NewServer(fx.cfg, fx.agent()), fx
}

// TestListSnapshotMappingAndOrder:条目映射 (path, chunk, size) 且按 (path, chunk) 字典序;
// prefix 过滤;空 prefix = 全量。
func TestListSnapshotMappingAndOrder(t *testing.T) {
	s, fx := newSnapshotServer(t)
	fx.commitDataset(t, testDataset, []svcBlockSpec{
		{path: "b-000.tar", chunk: 0, content: "snap-b0", local: true},
		{path: "a-001.tar", chunk: 3, content: "snap-a1", local: true},
		{path: "a-000.tar", chunk: 0, content: "snap-a0", local: true},
	})
	ctx := context.Background()

	t.Run("全量按字典序", func(t *testing.T) {
		resp, err := s.ListSnapshot(ctx, &agentpb.ListSnapshotRequest{Dataset: testDataset})
		if err != nil {
			t.Fatalf("ListSnapshot: %v", err)
		}
		want := []struct {
			path  string
			chunk int64
			size  int64
		}{
			{"a-000.tar", 0, int64(len("snap-a0"))},
			{"a-001.tar", 3, int64(len("snap-a1"))},
			{"b-000.tar", 0, int64(len("snap-b0"))},
		}
		if len(resp.GetEntries()) != len(want) {
			t.Fatalf("entries 数 = %d, want %d", len(resp.GetEntries()), len(want))
		}
		for i, w := range want {
			e := resp.GetEntries()[i]
			if e.GetPath() != w.path || e.GetChunk() != w.chunk || e.GetSize() != w.size {
				t.Errorf("entries[%d] = (%s, %d, %d), want (%s, %d, %d)",
					i, e.GetPath(), e.GetChunk(), e.GetSize(), w.path, w.chunk, w.size)
			}
		}
	})

	t.Run("prefix 过滤", func(t *testing.T) {
		resp, err := s.ListSnapshot(ctx, &agentpb.ListSnapshotRequest{Dataset: testDataset, Prefix: "a-"})
		if err != nil {
			t.Fatalf("ListSnapshot(a-): %v", err)
		}
		if len(resp.GetEntries()) != 2 {
			t.Fatalf("entries 数 = %d, want 2(仅 a- 前缀)", len(resp.GetEntries()))
		}
		for _, e := range resp.GetEntries() {
			if e.GetPath() != "a-000.tar" && e.GetPath() != "a-001.tar" {
				t.Errorf("prefix 过滤后出现 %q", e.GetPath())
			}
		}
	})

	t.Run("无匹配前缀返回空", func(t *testing.T) {
		resp, err := s.ListSnapshot(ctx, &agentpb.ListSnapshotRequest{Dataset: testDataset, Prefix: "zzz-"})
		if err != nil {
			t.Fatalf("ListSnapshot(zzz-): %v", err)
		}
		if len(resp.GetEntries()) != 0 {
			t.Errorf("entries = %v, want 空", resp.GetEntries())
		}
	})
}

// TestListSnapshotErrNoCurrent:dataset 尚无 CURRENT → 空 entries 无错误(裁决口径)。
func TestListSnapshotErrNoCurrent(t *testing.T) {
	s, _ := newSnapshotServer(t)
	resp, err := s.ListSnapshot(context.Background(),
		&agentpb.ListSnapshotRequest{Dataset: "no-such-dataset"})
	if err != nil {
		t.Fatalf("ListSnapshot(未知 dataset) err = %v, want nil(ErrNoCurrent 视作空)", err)
	}
	if len(resp.GetEntries()) != 0 {
		t.Errorf("entries = %v, want 空", resp.GetEntries())
	}
}

// TestListSnapshotValidation:dataset 空 / snapshot_id 非空 → InvalidArgument(仅 CURRENT)。
func TestListSnapshotValidation(t *testing.T) {
	s, fx := newSnapshotServer(t)
	fx.commitDataset(t, testDataset, []svcBlockSpec{
		{path: "v-000.tar", chunk: 0, content: "snap-v0", local: true},
	})
	ctx := context.Background()

	if _, err := s.ListSnapshot(ctx, &agentpb.ListSnapshotRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("dataset 空 code = %v, want InvalidArgument(err=%v)", status.Code(err), err)
	}
	if _, err := s.ListSnapshot(ctx, &agentpb.ListSnapshotRequest{Dataset: testDataset, SnapshotId: "snap-1"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("snapshot_id 非空 code = %v, want InvalidArgument(err=%v)", status.Code(err), err)
	}
	// 合法请求仍可用(校验未破坏后继路径)。
	if _, err := s.ListSnapshot(ctx, &agentpb.ListSnapshotRequest{Dataset: testDataset}); err != nil {
		t.Errorf("合法请求 err = %v, want nil", err)
	}
}
