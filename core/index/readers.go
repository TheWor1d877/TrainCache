package index

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"traincache/core/objectstore"
)

// defaultReaderGrace 是 NewReaderRegistry 在 grace<=0 时使用的切换宽限期。
const defaultReaderGrace = 10 * time.Second

// ReaderClaim 是一个读者节点对某数据集当前跟随快照的声明（一节点一数据集一对象，
// 6.1 §5.10）。GC 依据声明保护快照：SnapID 与 PreviousSnapID 都计入 protected，
// UpdatedAt 超过 Config.ReaderGrace 的声明整体忽略（视为读者已死）。
type ReaderClaim struct {
	NodeID         string    `json:"node_id"`
	SnapID         string    `json:"snap_id"`
	PreviousSnapID string    `json:"previous_snap_id,omitempty"`
	PreviousUntil  time.Time `json:"previous_until,omitempty"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// ReaderKey 返回节点 nodeID 在 dataset 下的声明对象键：
// dataset/<dataset>/readers/<url.PathEscape(nodeID)>.json（6.1 §5.10）。
// nodeID 必须经 url.PathEscape 编码，避免含 "/" 等字符破坏键布局。
func ReaderKey(dataset, nodeID string) string {
	return "dataset/" + dataset + "/readers/" + url.PathEscape(nodeID) + ".json"
}

// readersPrefix 返回数据集全部读者声明对象的前缀 "dataset/<name>/readers/"。
func readersPrefix(dataset string) string {
	return "dataset/" + dataset + "/readers/"
}

// ReaderRegistry 维护本进程作为读者的 claim：每个 dataset 一个对象，声明本进程
// 当前跟随的 snapID 并定时刷新心跳（R2）。零值不可用，须经 NewReaderRegistry
// 构造；并发安全由实现保证。
type ReaderRegistry struct {
	store  objectstore.Store
	nodeID string
	grace  time.Duration
	now    func() time.Time

	mu        sync.Mutex
	last      map[string]string    // dataset → 上次成功声明的 snapID
	prev      map[string]string    // dataset → 上次声明的 PreviousSnapID
	prevUntil map[string]time.Time // dataset → 上次声明的 PreviousUntil
}

// NewReaderRegistry 构造绑定到本节点 nodeID 的读者登记器；store 用于读写
// dataset/<dataset>/readers/ 下的 claim 对象。grace 用于 PreviousUntil，
// <=0 时回退 10s。
func NewReaderRegistry(store objectstore.Store, nodeID string, grace time.Duration) *ReaderRegistry {
	if grace <= 0 {
		grace = defaultReaderGrace
	}
	return &ReaderRegistry{
		store:     store,
		nodeID:    nodeID,
		grace:     grace,
		now:       time.Now,
		last:      make(map[string]string),
		prev:      make(map[string]string),
		prevUntil: make(map[string]time.Time),
	}
}

// Declare 声明本进程在 dataset 上跟随 snapID（幂等）：previous = 本进程上一次
// 声明的 snapID（与本次不同才写入，首次声明 previous 为空）；Put 覆盖同一对象。
// 重复声明相同 snapID 不改变语义（保留尚未过期的 previous，过期后清空）。
// 成功后更新内存中的 last 声明；失败不更新。
//
// 错误语义：ctx 取消返回 ctx.Err()；对象存储错误原样包装返回。本方法只写不读，
// 损坏的既有对象由 ReadReaderClaims 在读取侧报错。
func (r *ReaderRegistry) Declare(ctx context.Context, dataset, snapID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	now := r.now()
	previous := r.prev[dataset]
	previousUntil := r.prevUntil[dataset]
	if snapID != r.last[dataset] {
		previous = r.last[dataset]
		previousUntil = now.Add(r.grace)
	} else if !previousUntil.IsZero() && now.After(previousUntil) {
		// 同一快照的重复声明：previous 宽限期已过则清空。
		previous = ""
		previousUntil = time.Time{}
	}

	claim := ReaderClaim{
		NodeID:         r.nodeID,
		SnapID:         snapID,
		PreviousSnapID: previous,
		PreviousUntil:  previousUntil,
		UpdatedAt:      now,
	}
	if err := putReaderClaim(ctx, r.store, dataset, claim); err != nil {
		return err
	}

	r.last[dataset] = snapID
	r.prev[dataset] = previous
	r.prevUntil[dataset] = previousUntil
	return nil
}

// Withdraw 撤销本进程在 dataset 上的声明：写 snap_id=""、
// previous_snap_id=<last>、previous_until=now+grace、updated_at=now，之后停止该
// dataset 的心跳。last 保留在内存中。
// best-effort：写入失败也视为撤销完成（不阻止进程退出）；无 last 声明时仍写
// 空声明。ctx 取消返回 ctx.Err()，调用方通常另设短超时。
func (r *ReaderRegistry) Withdraw(ctx context.Context, dataset string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	now := r.now()
	last := r.last[dataset]
	until := now.Add(r.grace)
	claim := ReaderClaim{
		NodeID:         r.nodeID,
		SnapID:         "",
		PreviousSnapID: last,
		PreviousUntil:  until,
		UpdatedAt:      now,
	}
	if err := putReaderClaim(ctx, r.store, dataset, claim); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		// best-effort：写入失败视为撤销完成。
	}
	r.prev[dataset] = last
	r.prevUntil[dataset] = until
	return nil
}

// putReaderClaim 序列化并覆盖写入单个读者声明对象。
func putReaderClaim(ctx context.Context, store objectstore.Store, dataset string, claim ReaderClaim) error {
	data, err := json.Marshal(claim)
	if err != nil {
		return fmt.Errorf("index: encode reader claim for %q: %w", dataset, err)
	}
	key := ReaderKey(dataset, claim.NodeID)
	if _, err := store.Put(ctx, key, bytes.NewReader(data), int64(len(data))); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("index: put reader claim %s: %w", key, err)
	}
	return nil
}

// ReadReaderClaims 读取 dataset 下全部读者声明（键前缀
// dataset/<dataset>/readers/）。无 claim 对象时返回空集合；任一对象 JSON 损坏
// 返回错误（GC 需 fail-safe：不得把读取失败当作"无声明"继续删除）。
func ReadReaderClaims(ctx context.Context, store objectstore.Store, dataset string) ([]ReaderClaim, error) {
	prefix := readersPrefix(dataset)
	keys, err := store.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("index: list reader claims %s: %w", prefix, err)
	}
	claims := make([]ReaderClaim, 0, len(keys))
	for _, key := range keys {
		data, err := getObject(ctx, store, key)
		if err != nil {
			if errors.Is(err, objectstore.ErrNotFound) {
				continue
			}
			return nil, err
		}
		var claim ReaderClaim
		if err := json.Unmarshal(data, &claim); err != nil {
			return nil, fmt.Errorf("index: decode reader claim %s: %w", key, err)
		}
		claims = append(claims, claim)
	}
	return claims, nil
}
