package evictor

import (
	"testing"
	"time"

	"traincache/core/cas"
)

// policyBlock 构造一个块:指纹按序号区分,热度/最后访问时间/访问频次由调用方给。
func policyBlock(i int, heat float64, touch time.Time, accesses int64) Block {
	var fp cas.Fingerprint
	fp[0] = byte(i)
	fp[1] = byte(i >> 8)
	return Block{FP: fp, Heat: heat, LastTouch: touch, Accesses: accesses}
}

// policyFPs 把排序结果还原成指纹序号,便于断言顺序。
func policyFPs(blocks []Block) []int {
	out := make([]int, 0, len(blocks))
	for _, b := range blocks {
		out = append(out, int(b.FP[0]))
	}
	return out
}

func policyEqual(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// 三档策略编译期都实现 Policy 接口(D9 撤档后接口只剩 Rank)。
var (
	_ Policy = (*HeatPolicy)(nil)
	_ Policy = (*LRUPolicy)(nil)
	_ Policy = (*LFUPolicy)(nil)
)

// TestPolicyNames 三档策略名固定:heat / lru / lfu。
// 撤档后不存在第四档,任何新名字都必须先改这里与 svc 的合法清单。
func TestPolicyNames(t *testing.T) {
	cases := []struct {
		p    Policy
		want string
	}{
		{NewHeatPolicy(), "heat"},
		{&LRUPolicy{}, "lru"},
		{NewLFUPolicy(), "lfu"},
	}
	for _, c := range cases {
		if got := c.p.Name(); got != c.want {
			t.Errorf("策略名 = %q, want %q", got, c.want)
		}
	}
}

// TestPolicyRankOrder 三档策略对同一组块给出三���不同的淘汰顺序。
//
// 构造的三个块故意互相"打架":
//
//	块1:热度最高但最久没访问、访问频次最低
//	块2:热度最低但刚访问
//	块3:热度、访问时间、频次都在中间
//
// 于是 heat 看热度(块3 先走)、lru 看最后访问时间(块1 先走)、
// lfu 看老化后的访问频次(块1 先走、块3 最后走),三档顺序各不相同。
func TestPolicyRankOrder(t *testing.T) {
	now := time.Now()
	blocks := []Block{
		policyBlock(1, 9.0, now.Add(-2*time.Hour), 1),
		policyBlock(2, 0.1, now.Add(-1*time.Minute), 1),
		policyBlock(3, 0.5, now.Add(-30*time.Minute), 50),
	}

	cases := []struct {
		name string
		p    Policy
		want []int
	}{
		{"heat", NewHeatPolicy(), []int{3, 2, 1}},
		{"lru", &LRUPolicy{}, []int{1, 3, 2}},
		{"lfu", NewLFUPolicy(), []int{1, 2, 3}},
	}
	for _, c := range cases {
		got := policyFPs(c.p.Rank(blocks, false))
		if !policyEqual(got, c.want) {
			t.Errorf("%s 档排序 = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestPolicyRankDangerTurn heat 档在危险线轮只按热度排序(新鲜度豁免失效);
// lru / lfu 不看 danger 参数,危险线轮与普通轮同序。
func TestPolicyRankDangerTurn(t *testing.T) {
	now := time.Now()
	blocks := []Block{
		policyBlock(1, 9.0, now.Add(-2*time.Hour), 1),
		policyBlock(2, 0.1, now.Add(-1*time.Minute), 1),
		policyBlock(3, 0.5, now.Add(-30*time.Minute), 50),
	}

	heat := NewHeatPolicy()
	gotDanger := policyFPs(heat.Rank(blocks, true))
	if want := []int{2, 3, 1}; !policyEqual(gotDanger, want) {
		t.Errorf("heat 危险线轮排序 = %v, want %v(只看热度)", gotDanger, want)
	}

	lru := &LRUPolicy{}
	if normal, danger := policyFPs(lru.Rank(blocks, false)), policyFPs(lru.Rank(blocks, true)); !policyEqual(normal, danger) {
		t.Errorf("lru 档不应受 danger 影响:普通轮 %v,危险轮 %v", normal, danger)
	}

	lfu := NewLFUPolicy()
	if normal, danger := policyFPs(lfu.Rank(blocks, false)), policyFPs(lfu.Rank(blocks, true)); !policyEqual(normal, danger) {
		t.Errorf("lfu 档不应受 danger 影响:普通轮 %v,危险轮 %v", normal, danger)
	}
}

// TestPolicyRankDoesNotMutateInput Rank 返回的是副本,不得改动调用方传入的切片。
// 淘汰循环会反复拿同一批候选去排序,原地修改会让排序结果互相污染。
func TestPolicyRankDoesNotMutateInput(t *testing.T) {
	now := time.Now()
	orig := []Block{
		policyBlock(1, 9.0, now.Add(-2*time.Hour), 1),
		policyBlock(2, 0.1, now.Add(-1*time.Minute), 1),
		policyBlock(3, 0.5, now.Add(-30*time.Minute), 50),
	}
	before := policyFPs(orig)

	for _, p := range []Policy{NewHeatPolicy(), &LRUPolicy{}, NewLFUPolicy()} {
		_ = p.Rank(orig, false)
		_ = p.Rank(orig, true)
		if after := policyFPs(orig); !policyEqual(before, after) {
			t.Errorf("%s 档 Rank 改动了入参切片:%v → %v", p.Name(), before, after)
		}
	}
}
