package identity

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func testConfig(t *testing.T) Config {
	cfg := DefaultConfig(t.TempDir())
	cfg.Actors = map[string][]string{
		"admin":   {"*"},
		"analyst": {"read:profile"},
	}
	return cfg
}

func sig(id, cust, channel string, mk, attrs map[string]string, conf float64, ts int64) Signal {
	return Signal{
		ID: id, CustomerKey: cust, Channel: channel,
		MatchKeys: mk, Attributes: attrs, Confidence: conf,
		Consent: []string{"profile"}, ObservedAt: ts,
	}
}

func mustIngest(t *testing.T, e *Engine, sigs ...Signal) BatchResult {
	t.Helper()
	res, err := e.IngestBatch(sigs)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	return res
}

// 样本信号集：两个真实客户被拆成多条记录。
func sampleSignals() []Signal {
	return []Signal{
		sig("s1", "custA", "app", map[string]string{"email": "a@x.com"},
			map[string]string{"name": "Alice", "city": "BJ"}, 0.9, 100),
		sig("s2", "custB", "web", map[string]string{"email": "a@x.com"},
			map[string]string{"name": "Alicia"}, 0.5, 90),
		sig("s3", "custC", "crm", map[string]string{"phone": "138"},
			map[string]string{"name": "Alice Chen", "city": "SH"}, 0.8, 110),
		sig("s4", "custD", "app", map[string]string{"phone": "138", "email": "a@x.com"},
			map[string]string{"name": "Alice"}, 0.7, 105),
		sig("s5", "custE", "web", map[string]string{"device": "d1"},
			map[string]string{"name": "Bob"}, 0.6, 100),
	}
}

func snapshotOf(t *testing.T, e *Engine) ([]Cluster, map[string]Profile) {
	t.Helper()
	cs, err := e.Clusters("admin")
	if err != nil {
		t.Fatalf("clusters: %v", err)
	}
	ps := map[string]Profile{}
	for _, c := range cs {
		p, err := e.Profile("admin", c.ID)
		if err != nil {
			t.Fatalf("profile %s: %v", c.ID, err)
		}
		ps[c.ID] = p
	}
	return cs, ps
}

// 乱序、重复、迟到、任意批次划分下结果逐项一致。
func TestDeterminismUnderReorderAndBatching(t *testing.T) {
	base := sampleSignals()
	var wantCl []Cluster
	var wantPr map[string]Profile

	for trial := 0; trial < 8; trial++ {
		e, err := Open(testConfig(t))
		if err != nil {
			t.Fatal(err)
		}
		// 随机打乱 + 随机分批 + 重复与迟到（重复投递部分信号）。
		shuffled := append([]Signal(nil), base...)
		rand.New(rand.NewSource(int64(trial))).Shuffle(len(shuffled), func(i, j int) {
			shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
		})
		r := rand.New(rand.NewSource(int64(trial + 100)))
		i := 0
		for i < len(shuffled) {
			n := 1 + r.Intn(3)
			if i+n > len(shuffled) {
				n = len(shuffled) - i
			}
			batch := append([]Signal(nil), shuffled[i:i+n]...)
			if r.Intn(2) == 0 && i > 0 { // 迟到重复：重投上一条
				batch = append(batch, shuffled[i-1])
			}
			mustIngest(t, e, batch...)
			i += n
		}
		mustIngest(t, e, base[0]) // 整体重复投递，幂等
		cs, ps := snapshotOf(t, e)
		if trial == 0 {
			wantCl, wantPr = cs, ps
			continue
		}
		if !reflect.DeepEqual(cs, wantCl) {
			t.Fatalf("trial %d: clusters diverge:\n got %+v\nwant %+v", trial, cs, wantCl)
		}
		if !reflect.DeepEqual(ps, wantPr) {
			t.Fatalf("trial %d: profiles diverge", trial)
		}
		e.Close()
	}
}

// 跨渠道属性冲突消解：来源优先级 > 可信度 > 新近度，且结果可解释。
func TestAttributeConflictResolution(t *testing.T) {
	e, err := Open(testConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	mustIngest(t, e,
		sig("s1", "c1", "web", map[string]string{"email": "a@x.com"},
			map[string]string{"name": "web-name"}, 0.99, 999), // 新近且可信但优先级低
		sig("s2", "c2", "crm", map[string]string{"email": "a@x.com"},
			map[string]string{"name": "crm-name"}, 0.1, 1), // 优先级最高
	)
	cs, _ := e.Clusters("admin")
	if len(cs) != 1 {
		t.Fatalf("expect 1 cluster, got %d", len(cs))
	}
	p, _ := e.Profile("admin", cs[0].ID)
	got := p.Attributes["name"]
	if got.Value != "crm-name" || got.Source != "crm" {
		t.Fatalf("priority rule broken: %+v", got)
	}
	if got.Reason == "" {
		t.Fatal("resolution must be explainable")
	}

	// 同来源下可信度优先。
	e2, _ := Open(testConfig(t))
	defer e2.Close()
	mustIngest(t, e2,
		sig("s1", "c1", "app", map[string]string{"email": "b@x.com"},
			map[string]string{"name": "low"}, 0.3, 999),
		sig("s2", "c2", "app", map[string]string{"email": "b@x.com"},
			map[string]string{"name": "high"}, 0.9, 1),
	)
	cs2, _ := e2.Clusters("admin")
	p2, _ := e2.Profile("admin", cs2[0].ID)
	if p2.Attributes["name"].Value != "high" {
		t.Fatalf("confidence rule broken: %+v", p2.Attributes["name"])
	}
}

// 误合撤销：不影响其余簇；历史时点可见合并前/撤销前状态；撤销不复活。
func TestUndoMerge(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t)
	cfg.DataDir = dir
	e, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	mustIngest(t, e,
		sig("s1", "c1", "app", map[string]string{"email": "a@x.com"}, nil, 0.9, 1),
		sig("s2", "c2", "app", map[string]string{"email": "b@x.com"}, nil, 0.9, 1),
		sig("s3", "c3", "app", map[string]string{"email": "c@x.com"}, nil, 0.9, 1),
	)
	cs, _ := e.Clusters("admin")
	if len(cs) != 3 {
		t.Fatalf("expect 3 clusters, got %d", len(cs))
	}
	preMergeSeq := e.seq
	mergeID, err := e.Merge("admin", "C-s1", "C-s2")
	if err != nil {
		t.Fatal(err)
	}
	postMergeSeq := e.seq
	cs, _ = e.Clusters("admin")
	if len(cs) != 2 {
		t.Fatalf("after merge expect 2 clusters, got %d", len(cs))
	}

	if err := e.UndoMerge("admin", mergeID); err != nil {
		t.Fatal(err)
	}
	cs, _ = e.Clusters("admin")
	if len(cs) != 3 {
		t.Fatalf("after undo expect 3 clusters, got %d", len(cs))
	}

	// 历史时点：合并前 3 簇，撤销前 2 簇。
	st := newState()
	for _, ev := range e.events {
		if ev.Seq > preMergeSeq {
			break
		}
		st.apply(ev)
	}
	st.finalize()
	if len(st.clusters) != 3 {
		t.Fatalf("asof pre-merge expect 3, got %d", len(st.clusters))
	}
	st2 := newState()
	for _, ev := range e.events {
		if ev.Seq > postMergeSeq {
			break
		}
		st2.apply(ev)
	}
	st2.finalize()
	if len(st2.clusters) != 2 {
		t.Fatalf("asof post-merge expect 2, got %d", len(st2.clusters))
	}

	// 重启重放：撤销不得复活。
	e.Close()
	e2, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	cs, _ = e2.Clusters("admin")
	if len(cs) != 3 {
		t.Fatalf("after restart expect 3 clusters, got %d", len(cs))
	}
}

// 拆分后再归并，与直接归并一致。
func TestSplitThenMergeEqualsDirectMerge(t *testing.T) {
	mk := func() []Signal {
		return []Signal{
			sig("s1", "c1", "web", map[string]string{"email": "a@x.com"}, nil, 0.9, 1),
			sig("s2", "c2", "app", map[string]string{"email": "a@x.com", "phone": "138"}, nil, 0.9, 1),
			sig("s3", "c3", "crm", map[string]string{"phone": "138"}, nil, 0.9, 1),
		}
	}
	// 直接归并。
	e1, _ := Open(testConfig(t))
	defer e1.Close()
	mustIngest(t, e1, mk()...)
	direct, directP := snapshotOf(t, e1)
	if len(direct) != 1 {
		t.Fatalf("direct: expect 1 cluster, got %d", len(direct))
	}

	// 拆分（忽略 phone 边）后再合并。
	e2, _ := Open(testConfig(t))
	defer e2.Close()
	mustIngest(t, e2, mk()...)
	parts, err := e2.Split("admin", "C-s1", SplitRule{
		ExcludeKeyTypes: []string{"phone"}, Reason: "phone键疑似共享设备"})
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 2 {
		t.Fatalf("expect split into 2, got %v", parts)
	}
	if _, err := e2.Merge("admin", parts[0], parts[1]); err != nil {
		t.Fatal(err)
	}
	merged, mergedP := snapshotOf(t, e2)
	if len(merged) != 1 {
		t.Fatalf("after re-merge expect 1 cluster, got %d", len(merged))
	}
	if !reflect.DeepEqual(merged[0].SignalIDs, direct[0].SignalIDs) {
		t.Fatalf("re-merge diverges from direct merge: %v vs %v",
			merged[0].SignalIDs, direct[0].SignalIDs)
	}
	for cid, p := range mergedP {
		if !reflect.DeepEqual(p.Attributes, directP[cid].Attributes) {
			t.Fatalf("profile diverges after re-merge")
		}
	}
}

// 采集与查询并发：慢查询不阻塞接入（-race 下验证）。
func TestConcurrentIngestAndQuery(t *testing.T) {
	e, err := Open(testConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				id := fmt.Sprintf("w%d-s%d", w, i)
				if _, err := e.IngestBatch([]Signal{sig(id, id, "app",
					map[string]string{"device": fmt.Sprintf("d%d", i%10)},
					map[string]string{"name": id}, 0.5, int64(i))}); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				e.Clusters("analyst")
				e.ProfileAsOf("analyst", "C-w0-s0", uint64(i))
			}
		}()
	}
	wg.Wait()
	cs, _ := e.Clusters("admin")
	total := 0
	for _, c := range cs {
		total += len(c.SignalIDs)
	}
	if total != 200 {
		t.Fatalf("expect 200 signals, got %d", total)
	}
}

// 删除后不得复活；撤回同意后信号退出画像。
func TestDeleteAndConsent(t *testing.T) {
	e, err := Open(testConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	mustIngest(t, e,
		sig("s1", "custA", "app", map[string]string{"email": "a@x.com"},
			map[string]string{"name": "Alice"}, 0.9, 1),
		sig("s2", "custB", "web", map[string]string{"email": "a@x.com"},
			map[string]string{"name": "Alicia"}, 0.9, 1),
	)
	p, _ := e.Profile("admin", "C-s1")
	if len(p.Attributes) == 0 {
		t.Fatal("expect attributes")
	}

	// 撤回 custA 的画像同意：其属性退出画像。
	if err := e.WithdrawConsent("admin", "custA", []string{"profile"}); err != nil {
		t.Fatal(err)
	}
	p, _ = e.Profile("admin", "C-s1")
	for _, a := range p.Attributes {
		if a.Source == "app" {
			t.Fatalf("withdrawn signal still in profile: %+v", a)
		}
	}

	// 删除 custA：信号清除，重投被拒绝（不复活）。
	if err := e.DeleteCustomer("admin", "custA"); err != nil {
		t.Fatal(err)
	}
	res := mustIngest(t, e, sig("s1", "custA", "app",
		map[string]string{"email": "a@x.com"}, map[string]string{"name": "Alice"}, 0.9, 1))
	if res.Accepted != 0 || len(res.Rejected) != 1 {
		t.Fatalf("deleted customer signal must be rejected: %+v", res)
	}
	p, _ = e.Profile("admin", "C-s1")
	for _, id := range p.SignalIDs {
		if id == "s1" {
			t.Fatal("deleted signal resurrected in profile")
		}
	}
}

// 越权读取被拒绝并留痕。
func TestPermissionAudit(t *testing.T) {
	e, err := Open(testConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	mustIngest(t, e, sig("s1", "c1", "app", nil, nil, 0.9, 1))
	if _, err := e.Profile("outsider", "C-s1"); err != ErrPermission {
		t.Fatalf("expect ErrPermission, got %v", err)
	}
	found := false
	for _, a := range e.AuditLog() {
		if a.Actor == "outsider" && !a.Allowed {
			found = true
		}
	}
	if !found {
		t.Fatal("denied access must be audited")
	}
}

// 重启恢复：已提交状态不丢失；未提交批次被区分并丢弃。
func TestRestartRecovery(t *testing.T) {
	cfg := testConfig(t)
	e, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	mustIngest(t, e, sig("s1", "c1", "app", map[string]string{"email": "a@x.com"},
		map[string]string{"name": "Alice"}, 0.9, 1))
	mergeSrc := []Signal{
		sig("s2", "c2", "web", map[string]string{"phone": "1"}, nil, 0.9, 1),
		sig("s3", "c3", "crm", map[string]string{"phone": "2"}, nil, 0.9, 1),
	}
	mustIngest(t, e, mergeSrc...)
	if _, err := e.Merge("admin", "C-s2", "C-s3"); err != nil {
		t.Fatal(err)
	}
	want, wantP := snapshotOf(t, e)
	e.Close()

	// 模拟崩溃留下的未提交部分批次：追加一条无 commit 标记的信号事件。
	f, _ := os.OpenFile(filepath.Join(cfg.DataDir, "events.log"),
		os.O_WRONLY|os.O_APPEND, 0o644)
	ev := Event{Seq: 999, Kind: EvSignal, Signal: &Signal{ID: "ghost", CustomerKey: "c9",
		Channel: "app", MatchKeys: map[string]string{"email": "g@x.com"}}}
	b, _ := json.Marshal(ev)
	f.Write(append(b, '\n'))
	f.Close()

	e2, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	got, gotP := snapshotOf(t, e2)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("clusters differ after restart:\n got %+v\nwant %+v", got, want)
	}
	if !reflect.DeepEqual(gotP, wantP) {
		t.Fatal("profiles differ after restart")
	}
	for _, c := range got {
		for _, id := range c.SignalIDs {
			if id == "ghost" {
				t.Fatal("uncommitted batch must not be applied")
			}
		}
	}
}

// 超限与异常信号：可区分、可定位，不静默丢弃。
func TestLimitsAndQuarantine(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxBatchSize = 2
	e, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if _, err := e.IngestBatch(make([]Signal, 3)); !errors.Is(err, ErrLimit) {
		t.Fatalf("expect ErrLimit, got %v", err)
	}
	res := mustIngest(t, e,
		sig("ok", "c1", "app", nil, nil, 0.5, 1),
		sig("bad", "c2", "app", nil, nil, 1.5, 1), // 可信度越界
	)
	if res.Accepted != 1 || len(res.Rejected) != 1 {
		t.Fatalf("expect 1 accepted 1 rejected, got %+v", res)
	}
	q := e.Quarantined()
	if len(q) != 1 || q[0].Signal.ID != "bad" || q[0].Reason == "" {
		t.Fatalf("quarantine must locate bad signal: %+v", q)
	}
}
