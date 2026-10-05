package identity

import (
	"testing"
)

func mustIngest(t *testing.T, e *Engine, sigs ...Signal) {
	t.Helper()
	if _, err := e.Ingest(sigs); err != nil {
		t.Fatalf("ingest: %v", err)
	}
}

func mustQuery(t *testing.T, e *Engine, id Identifier) Profile {
	t.Helper()
	p, err := e.Query(admin, id)
	if err != nil {
		t.Fatalf("query %v: %v", id, err)
	}
	return p
}

// 误合撤销：撤销后两簇恢复原状，其余簇不受影响，历史时点可见合并前/后状态。
func TestMergeAndUndo(t *testing.T) {
	e := openTemp(t, nil)
	mustIngest(t, e, withAttr(sig("a1", "crm", ident("email", "a@x.com")), "name", "Alice", 0.9, t0))
	mustIngest(t, e, withAttr(sig("b1", "crm", ident("email", "b@x.com")), "name", "Bob", 0.9, t0))
	mustIngest(t, e, sig("c1", "app", ident("email", "c@x.com")))

	paBefore := mustQuery(t, e, ident("email", "a@x.com"))
	pbBefore := mustQuery(t, e, ident("email", "b@x.com"))
	if paBefore.ClusterID == pbBefore.ClusterID {
		t.Fatalf("should be separate clusters")
	}

	mid, err := e.Merge("ops", ident("email", "a@x.com"), ident("email", "b@x.com"))
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	paMerged := mustQuery(t, e, ident("email", "a@x.com"))
	pbMerged := mustQuery(t, e, ident("email", "b@x.com"))
	if paMerged.ClusterID != pbMerged.ClusterID {
		t.Fatalf("merge did not unify clusters")
	}
	mergedVersion := paMerged.Version

	if err := e.UndoMerge("ops", mid); err != nil {
		t.Fatalf("undo: %v", err)
	}
	paAfter := mustQuery(t, e, ident("email", "a@x.com"))
	pbAfter := mustQuery(t, e, ident("email", "b@x.com"))
	if paAfter.ClusterID == pbAfter.ClusterID {
		t.Fatalf("undo must separate clusters")
	}
	if paAfter.ClusterID != paBefore.ClusterID || pbAfter.ClusterID != pbBefore.ClusterID {
		t.Fatalf("undo must restore original cluster IDs: %s/%s vs %s/%s",
			paAfter.ClusterID, paBefore.ClusterID, pbAfter.ClusterID, pbBefore.ClusterID)
	}
	if paAfter.Attributes["name"] != "Alice" || pbAfter.Attributes["name"] != "Bob" {
		t.Fatalf("undo must restore profiles: %v / %v", paAfter.Attributes, pbAfter.Attributes)
	}
	// 其余身份簇不受影响。
	pc := mustQuery(t, e, ident("email", "c@x.com"))
	if pc.MemberCount != 1 {
		t.Fatalf("unrelated cluster damaged: %+v", pc)
	}
	// 历史时点查询：合并前状态与合并期间状态都可见。
	pre, err := e.QueryAsOf(admin, ident("email", "a@x.com"), paBefore.Version)
	if err != nil || pre.Attributes["name"] != "Alice" || pre.MemberCount != 1 {
		t.Fatalf("as-of pre-merge: %+v err=%v", pre, err)
	}
	during, err := e.QueryAsOf(admin, ident("email", "a@x.com"), mergedVersion)
	if err != nil || during.MemberCount != 2 {
		t.Fatalf("as-of during-merge: %+v err=%v", during, err)
	}
	// 重复撤销报错。
	if err := e.UndoMerge("ops", mid); err != ErrMergeNotFound {
		t.Fatalf("double undo: %v", err)
	}
}

// 撤销后的归属不会在重放（重启）中复活。
func TestUndoSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	e, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	mustIngest(t, e, sig("a1", "crm", ident("email", "a@x.com")))
	mustIngest(t, e, sig("b1", "crm", ident("email", "b@x.com")))
	mid, _ := e.Merge("ops", ident("email", "a@x.com"), ident("email", "b@x.com"))
	if err := e.UndoMerge("ops", mid); err != nil {
		t.Fatalf("undo: %v", err)
	}
	e.Close()

	e2, err := Open(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer e2.Close()
	pa := mustQuery(t, e2, ident("email", "a@x.com"))
	pb := mustQuery(t, e2, ident("email", "b@x.com"))
	if pa.ClusterID == pb.ClusterID {
		t.Fatalf("undone merge revived after restart")
	}
}

// 拆分再归并：按渠道拆分后，再经正常归并必须得到与直接归并一致的结果。
func TestSplitThenReMerge(t *testing.T) {
	mkSignals := func() []Signal {
		return []Signal{
			withAttr(sig("w1", "web", ident("email", "a@x.com")), "name", "WebA", 0.6, t0),
			withAttr(sig("c1", "crm", ident("email", "a@x.com"), ident("phone", "p1")), "name", "CrmA", 0.9, t0),
			sig("c2", "crm", ident("phone", "p1"), ident("device", "d1")),
		}
	}
	// 参照：直接归并。
	e1 := openTemp(t, nil)
	mustIngest(t, e1, mkSignals()...)
	want := mustQuery(t, e1, ident("email", "a@x.com"))
	if want.MemberCount != 3 {
		t.Fatalf("direct merge members = %d", want.MemberCount)
	}

	// 实验：拆分后再归并。
	e2 := openTemp(t, nil)
	mustIngest(t, e2, mkSignals()...)
	ids, rationale, err := e2.Split("ops", ident("email", "a@x.com"), "by-channel")
	if err != nil {
		t.Fatalf("split: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("by-channel split should yield 2 clusters, got %v (%s)", ids, rationale)
	}
	if rationale == "" {
		t.Fatalf("split must record rationale")
	}
	// 拆分后 web 与 crm 各自成簇，成员合计不变。
	var total int
	seen := map[string]bool{}
	for _, cid := range ids {
		p, err := e2.Inspect(admin, cid)
		if err != nil {
			t.Fatalf("inspect %s: %v", cid, err)
		}
		total += p.MemberCount
		for _, m := range p.Members {
			if seen[m] {
				t.Fatalf("signal %s in two clusters after split", m)
			}
			seen[m] = true
		}
	}
	if total != 3 {
		t.Fatalf("split must preserve membership, got %d", total)
	}
	// 再归并：新信号把两组重新连起来（正常归并路径）。
	mustIngest(t, e2, sig("bridge", "app", ident("email", "a@x.com"), ident("device", "d1")))
	got := mustQuery(t, e2, ident("email", "a@x.com"))
	wantMembers := map[string]bool{"w1": true, "c1": true, "c2": true, "bridge": true}
	if got.MemberCount != len(wantMembers) {
		t.Fatalf("re-merge members = %d, want %d", got.MemberCount, len(wantMembers))
	}
	for _, m := range got.Members {
		if !wantMembers[m] {
			t.Fatalf("unexpected member %s after re-merge", m)
		}
	}
	// 与直接归并的画像一致（name 由 crm 高优先级胜出）。
	if got.Attributes["name"] != want.Attributes["name"] {
		t.Fatalf("re-merge profile %v != direct %v", got.Attributes, want.Attributes)
	}
}

// 未知拆分规则与未知簇给出可区分错误。
func TestSplitErrors(t *testing.T) {
	e := openTemp(t, nil)
	mustIngest(t, e, sig("s1", "app", ident("email", "a@x.com")))
	if _, _, err := e.Split("ops", ident("email", "a@x.com"), "nope"); err != ErrSplitRuleNotFound {
		t.Fatalf("err = %v", err)
	}
	if _, _, err := e.Split("ops", ident("email", "ghost@x.com"), "by-channel"); err != ErrClusterNotFound {
		t.Fatalf("err = %v", err)
	}
}
