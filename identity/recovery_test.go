package identity

import (
	"encoding/json"
	"testing"
)

// 重启后已提交的归并与画像不丢失、不重复应用。
func TestRestartRecovery(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	e, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	mustIngest(t, e,
		withAttr(sig("s1", "crm", ident("email", "a@x.com")), "name", "Alice", 0.9, t0),
		sig("s2", "app", ident("email", "a@x.com"), ident("device", "d1")),
	)
	mustIngest(t, e, sig("s3", "web", ident("device", "d1"), ident("phone", "p1")))
	before := mustQuery(t, e, ident("email", "a@x.com"))
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	e2, err := Open(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	after := mustQuery(t, e2, ident("email", "a@x.com"))
	if after.ClusterID != before.ClusterID {
		t.Fatalf("cluster ID changed across restart: %s -> %s", before.ClusterID, after.ClusterID)
	}
	if after.MemberCount != 3 || after.Attributes["name"] != "Alice" {
		t.Fatalf("profile lost across restart: %+v", after)
	}
	// 不重复应用：成员数不膨胀。
	if after.MemberCount != before.MemberCount {
		t.Fatalf("signals re-applied after restart: %d -> %d", before.MemberCount, after.MemberCount)
	}
	// 重复信号在重启后仍被识别为重复。
	res, err := e2.Ingest([]Signal{sig("s1", "crm", ident("email", "a@x.com"))})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if res.Items[0].Status != "duplicate" {
		t.Fatalf("duplicate not detected after restart: %+v", res.Items[0])
	}
	// 历史时点查询在重启后仍可用。
	old, err := e2.QueryAsOf(admin, ident("email", "a@x.com"), before.Version)
	if err != nil || old.Attributes["name"] != "Alice" {
		t.Fatalf("as-of after restart: %+v err=%v", old, err)
	}
	e2.Close()
}

// 未提交批次与已提交状态区分开：崩溃留下的半截批次不被应用，不留部分写入。
func TestUncommittedBatchDiscarded(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	e, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	mustIngest(t, e, sig("committed", "crm", ident("email", "a@x.com")))
	e.Close()

	// 模拟崩溃：直接往 WAL 追加一个没有 commit 的批次。
	st, err := openStore(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.append(event{Kind: "batch-begin", Payload: json.RawMessage(`{"id":"batch-x"}`)}); err != nil {
		t.Fatalf("append: %v", err)
	}
	raw, _ := json.Marshal(sig("uncommitted", "app", ident("email", "b@x.com")))
	if err := st.append(event{Kind: "signal", Payload: raw}); err != nil {
		t.Fatalf("append: %v", err)
	}
	st.close()

	e2, err := Open(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer e2.Close()
	if _, err := e2.Query(admin, ident("email", "b@x.com")); err != ErrClusterNotFound {
		t.Fatalf("uncommitted batch must not be applied: %v", err)
	}
	p := mustQuery(t, e2, ident("email", "a@x.com"))
	if p.MemberCount != 1 {
		t.Fatalf("committed state damaged: %+v", p)
	}
}

// 删除结果重启后依然有效：墓碑持久化，已删除内容不重放复活。
func TestEraseSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	e, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	mustIngest(t, e, sig("s1", "crm", ident("email", "a@x.com")))
	if err := e.Erase("dpo", ident("email", "a@x.com")); err != nil {
		t.Fatalf("erase: %v", err)
	}
	e.Close()

	e2, err := Open(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer e2.Close()
	if _, err := e2.Query(admin, ident("email", "a@x.com")); err != ErrClusterNotFound {
		t.Fatalf("erased subject resurrected after restart: %v", err)
	}
	res, err := e2.Ingest([]Signal{sig("late", "app", ident("email", "a@x.com"))})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if res.Items[0].Status != "rejected" {
		t.Fatalf("tombstone lost after restart: %+v", res.Items[0])
	}
}

// 拆分结果重启后保持，且重放结果与在线结果一致。
func TestSplitSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	e, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	mustIngest(t, e,
		sig("w1", "web", ident("email", "a@x.com")),
		sig("c1", "crm", ident("email", "a@x.com"), ident("phone", "p1")),
	)
	ids, _, err := e.Split("ops", ident("email", "a@x.com"), "by-channel")
	if err != nil {
		t.Fatalf("split: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("split: %v", ids)
	}
	e.Close()

	e2, err := Open(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer e2.Close()
	// 重放后仍是两个簇。
	var total int
	for _, cid := range ids {
		p, err := e2.Inspect(admin, cid)
		if err != nil {
			t.Fatalf("split cluster %s lost after restart: %v", cid, err)
		}
		total += p.MemberCount
	}
	if total != 2 {
		t.Fatalf("membership changed after restart: %d", total)
	}
}
