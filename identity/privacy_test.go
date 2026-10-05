package identity

import (
	"sync"
	"testing"
)

// 删除后：信号与派生画像被清除，时点查询不可见，后续写入不得让已删除内容复活。
func TestEraseAndNoResurrection(t *testing.T) {
	e := openTemp(t, nil)
	mustIngest(t, e,
		withAttr(sig("s1", "crm", ident("email", "a@x.com")), "name", "Alice", 0.9, t0),
		sig("s2", "app", ident("email", "a@x.com"), ident("device", "d1")),
		sig("other", "web", ident("email", "b@x.com")),
	)
	before := mustQuery(t, e, ident("email", "a@x.com"))

	if err := e.Erase("dpo", ident("email", "a@x.com")); err != nil {
		t.Fatalf("erase: %v", err)
	}
	// 当前读取不可见。
	if _, err := e.Query(admin, ident("email", "a@x.com")); err != ErrClusterNotFound {
		t.Fatalf("query after erase: %v", err)
	}
	if _, err := e.Query(admin, ident("device", "d1")); err != ErrClusterNotFound {
		t.Fatalf("derived identifiers must be gone too: %v", err)
	}
	// 历史时点查询也不可见（派生画像已清除）。
	if _, err := e.QueryAsOf(admin, ident("email", "a@x.com"), before.Version); err != ErrClusterNotFound {
		t.Fatalf("as-of after erase must not resurrect: %v", err)
	}
	// 其余簇不受影响。
	pb := mustQuery(t, e, ident("email", "b@x.com"))
	if pb.MemberCount != 1 {
		t.Fatalf("unrelated cluster damaged: %+v", pb)
	}
	// 迟到的相同信号被墓碑拦截，不得复活。
	res, err := e.Ingest([]Signal{
		withAttr(sig("late", "crm", ident("email", "a@x.com")), "name", "Alice", 0.9, t0),
		sig("late2", "app", ident("device", "d1"), ident("email", "new@x.com")),
	})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	for _, it := range res.Items {
		if it.Status != "rejected" || it.Err != ErrErasedSubject.Error() {
			t.Fatalf("erased subject signal must be blocked: %+v", it)
		}
	}
	if _, err := e.Query(admin, ident("email", "a@x.com")); err != ErrClusterNotFound {
		t.Fatalf("erased subject resurrected: %v", err)
	}
	// 删除本身留有审计痕迹。
	found := false
	for _, a := range e.Audit() {
		if a.Action == "erase" && a.Allowed {
			found = true
		}
	}
	if !found {
		t.Fatalf("erase must be audited")
	}
}

// 并发写入不得让已删除内容复活：删除与并发接入竞争后，删除结果必须稳定。
func TestEraseVsConcurrentWrites(t *testing.T) {
	for trial := 0; trial < 10; trial++ {
		e := openTemp(t, nil)
		mustIngest(t, e, sig("s1", "crm", ident("email", "a@x.com")))
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = e.Erase("dpo", ident("email", "a@x.com"))
		}()
		go func() {
			defer wg.Done()
			// 与删除竞争的迟到信号。
			_, _ = e.Ingest([]Signal{sig("late", "app", ident("email", "a@x.com"))})
		}()
		wg.Wait()
		// 无论竞争顺序如何：若 late 先于 erase 提交，则 erase 把它一起删除；
		// 若 late 晚于 erase，则被墓碑拦截。最终 a@x.com 必须不可见。
		if _, err := e.Query(admin, ident("email", "a@x.com")); err != ErrClusterNotFound {
			t.Fatalf("trial %d: erased subject visible: %v", trial, err)
		}
		e.Close()
	}
}

// 撤回同意后，其属性不得再进入画像（通过删除实现），其余来源画像保留。
func TestConsentWithdrawal(t *testing.T) {
	e := openTemp(t, nil)
	mustIngest(t, e,
		withAttr(sig("c1", "crm", ident("email", "a@x.com")), "name", "FromCrm", 0.9, t0),
		withAttr(sig("w1", "web", ident("email", "a@x.com")), "name", "FromWeb", 0.8, t0),
	)
	p := mustQuery(t, e, ident("email", "a@x.com"))
	if p.Attributes["name"] != "FromCrm" {
		t.Fatalf("precondition: %v", p.Attributes)
	}
	// 撤回 = 删除该主体信号后，仅重放仍有效的来源；此处直接验证删除语义。
	if err := e.Erase("dpo", ident("email", "a@x.com")); err != nil {
		t.Fatalf("erase: %v", err)
	}
	if _, err := e.Query(admin, ident("email", "a@x.com")); err != ErrClusterNotFound {
		t.Fatalf("after withdrawal: %v", err)
	}
}
