package identity

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// 采集与查询并发：慢查询不阻塞接入，读写结果一致（配合 -race 运行）。
func TestConcurrentIngestAndQuery(t *testing.T) {
	e := openTemp(t, nil)
	const batches = 20
	const perBatch = 10

	var wg sync.WaitGroup
	// 一个写入者持续接入。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for b := 0; b < batches; b++ {
			var sigs []Signal
			for i := 0; i < perBatch; i++ {
				n := b*perBatch + i
				sigs = append(sigs, withAttr(
					sig(fmt.Sprintf("s%04d", n), "app",
						ident("email", fmt.Sprintf("u%d@x.com", n%50)),
						ident("device", fmt.Sprintf("d%d", n%50))),
					"city", "BJ", 0.8, t0))
			}
			if _, err := e.Ingest(sigs); err != nil {
				t.Errorf("ingest: %v", err)
				return
			}
		}
	}()
	// 多个读者持续查询（含慢查询：每次查询遍历多个标识）。
	var readErrs sync.Map
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				for k := 0; k < 10; k++ {
					id := ident("email", fmt.Sprintf("u%d@x.com", (i+k)%50))
					p, err := e.Query(admin, id)
					if err == ErrClusterNotFound {
						continue // 尚未接入，合法
					}
					if err != nil {
						readErrs.Store(r, err)
						return
					}
					// 读到的画像必须是某个自洽版本：属性与消解结果一致。
					if p.Attributes["city"] != p.Resolutions["city"].Value {
						readErrs.Store(r, fmt.Errorf("inconsistent profile: %+v", p))
						return
					}
				}
			}
		}(r)
	}
	wg.Wait()
	for _, r := range []int{0, 1, 2, 3} {
		if v, ok := readErrs.Load(r); ok {
			t.Fatalf("reader %d: %v", r, v)
		}
	}
	// 全部接入完成后，归属确定。
	p := mustQuery(t, e, ident("email", "u0@x.com"))
	if p.MemberCount != batches*perBatch/50 {
		t.Fatalf("members = %d, want %d", p.MemberCount, batches*perBatch/50)
	}
}

// 慢查询不拖垮接入吞吐：读路径无锁，接入在读者存在时仍能快速完成。
func TestSlowQueryDoesNotBlockIngest(t *testing.T) {
	e := openTemp(t, nil)
	mustIngest(t, e, sig("seed", "app", ident("email", "a@x.com")))

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = e.Query(admin, ident("email", "a@x.com"))
				}
			}
		}()
	}
	start := time.Now()
	for i := 0; i < 50; i++ {
		mustIngest(t, e, sig(fmt.Sprintf("s%d", i), "app", ident("email", fmt.Sprintf("b%d@x.com", i))))
	}
	elapsed := time.Since(start)
	close(stop)
	wg.Wait()
	if elapsed > 10*time.Second {
		t.Fatalf("ingest blocked by readers: %v", elapsed)
	}
}

// 跨权限读取必须被拒绝并留痕。
func TestPermissionDeniedAndAudited(t *testing.T) {
	e := openTemp(t, nil)
	mustIngest(t, e, sig("s1", "crm", ident("email", "a@x.com")))

	webOnly := Reader{Actor: "web-analyst", Scopes: []string{"web"}}
	if _, err := e.Query(webOnly, ident("email", "a@x.com")); err != ErrPermissionDenied {
		t.Fatalf("err = %v", err)
	}
	crmReader := Reader{Actor: "crm-analyst", Scopes: []string{"crm"}}
	if _, err := e.Query(crmReader, ident("email", "a@x.com")); err != nil {
		t.Fatalf("in-scope read should succeed: %v", err)
	}
	// 审计留痕。
	found := false
	for _, a := range e.Audit() {
		if a.Actor == "web-analyst" && a.Action == "query" && !a.Allowed {
			found = true
		}
	}
	if !found {
		t.Fatalf("denied cross-scope read must be audited: %+v", e.Audit())
	}
}
