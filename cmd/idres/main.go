// idres 是本进程 + 本地存储的客户身份归并演示：
// 使用本地样本信号流演示批量接入、冲突消解、误合撤销、拆分再归并与删除。
package main

import (
	"fmt"
	"log"
	"os"
	"time"

	identity "github.com/huchangjun118/customer-identity-resolution-go/identity"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func sig(id, channel string, consented bool, idents ...identity.Identifier) identity.Signal {
	return identity.Signal{
		ID:          id,
		Channel:     channel,
		Identifiers: idents,
		Consent:     identity.Consent{"profile": consented},
		ReceivedAt:  time.Now(),
	}
}

func attr(s identity.Signal, name, value string, conf float64, hours int) identity.Signal {
	s.Attributes = append(s.Attributes, identity.Attribute{
		Name: name, Value: value, Confidence: conf, ObservedAt: t0.Add(time.Duration(hours) * time.Hour),
	})
	return s
}

func main() {
	dir := os.TempDir() + "/idres-demo"
	os.RemoveAll(dir)
	e, err := identity.Open(identity.DefaultConfig(dir))
	if err != nil {
		log.Fatal(err)
	}
	defer e.Close()
	admin := identity.Reader{Actor: "admin", Scopes: []string{"*"}}

	fmt.Println("== 1. 批量接入（乱序、重复、迟到） ==")
	res, err := e.Ingest([]identity.Signal{
		attr(sig("web-1", "web", true, identity.Identifier{Type: "email", Value: "alice@x.com"}), "name", "Alice(web)", 0.6, 2),
		attr(sig("crm-1", "crm", true, identity.Identifier{Type: "phone", Value: "13800000000"}), "name", "Alice", 0.95, 1),
		sig("app-1", "app", true, identity.Identifier{Type: "email", Value: "alice@x.com"}, identity.Identifier{Type: "phone", Value: "13800000000"}),
		attr(sig("web-1", "web", true, identity.Identifier{Type: "email", Value: "alice@x.com"}), "name", "Alice(web)", 0.6, 2), // 重复
	})
	must(err)
	for _, it := range res.Items {
		fmt.Printf("  %-8s -> %-9s cluster=%s %s\n", it.SignalID, it.Status, it.ClusterID, it.Err)
	}

	p, err := e.Query(admin, identity.Identifier{Type: "email", Value: "alice@x.com"})
	must(err)
	fmt.Printf("画像 v%d 成员=%d name=%s\n  消解依据: %s\n", p.Version, p.MemberCount, p.Attributes["name"], p.Resolutions["name"].Reason)

	fmt.Println("== 2. 手工合并与误合撤销 ==")
	must(func() error {
		_, err := e.Ingest([]identity.Signal{sig("crm-2", "crm", true, identity.Identifier{Type: "email", Value: "bob@x.com"})})
		return err
	}())
	mid, err := e.Merge("ops", identity.Identifier{Type: "email", Value: "alice@x.com"}, identity.Identifier{Type: "email", Value: "bob@x.com"})
	must(err)
	merged, _ := e.Query(admin, identity.Identifier{Type: "email", Value: "bob@x.com"})
	fmt.Printf("  合并 %s：bob 并入簇 %s（成员 %d）\n", mid, merged.ClusterID, merged.MemberCount)
	must(e.UndoMerge("ops", mid))
	back, _ := e.Query(admin, identity.Identifier{Type: "email", Value: "bob@x.com"})
	fmt.Printf("  撤销后：bob 回到簇 %s（成员 %d）\n", back.ClusterID, back.MemberCount)

	fmt.Println("== 3. 拆分与再归并 ==")
	ids, rationale, err := e.Split("ops", identity.Identifier{Type: "email", Value: "alice@x.com"}, "by-channel")
	must(err)
	fmt.Printf("  拆分依据: %s\n  拆出簇: %v\n", rationale, ids)
	_, err = e.Ingest([]identity.Signal{sig("app-2", "app", true,
		identity.Identifier{Type: "email", Value: "alice@x.com"},
		identity.Identifier{Type: "phone", Value: "13800000000"})})
	must(err)
	reunited, _ := e.Query(admin, identity.Identifier{Type: "email", Value: "alice@x.com"})
	fmt.Printf("  新信号到达后重新归并：簇 %s 成员 %d\n", reunited.ClusterID, reunited.MemberCount)

	fmt.Println("== 4. 权限与删除 ==")
	webOnly := identity.Reader{Actor: "web-analyst", Scopes: []string{"web"}}
	if _, err := e.Query(webOnly, identity.Identifier{Type: "email", Value: "bob@x.com"}); err != nil {
		fmt.Printf("  跨权限读取被拒绝: %v\n", err)
	}
	must(e.Erase("dpo", identity.Identifier{Type: "email", Value: "bob@x.com"}))
	fmt.Println("  bob 已删除；其信号与派生画像已清除，迟到信号将被墓碑拦截")

	fmt.Println("== 5. 审计留痕 ==")
	for _, a := range e.Audit() {
		fmt.Printf("  [%s] actor=%s action=%s allowed=%v %s\n", a.At.Format("15:04:05"), a.Actor, a.Action, a.Allowed, a.Detail)
	}
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
