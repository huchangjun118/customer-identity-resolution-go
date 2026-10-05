// idemodemo 用本地样本信号流演示身份归并与统一画像链路。
// 不依赖任何外部服务与真实凭据，结论可在本地稳定复现。
package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"

	identity "github.com/huchangjun118/customer-identity-resolution-go/identity"
)

func main() {
	dir, err := os.MkdirTemp("", "idemo-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := identity.DefaultConfig(dir)
	cfg.Actors = map[string][]string{
		"admin":   {"*"},
		"analyst": {"read:profile"},
	}
	e, err := identity.Open(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer e.Close()

	// 样本信号流：同一客户被不同渠道拆成多条记录，属性互相冲突。
	signals := []identity.Signal{
		{ID: "s1", CustomerKey: "web-001", Channel: "web",
			MatchKeys: map[string]string{"email": "alice@x.com"},
			Attributes: map[string]string{"name": "Alicia", "city": "BJ"},
			Confidence: 0.5, Consent: []string{"profile"}, ObservedAt: 90},
		{ID: "s2", CustomerKey: "app-777", Channel: "app",
			MatchKeys: map[string]string{"email": "alice@x.com", "device": "d-1"},
			Attributes: map[string]string{"name": "Alice"},
			Confidence: 0.9, Consent: []string{"profile"}, ObservedAt: 100},
		{ID: "s3", CustomerKey: "crm-42", Channel: "crm",
			MatchKeys: map[string]string{"device": "d-1"},
			Attributes: map[string]string{"name": "Alice Chen", "city": "SH", "vip": "gold"},
			Confidence: 0.8, Consent: []string{"profile"}, ObservedAt: 110},
		{ID: "s4", CustomerKey: "web-002", Channel: "web",
			MatchKeys: map[string]string{"email": "bob@x.com"},
			Attributes: map[string]string{"name": "Bob"},
			Confidence: 0.6, Consent: []string{"profile"}, ObservedAt: 95},
	}

	// 乱序、随机分批、重复投递。
	shuffled := append([]identity.Signal(nil), signals...)
	rand.New(rand.NewSource(42)).Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})
	for i := 0; i < len(shuffled); i += 2 {
		batch := append([]identity.Signal(nil), shuffled[i:min(i+2, len(shuffled))]...)
		if i > 0 {
			batch = append(batch, shuffled[i-1]) // 迟到重复
		}
		res, err := e.IngestBatch(batch)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("批次#%d: 接受%d 拒绝%d\n", res.BatchID, res.Accepted, len(res.Rejected))
	}

	fmt.Println("\n== 身份簇与统一画像 ==")
	clusters, err := e.Clusters("analyst")
	if err != nil {
		log.Fatal(err)
	}
	for _, c := range clusters {
		p, err := e.Profile("analyst", c.ID)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("簇 %s 信号=%v 画像版本=%d\n", c.ID, c.SignalIDs, p.Version)
		for k, v := range p.Attributes {
			fmt.Printf("  %s = %s (来源=%s)\n    依据: %s\n", k, v.Value, v.Source, v.Reason)
		}
	}

	fmt.Println("\n== 越权读取留痕 ==")
	if _, err := e.Profile("outsider", clusters[0].ID); err != nil {
		fmt.Println("拒绝:", err)
	}
	for _, a := range e.AuditLog() {
		if !a.Allowed {
			fmt.Printf("审计: actor=%s action=%s allowed=%v\n", a.Actor, a.Action, a.Allowed)
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
