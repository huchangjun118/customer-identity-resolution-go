package identity

import (
	"strings"
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// 来源优先级优先于可信度与新近度。
func TestResolveBySourcePriority(t *testing.T) {
	e := openTemp(t, nil)
	// web 更新更可信，但 crm 优先级更高。
	s1 := withAttr(sig("w1", "web", ident("email", "a@x.com")), "name", "WebName", 0.99, t0.Add(2*time.Hour))
	s2 := withAttr(sig("c1", "crm", ident("device", "d1")), "name", "CrmName", 0.5, t0)
	s3 := sig("link", "app", ident("email", "a@x.com"), ident("device", "d1"))
	if _, err := e.Ingest([]Signal{s1, s2, s3}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	p, err := e.Query(admin, ident("email", "a@x.com"))
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if p.Attributes["name"] != "CrmName" {
		t.Fatalf("priority should win: %v", p.Attributes)
	}
	res := p.Resolutions["name"]
	if res.Winner != "c1" || res.Reason == "" || len(res.Candidates) != 2 {
		t.Fatalf("resolution must be explainable: %+v", res)
	}
	if !strings.Contains(res.Reason, "crm") {
		t.Fatalf("reason should mention winning channel: %s", res.Reason)
	}
}

// 同渠道下可信度优先，其次时间新近度。
func TestResolveByConfidenceThenRecency(t *testing.T) {
	e := openTemp(t, nil)
	s1 := withAttr(sig("a1", "app", ident("email", "a@x.com")), "city", "BJ", 0.6, t0.Add(10*time.Hour))
	s2 := withAttr(sig("a2", "app", ident("device", "d1")), "city", "SH", 0.9, t0)
	s3 := withAttr(sig("a3", "app", ident("phone", "p1")), "city", "GZ", 0.9, t0.Add(5*time.Hour))
	link := sig("lk", "app", ident("email", "a@x.com"), ident("device", "d1"), ident("phone", "p1"))
	if _, err := e.Ingest([]Signal{s1, s2, s3, link}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	p, err := e.Query(admin, ident("email", "a@x.com"))
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	// a2 与 a3 可信度相同，a3 更新 → GZ 胜出；两者都胜过可信度更低的 a1。
	if p.Attributes["city"] != "GZ" {
		t.Fatalf("recency tiebreak failed: %v", p.Attributes)
	}
}

// 对外暴露的画像始终是某个自洽的完整版本：版本号单调，属性来自同一次构建。
func TestProfileVersionConsistency(t *testing.T) {
	e := openTemp(t, nil)
	s1 := withAttr(sig("s1", "crm", ident("email", "a@x.com")), "name", "A", 0.9, t0)
	if _, err := e.Ingest([]Signal{s1}); err != nil {
		t.Fatalf("ingest1: %v", err)
	}
	p1, _ := e.Query(admin, ident("email", "a@x.com"))
	s2 := withAttr(sig("s2", "crm", ident("email", "a@x.com")), "name", "B", 0.95, t0.Add(time.Hour))
	if _, err := e.Ingest([]Signal{s2}); err != nil {
		t.Fatalf("ingest2: %v", err)
	}
	p2, _ := e.Query(admin, ident("email", "a@x.com"))
	if p2.Version <= p1.Version {
		t.Fatalf("version must increase: %d -> %d", p1.Version, p2.Version)
	}
	if p2.Attributes["name"] != "B" {
		t.Fatalf("new version must be self-consistent: %v", p2.Attributes)
	}
	// 历史时点查询仍能看到旧版本。
	old, err := e.QueryAsOf(admin, ident("email", "a@x.com"), p1.Version)
	if err != nil {
		t.Fatalf("as-of: %v", err)
	}
	if old.Attributes["name"] != "A" {
		t.Fatalf("as-of must return old whole version: %v", old.Attributes)
	}
}

// 未获同意的信号不得进入画像。
func TestConsentGatesProfileAttributes(t *testing.T) {
	e := openTemp(t, nil)
	s1 := withAttr(sig("s1", "crm", ident("email", "a@x.com")), "name", "NoConsent", 0.99, t0)
	s1.Consent = Consent{"profile": false}
	s2 := withAttr(sig("s2", "web", ident("email", "a@x.com")), "name", "Consented", 0.5, t0)
	if _, err := e.Ingest([]Signal{s1, s2}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	p, err := e.Query(admin, ident("email", "a@x.com"))
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if p.Attributes["name"] != "Consented" {
		t.Fatalf("non-consented signal must not enter profile: %v", p.Attributes)
	}
}
