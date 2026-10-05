package identity

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func openTemp(t *testing.T, cfg func(*Config)) *Engine {
	t.Helper()
	c := DefaultConfig(t.TempDir())
	if cfg != nil {
		cfg(&c)
	}
	e, err := Open(c)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

func sig(id, channel string, idents ...Identifier) Signal {
	return Signal{
		ID:          id,
		Channel:     channel,
		Identifiers: idents,
		Consent:     Consent{"profile": true},
		ReceivedAt:  time.Now(),
	}
}

func ident(t2, v string) Identifier { return Identifier{Type: t2, Value: v} }

func withAttr(s Signal, name, value string, conf float64, at time.Time) Signal {
	s.Attributes = append(s.Attributes, Attribute{Name: name, Value: value, Confidence: conf, ObservedAt: at})
	return s
}

var admin = Reader{Actor: "admin", Scopes: []string{"*"}}

func TestIngestBasicAndQuery(t *testing.T) {
	e := openTemp(t, nil)
	res, err := e.Ingest([]Signal{
		withAttr(sig("s1", "crm", ident("email", "a@x.com")), "name", "Alice", 0.9, time.Now()),
		sig("s2", "app", ident("email", "a@x.com"), ident("device", "d1")),
	})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if !res.Committed || len(res.Items) != 2 {
		t.Fatalf("bad result: %+v", res)
	}
	for _, it := range res.Items {
		if it.Status != "applied" || it.ClusterID == "" {
			t.Fatalf("bad item: %+v", it)
		}
	}
	if res.Items[0].ClusterID != res.Items[1].ClusterID {
		t.Fatalf("signals sharing email must be one cluster: %+v", res.Items)
	}
	p, err := e.Query(admin, ident("device", "d1"))
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if p.Attributes["name"] != "Alice" {
		t.Fatalf("profile attrs: %v", p.Attributes)
	}
	if p.MemberCount != 2 {
		t.Fatalf("member count = %d", p.MemberCount)
	}
}

// 同一输入在任意到达顺序、任意批次划分下，最终归属与画像逐项一致。
func TestDeterministicUnderOrderAndBatching(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var signals []Signal
	for i := 0; i < 30; i++ {
		s := sig(fmt.Sprintf("s%02d", i), []string{"crm", "app", "web"}[i%3],
			ident("email", fmt.Sprintf("u%d@x.com", i%6)),
			ident("device", fmt.Sprintf("d%d", i%6)))
		s = withAttr(s, "city", []string{"BJ", "SH", "GZ"}[i%3], float64(i%5)/10+0.5, base.Add(time.Duration(i)*time.Hour))
		signals = append(signals, s)
	}

	run := func(order []int, batchSize int) map[string]Profile {
		e := openTemp(t, nil)
		for i := 0; i < len(order); i += batchSize {
			j := i + batchSize
			if j > len(order) {
				j = len(order)
			}
			var batch []Signal
			for _, k := range order[i:j] {
				batch = append(batch, signals[k])
			}
			if _, err := e.Ingest(batch); err != nil {
				t.Fatalf("ingest: %v", err)
			}
		}
		out := map[string]Profile{}
		for i := 0; i < 6; i++ {
			p, err := e.Query(admin, ident("email", fmt.Sprintf("u%d@x.com", i)))
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			out[p.ClusterID] = p
		}
		return out
	}

	seq := make([]int, len(signals))
	for i := range seq {
		seq[i] = i
	}
	want := run(seq, 7)

	rng := rand.New(rand.NewSource(42))
	for trial := 0; trial < 5; trial++ {
		order := rand.Perm(len(signals))
		got := run(order, 1+trial)
		if len(got) != len(want) {
			t.Fatalf("trial %d: cluster count %d != %d", trial, len(got), len(want))
		}
		for id, w := range want {
			g, ok := got[id]
			if !ok {
				t.Fatalf("trial %d: missing cluster %s", trial, id)
			}
			if g.MemberCount != w.MemberCount {
				t.Fatalf("trial %d: cluster %s members %d != %d", trial, id, g.MemberCount, w.MemberCount)
			}
			for k, v := range w.Attributes {
				if g.Attributes[k] != v {
					t.Fatalf("trial %d: cluster %s attr %s = %q, want %q", trial, id, k, g.Attributes[k], v)
				}
			}
		}
	}
	_ = rng
}

// 重复信号幂等：重复到达不改变结果，且不重复计数。
func TestDuplicateSignalsIdempotent(t *testing.T) {
	e := openTemp(t, nil)
	s := withAttr(sig("s1", "crm", ident("email", "a@x.com")), "name", "Alice", 0.9, time.Now())
	if _, err := e.Ingest([]Signal{s, s}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	res, err := e.Ingest([]Signal{s})
	if err != nil {
		t.Fatalf("re-ingest: %v", err)
	}
	if res.Items[0].Status != "duplicate" {
		t.Fatalf("expected duplicate, got %+v", res.Items[0])
	}
	p, err := e.Query(admin, ident("email", "a@x.com"))
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if p.MemberCount != 1 {
		t.Fatalf("member count = %d, want 1", p.MemberCount)
	}
}

// 异常信号给出可区分、可定位的结果，且不静默丢弃。
func TestInvalidSignalOutcome(t *testing.T) {
	e := openTemp(t, nil)
	res, err := e.Ingest([]Signal{
		{ID: "", Channel: "app", Identifiers: []Identifier{ident("email", "a@x.com")}},
		sig("ok", "app", ident("email", "b@x.com")),
	})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if res.Items[0].Status != "rejected" || res.Items[0].Err == "" {
		t.Fatalf("invalid signal must be rejected with reason: %+v", res.Items[0])
	}
	if res.Items[1].Status != "applied" {
		t.Fatalf("valid signal must apply: %+v", res.Items[1])
	}
}

// 批大小上限：超限批次整体拒绝并给出可区分错误。
func TestBatchBudgetExceeded(t *testing.T) {
	e := openTemp(t, func(c *Config) { c.Budgets.MaxSignalsPerBatch = 2 })
	res, err := e.Ingest([]Signal{
		sig("s1", "app", ident("email", "a@x.com")),
		sig("s2", "app", ident("email", "b@x.com")),
		sig("s3", "app", ident("email", "c@x.com")),
	})
	if err != ErrBatchTooLarge {
		t.Fatalf("err = %v", err)
	}
	if res.Committed {
		t.Fatalf("over-limit batch must not commit")
	}
	for _, it := range res.Items {
		if it.Status != "rejected" {
			t.Fatalf("item: %+v", it)
		}
	}
}
