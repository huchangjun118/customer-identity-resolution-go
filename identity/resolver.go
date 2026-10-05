package identity

import "fmt"

// Resolution 记录一个属性冲突消解的结果与判定依据（可解释）。
type Resolution struct {
	Value      string   `json:"value"`
	Winner     string   `json:"winner"`     // 胜出信号 ID
	Reason     string   `json:"reason"`     // 人类可读的判定依据
	Candidates []string `json:"candidates"` // 参与竞争的信号 ID
}

// candidate 把一个属性取值与其来源信号关联起来。
type candidate struct {
	attr     Attribute
	signalID string
	channel  string
}

// resolveAttribute 在一组候选属性中按确定性次序选出胜出值：
//  1. 来源优先级（配置，越大越优先）
//  2. 取值可信度（confidence，越大越优先）
//  3. 时间新近度（ObservedAt，越新越优先）
//  4. 值字典序（最终兜底，保证与到达顺序无关）
//
// 该次序是全序，因此任意到达顺序、任意批次划分、任意并发度下结果逐项一致。
func resolveAttribute(cfg Config, cands []candidate) Resolution {
	if len(cands) == 0 {
		return Resolution{}
	}
	best := 0
	for i := 1; i < len(cands); i++ {
		if lessCandidate(cfg, cands[best], cands[i]) {
			best = i
		}
	}
	w := cands[best]
	ids := make([]string, 0, len(cands))
	for _, c := range cands {
		ids = append(ids, c.signalID)
	}
	return Resolution{
		Value:  w.attr.Value,
		Winner: w.signalID,
		Reason: fmt.Sprintf("channel=%s(priority=%d) confidence=%.2f observedAt=%s value=%q 按 来源优先级>可信度>新近度>字典序 胜出",
			w.channel, cfg.priorityOf(w.channel), w.attr.Confidence,
			w.attr.ObservedAt.UTC().Format("2006-01-02T15:04:05Z"), w.attr.Value),
		Candidates: ids,
	}
}

// lessCandidate 判断 a 是否应排在 b 之后（即 b 更优）。
func lessCandidate(cfg Config, a, b candidate) bool {
	pa, pb := cfg.priorityOf(a.channel), cfg.priorityOf(b.channel)
	if pa != pb {
		return pa < pb
	}
	if a.attr.Confidence != b.attr.Confidence {
		return a.attr.Confidence < b.attr.Confidence
	}
	if !a.attr.ObservedAt.Equal(b.attr.ObservedAt) {
		return a.attr.ObservedAt.Before(b.attr.ObservedAt)
	}
	if a.attr.Value != b.attr.Value {
		return a.attr.Value > b.attr.Value // 字典序小者胜
	}
	return a.signalID > b.signalID // 完全并列时按信号 ID 兜底
}
