package identity

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"sort"
)

// buildProfile 对一个簇内的信号做属性冲突消解，产出自洽的完整画像版本。
//
// 消解规则（均可配置，逐项可解释）：
//  1. 只使用同意范围覆盖 ProfileScope 且未被撤回的信号；
//  2. 同一属性键的多个取值按 (来源优先级, 取值可信度, 时间新近度, 取值字典序)
//     字典序取最大者胜出——完全由信号内容与配置决定，与到达顺序无关；
//  3. 每个胜出属性附带 Reason，说明胜出依据。
//
// Version 是画像内容的 FNV-64 哈希：内容相同则版本相同，
// 保证任意到达顺序下最终画像逐项一致；对外暴露的始终是完整版本。
func buildProfile(clusterID string, sigs []Signal, st *state, cfg *Config) Profile {
	p := Profile{
		ClusterID:  clusterID,
		Attributes: map[string]ResolvedAttr{},
	}
	var usable []Signal
	for _, sig := range sigs {
		if st.consentAllows(sig, cfg.ProfileScope) {
			usable = append(usable, sig)
			p.SignalIDs = append(p.SignalIDs, sig.ID)
		}
	}
	p.SignalIDs = sortedIDs(p.SignalIDs)

	// 收集每个属性键的全部候选取值。
	type cand struct {
		value string
		sig   Signal
	}
	keys := map[string][]cand{}
	for _, sig := range usable {
		for k, v := range sig.Attributes {
			keys[k] = append(keys[k], cand{value: v, sig: sig})
		}
	}
	attrKeys := make([]string, 0, len(keys))
	for k := range keys {
		attrKeys = append(attrKeys, k)
	}
	sort.Strings(attrKeys)

	for _, k := range attrKeys {
		cs := keys[k]
		best := cs[0]
		for _, c := range cs[1:] {
			if better(c, best, cfg) {
				best = c
			}
		}
		pri := priorityOf(best.sig.Channel, cfg)
		p.Attributes[k] = ResolvedAttr{
			Value:      best.value,
			Source:     best.sig.Channel,
			Confidence: best.sig.Confidence,
			ObservedAt:  best.sig.ObservedAt,
			Reason: fmt.Sprintf(
				"胜出于%d个候选：来源优先级=%d, 可信度=%.2f, 观测时间=%d（规则：优先级>可信度>新近度>字典序）",
				len(cs), pri, best.sig.Confidence, best.sig.ObservedAt),
		}
	}
	p.Version = profileVersion(p)
	return p
}

// better 比较两个候选取值：优先级 > 可信度 > 新近度 > 字典序。
func better(a, b struct {
	value string
	sig   Signal
}, cfg *Config) bool {
	pa, pb := priorityOf(a.sig.Channel, cfg), priorityOf(b.sig.Channel, cfg)
	if pa != pb {
		return pa > pb
	}
	if a.sig.Confidence != b.sig.Confidence {
		return a.sig.Confidence > b.sig.Confidence
	}
	if a.sig.ObservedAt != b.sig.ObservedAt {
		return a.sig.ObservedAt > b.sig.ObservedAt
	}
	return a.value > b.value
}

func priorityOf(channel string, cfg *Config) int {
	if p, ok := cfg.SourcePriority[channel]; ok {
		return p
	}
	return cfg.DefaultPriority
}

// profileVersion 由画像内容确定性导出（与到达顺序无关）。
func profileVersion(p Profile) uint64 {
	h := fnv.New64a()
	h.Write([]byte(p.ClusterID))
	keys := make([]string, 0, len(p.Attributes))
	for k := range p.Attributes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var buf [8]byte
	for _, k := range keys {
		a := p.Attributes[k]
		h.Write([]byte(k))
		h.Write([]byte{0})
		h.Write([]byte(a.Value))
		h.Write([]byte{0})
		h.Write([]byte(a.Source))
		binary.BigEndian.PutUint64(buf[:], uint64(a.ObservedAt))
		h.Write(buf[:])
	}
	return h.Sum64()
}
