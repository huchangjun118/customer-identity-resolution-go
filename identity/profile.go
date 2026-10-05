package identity

import "sort"

// Profile 是某一身份簇在某个版本上的完整自洽画像。
// 对外暴露的永远是某个完整版本，不存在字段级混合。
type Profile struct {
	ClusterID   string                `json:"clusterId"`
	Version     uint64                `json:"version"`
	Attributes  map[string]string     `json:"attributes"`
	Resolutions map[string]Resolution `json:"resolutions"` // 每个属性的消解依据
	MemberCount int                   `json:"memberCount"`
	Members     []string              `json:"members"` // 成员信号 ID（排序）
}

// buildProfile 由一个簇的成员信号确定性地构建画像。
// 仅纳入获得画像用途同意的信号；消解结果与成员到达顺序无关。
func buildProfile(cfg Config, members []Signal, version uint64, clusterID string) Profile {
	p := Profile{
		ClusterID:   clusterID,
		Version:     version,
		Attributes:  map[string]string{},
		Resolutions: map[string]Resolution{},
		MemberCount: len(members),
	}
	// 按信号 ID 排序，保证候选枚举顺序稳定（结果本身不依赖该顺序）。
	sorted := make([]Signal, len(members))
	copy(sorted, members)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })

	byName := map[string][]candidate{}
	for _, s := range sorted {
		if !s.Consent.Allows(cfg.ProfilePurpose) {
			continue // 未获同意的信号不得进入画像
		}
		for _, a := range s.Attributes {
			byName[a.Name] = append(byName[a.Name], candidate{
				attr:     a,
				signalID: s.ID,
				channel:  s.Channel,
			})
		}
	}
	for name, cands := range byName {
		res := resolveAttribute(cfg, cands)
		p.Attributes[name] = res.Value
		p.Resolutions[name] = res
	}
	for _, s := range sorted {
		p.Members = append(p.Members, s.ID)
	}
	return p
}
