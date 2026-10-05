package identity

import "time"

// SourceRule 配置某渠道在冲突消解中的来源优先级（越大越优先）。
type SourceRule struct {
	Channel  string `json:"channel"`
	Priority int    `json:"priority"`
}

// SplitRule 配置身份簇拆分依据。
// Kind 支持 "by-channel"（按渠道分组拆分）。
type SplitRule struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
}

// Budgets 是内存与单批处理时间的可配置上限。
type Budgets struct {
	MaxSignalsPerBatch int           `json:"maxSignalsPerBatch"`
	MaxBatchDuration   time.Duration `json:"maxBatchDuration"`
	MaxSignalsInMemory int           `json:"maxSignalsInMemory"`
}

// Config 是引擎配置。
type Config struct {
	DataDir        string       `json:"dataDir"`
	SourceRules    []SourceRule `json:"sourceRules"`
	SplitRules     []SplitRule  `json:"splitRules"`
	Budgets        Budgets      `json:"budgets"`
	ProfilePurpose string       `json:"profilePurpose"` // 进入画像所需的同意用途
}

// DefaultConfig 返回可用于本地样本运行的默认配置。
func DefaultConfig(dataDir string) Config {
	return Config{
		DataDir: dataDir,
		SourceRules: []SourceRule{
			{Channel: "crm", Priority: 100},
			{Channel: "app", Priority: 80},
			{Channel: "web", Priority: 50},
			{Channel: "offline", Priority: 10},
		},
		SplitRules: []SplitRule{{ID: "by-channel", Kind: "by-channel"}},
		Budgets: Budgets{
			MaxSignalsPerBatch: 1000,
			MaxBatchDuration:   5 * time.Second,
			MaxSignalsInMemory: 100000,
		},
		ProfilePurpose: "profile",
	}
}

// priorityOf 返回渠道优先级，未配置时为 0。
func (c Config) priorityOf(channel string) int {
	for _, r := range c.SourceRules {
		if r.Channel == channel {
			return r.Priority
		}
	}
	return 0
}

// splitRule 按 ID 查找拆分规则。
func (c Config) splitRule(id string) (SplitRule, bool) {
	for _, r := range c.SplitRules {
		if r.ID == id {
			return r, true
		}
	}
	return SplitRule{}, false
}
