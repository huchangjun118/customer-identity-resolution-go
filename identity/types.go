// Package identity 提供客户身份归并与统一画像能力。
// 整条链路仅在本进程与本地存储内运行，不依赖外部服务。
package identity

import "time"

// Identifier 是一条跨渠道可匹配的身份标识，例如 ("email", "a@b.com")。
type Identifier struct {
	Type  string `json:"type"`  // 标识类型：email / phone / device / loyalty ...
	Value string `json:"value"` // 归一化后的标识值
}

// Key 返回标识的规范化字符串键。
func (id Identifier) Key() string { return id.Type + ":" + id.Value }

// Attribute 是信号携带的一个属性取值及其可信度元数据。
type Attribute struct {
	Name       string    `json:"name"`
	Value      string    `json:"value"`
	Confidence float64   `json:"confidence"` // 0..1，取值可信度
	ObservedAt time.Time `json:"observedAt"`
}

// Consent 表示信号被允许使用的范围集合。
type Consent map[string]bool

// Allows 判断某个用途是否被同意。
func (c Consent) Allows(purpose string) bool { return c[purpose] }

// Signal 是一条来自某渠道的身份信号。
type Signal struct {
	ID          string       `json:"id"`
	Channel     string       `json:"channel"` // 来源渠道：app / web / crm / offline ...
	Identifiers []Identifier `json:"identifiers"`
	Attributes  []Attribute  `json:"attributes"`
	Consent     Consent      `json:"consent"`
	ArrivalSeq  uint64       `json:"arrivalSeq"` // 到达序号，仅用于审计，不参与归并决策
	ReceivedAt  time.Time    `json:"receivedAt"`
}

// BatchResult 是一批信号接入后的逐项处理结果。
type BatchResult struct {
	BatchID   string          `json:"batchId"`
	Committed bool            `json:"committed"`
	Items     []SignalOutcome `json:"items"`
}

// SignalOutcome 是单条信号的处理结果，异常信号给出可区分、可定位的结果而非静默丢弃。
type SignalOutcome struct {
	SignalID  string `json:"signalId"`
	Status    string `json:"status"` // applied / rejected / duplicate / erased-blocked
	Err       string `json:"err,omitempty"`
	ClusterID string `json:"clusterId,omitempty"`
}
