// Package identity 提供本进程内、本地存储的客户身份归并与统一画像能力。
package identity

import "time"

// Signal 是来自某一渠道的一条身份信号。
type Signal struct {
	ID          string            // 全局唯一信号 ID（内容由接入方保证稳定）
	CustomerKey string            // 信号归属的客户主体（同意/删除的作用单元）
	Channel     string            // 来源渠道，如 app/web/crm
	MatchKeys   map[string]string // 可归并键，如 email/phone/device
	Attributes  map[string]string // 原始属性值
	Confidence  float64           // 取值可信度 0..1
	Consent     []string          // 已获同意的用途范围
	ObservedAt  int64             // 信号观测时间（逻辑时间戳，决定新近度）
}

// SplitRule 描述拆分依据。
type SplitRule struct {
	ExcludeKeyTypes []string // 拆分时忽略的可归并键类型
	Reason          string   // 判定依据（留痕）
}

// Config 为引擎配置。
type Config struct {
	DataDir          string              // 本地存储目录（WAL）
	SourcePriority   map[string]int      // 来源渠道优先级，数值越大越优先
	DefaultPriority  int                 // 未配置渠道的默认优先级
	ProfileScope     string              // 进入画像所需的同意范围
	ReadScope        string              // 读取画像所需的权限
	Actors           map[string][]string // actor -> 允许的权限范围
	MaxSignals       int                 // 内存中信号总量上限
	MaxBatchSize     int                 // 单批信号条数上限
	MaxBatchDuration time.Duration       // 单批处理时间上限
}

// DefaultConfig 返回一份可用的默认配置。
func DefaultConfig(dataDir string) Config {
	return Config{
		DataDir:          dataDir,
		SourcePriority:   map[string]int{"crm": 30, "app": 20, "web": 10},
		DefaultPriority:  0,
		ProfileScope:     "profile",
		ReadScope:        "read:profile",
		Actors:           map[string][]string{"admin": {"read:profile", "write"}},
		MaxSignals:       1 << 20,
		MaxBatchSize:     10000,
		MaxBatchDuration: 10 * time.Second,
	}
}

// ResolvedAttr 是消解后的单个属性，带可解释信息。
type ResolvedAttr struct {
	Value      string
	Source     string
	Confidence float64
	ObservedAt  int64
	Reason     string // 消解依据说明
}

// Profile 是某一身份簇某一版本的完整画像。
// 对外暴露的画像始终是某个自洽的完整版本，不做字段级混合。
type Profile struct {
	ClusterID  string
	Version    uint64
	Attributes map[string]ResolvedAttr
	SignalIDs  []string
}

// Cluster 是一个身份簇。
type Cluster struct {
	ID        string
	SignalIDs []string
}

// RejectedSignal 是被拒绝/隔离的信号及原因（不静默丢弃）。
type RejectedSignal struct {
	Signal Signal
	Reason string
}

// BatchResult 是批量接入结果。
type BatchResult struct {
	BatchID  uint64
	Accepted int
	Rejected []RejectedSignal
}

// AuditEntry 是一条审计记录。
type AuditEntry struct {
	Seq     uint64
	At      time.Time
	Actor   string
	Action  string
	Detail  string
	Allowed bool
}
