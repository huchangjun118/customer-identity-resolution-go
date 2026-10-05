package identity

// EventKind 是事件类型。
type EventKind string

const (
	EvSignal   EventKind = "signal"        // 信号接入
	EvMerge    EventKind = "merge"         // 人工合并两个簇
	EvUndoMerge EventKind = "undo_merge"   // 撤销一次合并
	EvSplit    EventKind = "split"         // 拆分一个簇
	EvWithdraw EventKind = "withdraw"      // 撤回同意
	EvDelete   EventKind = "delete"        // 删除客户主体
	EvCommit   EventKind = "commit"        // 批次提交标记
)

// Event 是追加到本地 WAL 的一条事件。所有状态由事件流确定性重放得到。
type Event struct {
	Seq         uint64     `json:"seq"`
	BatchID     uint64     `json:"batch"`
	Kind        EventKind  `json:"kind"`
	Signal      *Signal    `json:"signal,omitempty"`
	MergeID     string     `json:"merge_id,omitempty"`
	ClusterA    string     `json:"cluster_a,omitempty"`
	ClusterB    string     `json:"cluster_b,omitempty"`
	UndoMergeID string     `json:"undo_merge_id,omitempty"`
	SplitTarget string     `json:"split_target,omitempty"`
	Rule        *SplitRule `json:"rule,omitempty"`
	CustomerKey string     `json:"customer_key,omitempty"`
	Scopes      []string   `json:"scopes,omitempty"`
}
