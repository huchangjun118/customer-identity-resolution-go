package identity

import "time"

// AuditEntry 是一条审计记录：跨权限读取拒绝、删除、撤销等都会留痕。
type AuditEntry struct {
	At      time.Time `json:"at"`
	Actor   string    `json:"actor"`
	Action  string    `json:"action"`
	Target  string    `json:"target"`
	Allowed bool      `json:"allowed"`
	Detail  string    `json:"detail,omitempty"`
}

// auditLog 是仅追加的内存审计日志（同时写入 WAL 以便重启后保留）。
type auditLog struct {
	entries []AuditEntry
}

func newAuditLog() *auditLog { return &auditLog{} }

func (a *auditLog) record(e AuditEntry) { a.entries = append(a.entries, e) }

func (a *auditLog) list() []AuditEntry {
	out := make([]AuditEntry, len(a.entries))
	copy(out, a.entries)
	return out
}
