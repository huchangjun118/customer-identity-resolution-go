package identity

import "errors"

// 可区分的错误类别，调用方可据此定位问题。
var (
	ErrSignalInvalid     = errors.New("identity: signal invalid")
	ErrBatchTooLarge     = errors.New("identity: batch exceeds max signals")
	ErrBatchTimeout      = errors.New("identity: batch exceeds max duration")
	ErrMemoryLimit       = errors.New("identity: in-memory signal limit exceeded")
	ErrPermissionDenied  = errors.New("identity: permission denied")
	ErrConsentMissing    = errors.New("identity: consent missing for purpose")
	ErrClusterNotFound   = errors.New("identity: cluster not found")
	ErrMergeNotFound     = errors.New("identity: merge record not found")
	ErrSplitRuleNotFound = errors.New("identity: split rule not found")
	ErrErasedSubject     = errors.New("identity: subject erased, signal blocked")
	ErrEngineClosed      = errors.New("identity: engine closed")
)
