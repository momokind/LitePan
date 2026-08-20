package domain

import (
	"context"
	"time"
)

// EventMonitorCursor 记录每个账号的事件监控游标（最新已处理事件 id）。
type EventMonitorCursor struct {
	AccountID   int64
	LastEventID string
	UpdatedAt   time.Time
}

// EventMonitorCursorRepository 定义事件监控游标的持久化端口。
type EventMonitorCursorRepository interface {
	Get(ctx context.Context, accountID int64) (*EventMonitorCursor, error)
	Upsert(ctx context.Context, c *EventMonitorCursor) error
	Delete(ctx context.Context, accountID int64) error
}
