package domain

import "time"

// OperationEvent 是驱动上报的远端文件/目录操作事件（增删改），用于事件监控触发增量同步。
type OperationEvent struct {
	// ID 是事件自增 id（115 为数值字符串），作为增量游标。
	ID string
	// Type 是操作类型：create/upload/copy/delete/move/rename。
	Type string
	// FileID 发生变更的文件/目录 id。
	FileID string
	// ParentID 变更发生时的父目录 id。
	ParentID string
	// FileName 文件名/目录名。
	FileName string
	FileSize int64
	// IsDir 事件对象是否为目录。
	IsDir bool
	// Time 事件发生时间。
	Time time.Time
}

// OperationEvent 类型常量。
const (
	OperationEventCreate = "create"
	OperationEventUpload = "upload"
	OperationEventCopy   = "copy"
	OperationEventDelete = "delete"
	OperationEventMove   = "move"
	OperationEventRename = "rename"
)
