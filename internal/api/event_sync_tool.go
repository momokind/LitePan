package api

import (
	"net/http"

	"litepan/internal/domain"
	"litepan/internal/eventmonitor"
)

// getEventSyncStatus 返回「115 事件同步」工具卡片状态。
func (h *Handler) getEventSyncStatus(w http.ResponseWriter, r *http.Request) {
	if h.eventMonitor == nil {
		writeOK(w, map[string]any{"enabled": false, "available": false})
		return
	}
	status, err := h.eventMonitor.Status(r.Context())
	if err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, status)
}

// putEventSyncConfig 保存「115 事件同步」工具配置（监听账号/轮询间隔/冷却/开关）。
func (h *Handler) putEventSyncConfig(w http.ResponseWriter, r *http.Request) {
	if h.eventMonitor == nil {
		writeErr(w, domain.Errf(domain.CodeNotImplement))
		return
	}
	var in eventmonitor.Config
	if err := decodeJSON(r, &in); err != nil {
		writeErr(w, err)
		return
	}
	if err := h.eventMonitor.SetConfig(r.Context(), in); err != nil {
		writeErr(w, err)
		return
	}
	writeOK(w, map[string]any{"ok": true})
}
