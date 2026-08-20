import { http } from "./client";

// 115 事件同步工具运行状态。
export interface EventSyncStatus {
  enabled: boolean;
  available: boolean;
  listener_account_id: number;
  listener_account_name?: string;
  poll_interval_sec: number;
  cooldown_min: number;
  cursor?: string;
  last_poll_at?: string;
  last_events: number;
  trigger_count: number;
  last_triggered?: string[];
}

// 115 事件同步工具可写配置。
export interface EventSyncConfig {
  enabled: boolean;
  listener_account_id: number;
  poll_interval_sec: number;
  cooldown_min: number;
}

export const eventSyncApi = {
  status: () => http.get<EventSyncStatus>("/admin/tools/115-event-sync/status"),
  saveConfig: (config: EventSyncConfig) => http.put<{ ok: boolean }>("/admin/tools/115-event-sync/config", config),
};
