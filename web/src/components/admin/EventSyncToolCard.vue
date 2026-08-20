<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { getApiErrorMessage } from "@/api/client";
import { accountsApi } from "@/api/accounts";
import { driversApi } from "@/api/drivers";
import { eventSyncApi, type EventSyncStatus } from "@/api/eventSync";
import type { Account, DriverInfo } from "@/api/types";
import { toast } from "@/composables/useToast";
import AppButton from "@/components/base/AppButton.vue";
import AppInput from "@/components/base/AppInput.vue";
import AppSelect from "@/components/base/AppSelect.vue";
import CloudToolCard from "@/components/admin/CloudToolCard.vue";

const props = withDefaults(defineProps<{ searchQuery?: string }>(), { searchQuery: "" });

const status = ref<EventSyncStatus>({ enabled: false, available: false, listener_account_id: 0, poll_interval_sec: 60, cooldown_min: 2, last_events: 0, trigger_count: 0 });
const accounts = ref<Account[]>([]);
const drivers = ref<DriverInfo[]>([]);
const loading = ref(false);
const saving = ref(false);

const listenerID = ref(0);
const pollInterval = ref(60);
const cooldown = ref(2);

const dirty = computed(
  () =>
    listenerID.value !== status.value.listener_account_id ||
    pollInterval.value !== status.value.poll_interval_sec ||
    cooldown.value !== status.value.cooldown_min,
);

// 监听源账号：启用中、且驱动支持事件能力（当前为 115 Cloud）。
const listenerOptions = computed(() =>
  accounts.value
    .filter((a) => a.is_active && drivers.value.some((d) => d.name === a.driver_type && d.supports_events))
    .map((a) => ({ value: a.id, label: a.name })),
);

function matches(title: string) {
  const q = props.searchQuery.trim().toLowerCase();
  return !q || title.toLowerCase().includes(q);
}

function formatTime(iso?: string): string {
  if (!iso) return "";
  const d = new Date(iso);
  return `${d.toLocaleDateString("zh-CN")} ${d.toLocaleTimeString("zh-CN")}`;
}

async function load() {
  loading.value = true;
  try {
    const [st, acc, drv] = await Promise.all([eventSyncApi.status(), accountsApi.list(), driversApi.list()]);
    status.value = st;
    accounts.value = acc;
    drivers.value = drv;
    listenerID.value = st.listener_account_id;
    pollInterval.value = st.poll_interval_sec;
    cooldown.value = st.cooldown_min;
  } catch (e) {
    toast.error(getApiErrorMessage(e, "加载 115 事件同步状态失败"));
  } finally {
    loading.value = false;
  }
}

function currentConfig(enabled: boolean) {
  return {
    enabled,
    listener_account_id: listenerID.value,
    poll_interval_sec: Math.min(600, Math.max(30, pollInterval.value || 60)),
    cooldown_min: Math.min(60, Math.max(1, cooldown.value || 2)),
  };
}

async function toggleEnabled() {
  if (!status.value.enabled && !listenerID.value) {
    toast.warning("请先选择监听账号");
    return;
  }
  const next = !status.value.enabled;
  saving.value = true;
  try {
    await eventSyncApi.saveConfig(currentConfig(next));
    status.value.enabled = next;
    if (next) {
      listenerID.value = listenerID.value || status.value.listener_account_id;
      toast.success("已启用：监听 115 操作事件并增量同步 STRM 任务");
    } else {
      toast.success("已停用 115 事件同步");
    }
  } catch (e) {
    toast.error(getApiErrorMessage(e, "修改开关失败"));
  } finally {
    saving.value = false;
  }
}

async function save() {
  saving.value = true;
  try {
    await eventSyncApi.saveConfig(currentConfig(status.value.enabled));
    status.value.listener_account_id = listenerID.value;
    status.value.poll_interval_sec = pollInterval.value;
    status.value.cooldown_min = cooldown.value;
    toast.success("115 事件同步配置已保存");
  } catch (e) {
    toast.error(getApiErrorMessage(e, "保存配置失败"));
  } finally {
    saving.value = false;
  }
}

onMounted(() => {
  void load();
});
</script>

<template>
  <div v-show="matches('115 事件同步')">
    <CloudToolCard
      :enabled="status.enabled"
      name="115 事件同步"
      driver="监听 115 操作事件 · 自动增量同步 STRM 任务"
      logo-src="/logos/115.png"
      logo-alt="115"
      :tags="[{ label: '115Cloud' }, { label: '实验性', variant: 'warn' }]"
      :stat-value="status.trigger_count"
      stat-label="累计触发次数"
    >
      <template #toggle>
        <button
          class="check-toggle"
          type="button"
          :class="{ on: status.enabled }"
          :aria-label="status.enabled ? '停用 115 事件同步' : '启用 115 事件同步'"
          :disabled="saving || loading || !listenerOptions.length"
          title="启用 / 停用"
          @click="toggleEnabled"
        >
          <svg viewBox="0 0 16 16" aria-hidden="true">
            <path
              d="M3.5 8.5 6.5 11.5 12.5 4.5"
              fill="none"
              stroke="currentColor"
              stroke-width="2"
              stroke-linecap="round"
              stroke-linejoin="round"
            />
          </svg>
        </button>
      </template>
      <p class="es-hint">
        监听 <strong>115 Cloud</strong> 账号的 115 生活操作事件，云端文件增删改后自动匹配
        <strong>115 Open</strong> STRM 任务的扫描目录并触发增量重扫（自动开启该账号事件收集）。
        监听账号与 STRM 任务账号须指向<strong>同一 115 空间</strong>，目录才能匹配。
      </p>

      <div v-if="loading" class="es-loading">加载中…</div>
      <template v-else>
        <div v-if="!listenerOptions.length" class="es-empty">
          没有支持操作事件的启用账号。请先添加 115 Cloud 驱动账号（Cookie），并确认其为启用状态。
        </div>
        <div v-else class="es-config">
          <div class="es-row">
            <label class="es-label">监听账号</label>
            <AppSelect
              class="es-select"
              :model-value="listenerID || null"
              :options="listenerOptions"
              placeholder="选择 115 Cloud 账号"
              @update:model-value="listenerID = Number($event)"
            />
          </div>
          <div class="es-row es-row--two">
            <label class="es-label">轮询间隔</label>
            <div class="es-field">
              <AppInput
                v-model="pollInterval"
                type="number"
                min="30"
                max="600"
                :aria-label="'轮询间隔'"
              />
              <span class="es-unit">秒</span>
            </div>
            <label class="es-label">触发冷却</label>
            <div class="es-field">
              <AppInput
                v-model="cooldown"
                type="number"
                min="1"
                max="60"
                :aria-label="'触发冷却'"
              />
              <span class="es-unit">分钟</span>
            </div>
          </div>
          <div class="es-status">
            <span v-if="status.listener_account_name" class="es-status__item">
              监听：{{ status.listener_account_name }}
            </span>
            <span v-if="status.last_poll_at" class="es-status__item">
              上次轮询 {{ formatTime(status.last_poll_at) }}
            </span>
            <span v-if="status.cursor" class="es-status__item">游标 {{ status.cursor }}</span>
            <span v-if="status.last_events > 0" class="es-status__item">事件 {{ status.last_events }}</span>
            <span v-if="status.last_triggered?.length" class="es-status__item">
              最近触发：{{ status.last_triggered.join("、") }}
            </span>
          </div>
        </div>
      </template>
      <template #actions>
        <AppButton variant="primary" :disabled="!dirty || saving" @click="save">
          {{ saving ? "保存中…" : dirty ? "保存配置" : "已保存" }}
        </AppButton>
      </template>
    </CloudToolCard>
  </div>
</template>

<style scoped>
.check-toggle {
  width: 28px;
  height: 28px;
  border-radius: 50%;
  border: 0;
  padding: 0;
  flex-shrink: 0;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  cursor: pointer;
  background: var(--border);
  color: var(--text-muted);
  transition: background 0.18s ease, color 0.18s ease, box-shadow 0.18s ease;
}
.check-toggle svg {
  width: 14px;
  height: 14px;
}
.check-toggle:hover {
  background: var(--surface-hover);
}
.check-toggle.on {
  background: var(--success);
  color: #fff;
  box-shadow: 0 0 0 4px rgba(16, 185, 129, 0.16);
}
.check-toggle.on:hover {
  background: color-mix(in srgb, var(--success) 88%, #000);
}
.check-toggle:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}
.es-hint {
  margin-bottom: 14px;
  font-size: 12px;
  line-height: 1.6;
  color: var(--text-muted);
}
.es-hint strong {
  color: var(--text);
}
.es-loading,
.es-empty {
  padding: 14px 0;
  font-size: 13px;
  color: var(--text-muted);
}
.es-config {
  display: flex;
  flex-direction: column;
  gap: 10px;
}
.es-row {
  display: flex;
  align-items: center;
  gap: 10px;
}
.es-row--two {
  flex-wrap: wrap;
}
.es-label {
  flex-shrink: 0;
  font-size: 12px;
  color: var(--text-muted);
  width: 68px;
}
.es-select {
  flex: 1;
  min-width: 200px;
}
.es-field {
  display: inline-flex;
  align-items: center;
  gap: 6px;
}
.es-field .app-input {
  width: 90px;
}
.es-unit {
  font-size: 12px;
  color: var(--text-muted);
}
.es-status {
  display: flex;
  flex-wrap: wrap;
  gap: 6px 14px;
  margin-top: 4px;
  padding-top: 10px;
  border-top: 1px solid var(--border-soft);
}
.es-status__item {
  font-size: 11px;
  color: var(--text-muted);
}
</style>
