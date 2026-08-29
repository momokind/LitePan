package eventmonitor

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"litepan/internal/cache"
	"litepan/internal/domain"
	"litepan/internal/driver"
	"litepan/internal/file"
	"litepan/internal/settings"
	"litepan/internal/strm"
)

// 启动退避与轮询配置。
const (
	startupDelay    = 30 * time.Second
	defaultPollSec  = 60
	defaultCooldown = 2 * time.Minute
	backoffBase     = time.Minute // 瞬态错误退避基数，按连续失败次数翻倍
	backoffMax      = 30 * time.Minute
	// eventPollExtraMS 与 115 驱动默认 800ms 闸门叠加，强制事件接口两次调用 ≥5s。
	eventPollExtraMS = 4200
	// targetDriverType 是事件可作用于的 STRM 任务所属驱动类型：115 Open。
	// 监听账号（115 Cloud）与目标 STRM 账号必须指向同一 115 空间，目录 ID/路径才互通。
	targetDriverType = "115_open"
)

// Options 是事件同步服务的依赖。
type Options struct {
	Accounts  domain.AccountRepository
	Cursors   domain.EventMonitorCursorRepository
	StrmTasks domain.StrmTaskRepository
	Drivers   driver.Provider
	Files     *file.Service
	Strm      *strm.Service
	Cache     *cache.Service
	Settings  *settings.Service
	Log       *slog.Logger
}

// Service 监听 115 Cloud 账号的操作事件，把远端增删改目录匹配到 115 Open STRM 任务并触发增量同步。
type Service struct {
	accounts  domain.AccountRepository
	cursors   domain.EventMonitorCursorRepository
	strmTasks domain.StrmTaskRepository
	drivers   driver.Provider
	files     *file.Service
	strm      *strm.Service
	cache     *cache.Service
	settings  *settings.Service
	log       *slog.Logger

	mu             sync.Mutex
	lastTrigger    map[int64]time.Time // 任务级冷却：最近触发时间
	backoff        map[int64]backoffState
	pending        map[int64]*pendingTrigger
	triggerCount   int64
	lastPollAt     time.Time
	lastEvents     int
	lastTriggered  []string
	recalc         chan struct{}
	appCtx         context.Context
	started        bool
	startupReadyAt time.Time

	pathCache *pathCache
}

// pendingTrigger 是冷却期内被合并跳过的待补触发记录：冷却到期后由轮询循环补触发一次，
// 保证冷却期间到达的事件（游标已前移、不会重放）不丢失。
type pendingTrigger struct {
	accountID         int64
	readyAt           time.Time
	dirIDs            map[string]struct{}
	invalidateAccount bool
}

// backoffState 是监听账号级的瞬态错误退避：连续失败按指数递增，成功后清零。
type backoffState struct {
	until    time.Time
	failures int
}

// NewService 构造事件同步服务。
func NewService(opts Options) *Service {
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}
	return &Service{
		accounts:    opts.Accounts,
		cursors:     opts.Cursors,
		strmTasks:   opts.StrmTasks,
		drivers:     opts.Drivers,
		files:       opts.Files,
		strm:        opts.Strm,
		cache:       opts.Cache,
		settings:    opts.Settings,
		log:         log,
		lastTrigger: make(map[int64]time.Time),
		backoff:     make(map[int64]backoffState),
		pending:     make(map[int64]*pendingTrigger),
		recalc:      make(chan struct{}, 1),
		pathCache:   newPathCache(time.Minute),
	}
}

// Start 启动轮询循环；启用开关关闭时循环空转，开启后立即生效。
func (s *Service) Start(ctx context.Context) {
	if s == nil || s.accounts == nil || s.cursors == nil || s.drivers == nil {
		return
	}
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return
	}
	s.started = true
	s.appCtx = ctx
	s.startupReadyAt = time.Now().Add(startupDelay)
	s.mu.Unlock()
	s.log.Info("115 事件同步已启动")
	go s.loop(ctx)
}

func (s *Service) loop(ctx context.Context) {
	s.waitStartup(ctx)
	ticker := time.NewTicker(s.pollInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.recalc:
			s.drainRecalc()
			ticker.Reset(s.pollInterval())
		case <-ticker.C:
			if !s.enabled() {
				continue
			}
			s.pollOnce(ctx)
			s.processPendingTriggers(ctx)
		}
	}
}

// TriggerRecalculation 请求立即按最新配置重算（非阻塞）。
func (s *Service) TriggerRecalculation() {
	select {
	case s.recalc <- struct{}{}:
	default:
	}
}

// pollOnce 读取当前监听账号并轮询一次事件。
func (s *Service) pollOnce(ctx context.Context) {
	accountID := s.listenerAccountID()
	if accountID <= 0 {
		s.log.Debug("事件同步已启用但未配置监听账号，跳过轮询")
		return
	}
	if !s.listenerSupportsEvents(ctx, accountID) {
		s.log.Debug("事件同步监听账号停用或驱动不支持事件，跳过轮询", "account_id", accountID)
		return
	}
	s.pollAccount(ctx, accountID)
}

func (s *Service) pollAccount(ctx context.Context, accountID int64) {
	if s.inBackoff(accountID) {
		return
	}
	drv, err := s.drivers.Get(ctx, accountID)
	if err != nil {
		s.log.Warn("事件同步获取驱动失败", "account_id", accountID, "err", err)
		return
	}
	prov, ok := drv.(driver.OperationEventProvider)
	if !ok {
		return
	}

	fromID := ""
	if c, err := s.cursors.Get(ctx, accountID); err == nil && c != nil {
		fromID = c.LastEventID
	}

	pollCtx := driver.WithExtraAPIDelay(ctx, eventPollExtraMS)
	events, next, err := prov.RecentOperations(pollCtx, fromID, 1000)
	if err != nil {
		s.logPollError(accountID, err)
		if isTransientError(err) {
			s.setBackoff(accountID)
		}
		return
	}
	s.clearBackoff(accountID)

	if fromID == "" {
		// 基线初始化：只建立游标，不回放历史事件。
		if next != "" {
			s.upsertCursor(ctx, accountID, next)
			s.log.Debug("事件同步基线初始化完成，历史事件不回放", "account_id", accountID, "cursor", next)
		}
		return
	}
	if len(events) == 0 {
		return
	}
	s.recordPoll(events)
	s.log.Debug("事件同步收到新事件", "account_id", accountID, "events", len(events), "cursor", next)
	for _, e := range events {
		s.log.Debug("事件同步事件明细",
			"type", e.Type, "file", e.FileName, "file_id", e.FileID,
			"parent_id", e.ParentID, "is_dir", e.IsDir)
	}
	s.matchAndTrigger(ctx, events)
	// 游标在事件处理完成后保存：进程崩溃时宁可下轮重复触发幂等重扫，也不丢事件。
	if next != "" && next != fromID {
		s.upsertCursor(ctx, accountID, next)
	}
}

// matchAndTrigger 把事件目录解析为路径，匹配 115 Open STRM 任务并触发同步。
func (s *Service) matchAndTrigger(ctx context.Context, events []domain.OperationEvent) {
	if s.strm == nil || s.files == nil || s.strmTasks == nil {
		s.log.Debug("事件同步依赖服务未就绪，跳过事件匹配")
		return
	}
	parentIDs := uniqueParentIDs(events)
	// 115 的删除事件 parent_id=0（不带原目录），无法按作用域定位；
	// 目录改名后任务作用域仍按旧路径配置，事件只能解析出新路径，同样无法对齐。
	// 两类事件改为触发全部候选任务做对账重扫，由扫描对齐远端实际清单。
	scopeUnknown := hasDeleteEvent(events) || hasDirRenameEvent(events)
	if len(parentIDs) == 0 && !scopeUnknown {
		s.log.Debug("事件中无可用的父目录 ID（根目录操作或字段缺失），跳过匹配", "events", len(events))
		return
	}

	tasks, err := s.strmTasks.List(ctx)
	if err != nil {
		s.log.Warn("事件同步 STRM 任务列表获取失败", "err", err)
		return
	}
	accounts, err := s.accounts.List(ctx)
	if err != nil {
		s.log.Warn("事件同步账号列表获取失败", "err", err)
		return
	}
	accByID := make(map[int64]*domain.Account, len(accounts))
	for _, a := range accounts {
		if a != nil {
			accByID[a.ID] = a
		}
	}

	// 候选任务：非暂停、所属账号启用且为 115 Open 驱动。
	type group struct {
		tasks []*domain.StrmTask
	}
	var skippedPaused, skippedInactive, skippedDriver int
	groups := make(map[int64]*group)
	for _, t := range tasks {
		if t == nil || t.Status == domain.StrmStatusPaused {
			if t != nil {
				skippedPaused++
			}
			continue
		}
		acc := accByID[t.AccountID]
		if acc == nil || !acc.IsActive {
			skippedInactive++
			continue
		}
		if acc.DriverType != targetDriverType {
			skippedDriver++
			continue
		}
		g := groups[t.AccountID]
		if g == nil {
			g = &group{}
			groups[t.AccountID] = g
		}
		g.tasks = append(g.tasks, t)
	}
	if len(groups) == 0 {
		s.log.Debug("事件同步无候选 STRM 任务",
			"tasks", len(tasks),
			"paused", skippedPaused,
			"account_inactive", skippedInactive,
			"not_115_open", skippedDriver,
			"hint", "事件同步只触发 115 Open 账号下的任务")
		return
	}

	cooldown := s.triggerCooldown()
	now := time.Now()
	var triggeredNames []string
	for accountID, g := range groups {
		resolved := s.resolvePaths(ctx, accountID, parentIDs)
		if len(resolved) == 0 && !scopeUnknown {
			s.log.Debug("事件目录路径反查全部失败（监听账号与该 STRM 账号可能不是同一 115 空间）",
				"account_id", accountID, "dir_ids", summarizeIDs(parentIDs))
			continue
		}
		for _, t := range g.tasks {
			var matched []string
			var unmatchedPaths []string
			for parentID, path := range resolved {
				if matchTaskScope(path, t) {
					matched = append(matched, parentID)
				} else {
					unmatchedPaths = append(unmatchedPaths, path)
				}
			}
			if len(matched) == 0 && !scopeUnknown {
				s.log.Debug("事件目录未命中任务扫描作用域",
					"task_id", t.ID, "task", t.Name,
					"task_path", t.Path, "recursive", t.Recursive,
					"event_dirs", summarizeIDs(unmatchedPaths))
				continue
			}
			if ok, readyAt := s.acquireTrigger(t.ID, cooldown, now); !ok {
				s.deferTrigger(t.ID, accountID, readyAt, matched, scopeUnknown)
				s.log.Debug("事件同步触发冷却中，已登记冷却结束后补触发",
					"task_id", t.ID, "task", t.Name, "ready_at", readyAt)
				continue
			}
			if s.triggerTask(ctx, t, matched, scopeUnknown) {
				s.log.Info("事件同步触发 STRM 任务",
					"task_id", t.ID, "task", t.Name, "dirs", len(matched), "scope_unknown", scopeUnknown)
				triggeredNames = append(triggeredNames, t.Name)
			}
		}
	}
	if len(triggeredNames) > 0 {
		sort.Strings(triggeredNames)
		s.mu.Lock()
		s.lastTriggered = triggeredNames
		s.mu.Unlock()
	}
}

// acquireTrigger 按任务级冷却决定是否允许触发；允许时记录触发时间。
// 拒绝时一并返回冷却结束时间，供登记补触发。
func (s *Service) acquireTrigger(taskID int64, cooldown time.Duration, now time.Time) (bool, time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if last, ok := s.lastTrigger[taskID]; ok {
		if ready := last.Add(cooldown); now.Before(ready) {
			return false, ready
		}
	}
	s.lastTrigger[taskID] = now
	s.triggerCount++
	return true, now
}

// deferTrigger 登记冷却结束后的补触发；同任务多个事件合并为一次补触发：
// 目录失效集合取并集、到期时间取最早者。
func (s *Service) deferTrigger(taskID, accountID int64, readyAt time.Time, dirIDs []string, invalidateAccount bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.pending[taskID]
	if p == nil {
		p = &pendingTrigger{dirIDs: make(map[string]struct{})}
		s.pending[taskID] = p
	}
	p.accountID = accountID
	if p.readyAt.IsZero() || readyAt.Before(p.readyAt) {
		p.readyAt = readyAt
	}
	if invalidateAccount {
		p.invalidateAccount = true
	}
	for _, id := range dirIDs {
		p.dirIDs[id] = struct{}{}
	}
}

// processPendingTriggers 补触发冷却期内被合并的任务；由轮询循环每个 tick 调用。
func (s *Service) processPendingTriggers(ctx context.Context) {
	now := time.Now()
	type dueEntry struct {
		id int64
		p  *pendingTrigger
	}
	var due []dueEntry
	s.mu.Lock()
	for id, p := range s.pending {
		if now.Before(p.readyAt) {
			continue
		}
		due = append(due, dueEntry{id: id, p: p})
		delete(s.pending, id)
	}
	s.mu.Unlock()
	for _, d := range due {
		s.firePendingTrigger(ctx, d.id, d.p)
	}
}

// firePendingTrigger 执行一次补触发；任务已删除/暂停或账号不可用时直接丢弃。
func (s *Service) firePendingTrigger(ctx context.Context, taskID int64, p *pendingTrigger) {
	t, err := s.strmTasks.Get(ctx, taskID)
	if err != nil || t == nil || t.Status == domain.StrmStatusPaused {
		return
	}
	acc, err := s.accounts.Get(ctx, p.accountID)
	if err != nil || acc == nil || !acc.IsActive || acc.DriverType != targetDriverType {
		return
	}
	if ok, _ := s.acquireTrigger(taskID, s.triggerCooldown(), time.Now()); !ok {
		// 冷却未到说明期间已被新事件批次触发过，那次扫描已覆盖本次补触发，直接丢弃。
		s.log.Debug("事件同步补触发冷却未到，跳过", "task_id", taskID)
		return
	}
	dirIDs := make([]string, 0, len(p.dirIDs))
	for id := range p.dirIDs {
		dirIDs = append(dirIDs, id)
	}
	if s.triggerTask(ctx, t, dirIDs, p.invalidateAccount) {
		s.log.Info("事件同步冷却结束后补触发 STRM 任务", "task_id", taskID, "task", t.Name, "dirs", len(dirIDs))
	}
}

// triggerTask 失效相关目录缓存并触发任务立即扫描；返回是否成功触发。
func (s *Service) triggerTask(ctx context.Context, t *domain.StrmTask, dirIDs []string, invalidateAccount bool) bool {
	if invalidateAccount {
		// 删除无法定位目录：失效该账号全部目录缓存，让对账重扫读到最新清单。
		cache.InvalidateAccountDirs(s.cache, t.AccountID)
	}
	// 失效该账号受影响目录缓存，确保重扫读取最新变更。
	for _, parentID := range dirIDs {
		cache.InvalidateDirKeys(s.cache, t.AccountID, parentID)
	}
	if _, err := s.strm.RunTaskNow(ctx, t.ID, domain.StrmRunModeAuto); err != nil {
		// 账号互斥/启动退避期间 RunTaskNow 返回 Validation 错误但已入延迟执行队列，属正常排队。
		if ae, ok := domain.AsAppError(err); ok && ae.Code == domain.CodeValidation {
			s.log.Info("事件同步触发已进入执行队列，稍后自动执行",
				"task_id", t.ID, "task", t.Name, "msg", ae.Message)
		} else {
			s.log.Warn("事件同步触发 STRM 任务失败", "task_id", t.ID, "task", t.Name, "err", err)
		}
		return false
	}
	return true
}

// resolvePaths 解析事件父目录的完整远端路径（带 TTL 缓存）；失败的目录跳过。
func (s *Service) resolvePaths(ctx context.Context, accountID int64, parentIDs []string) map[string]string {
	out := make(map[string]string, len(parentIDs))
	for _, pid := range parentIDs {
		path, ok := s.pathCache.get(accountID, pid)
		if ok {
			if path != "" {
				out[pid] = path
			}
			continue
		}
		p, err := s.files.ResolveDirPath(ctx, accountID, pid)
		if err != nil {
			s.log.Debug("事件目录路径反查失败", "account_id", accountID, "dir_id", pid, "err", err)
			s.pathCache.set(accountID, pid, "", false)
			continue
		}
		s.pathCache.set(accountID, pid, p, true)
		if p != "" {
			out[pid] = p
		}
	}
	return out
}

// matchTaskScope 判断事件目录路径是否命中任务扫描作用域。
// 事件解析出的路径（buildDirPath）不带前导斜杠，而任务 Path 带前导斜杠，
// 统一用 strings.Trim 去掉首尾斜杠后再比较，避免因斜杠差异导致永不命中。
func matchTaskScope(path string, t *domain.StrmTask) bool {
	path = strings.Trim(path, "/")
	base := strings.Trim(strings.TrimSpace(t.Path), "/")
	if base == "" {
		return true // 任务根为整个空间。
	}
	if path == base {
		return true
	}
	if !t.Recursive {
		return false
	}
	return strings.HasPrefix(path, base+"/")
}

// summarizeIDs 汇总目录 ID/路径列表用于日志输出，最多展示 10 项，超出部分以计数提示。
func summarizeIDs(items []string) string {
	const max = 10
	if len(items) <= max {
		return strings.Join(items, ",")
	}
	return strings.Join(items[:max], ",") + fmt.Sprintf("…等%d项", len(items))
}

// hasDeleteEvent 报告事件批次中是否含删除。
// 115 的删除事件 parent_id=0（不带原目录），无法按作用域定位，需全任务对账清理。
func hasDeleteEvent(events []domain.OperationEvent) bool {
	for _, e := range events {
		if e.Type == domain.OperationEventDelete {
			return true
		}
	}
	return false
}

// hasDirRenameEvent 报告事件批次中是否含目录改名。
// 任务作用域按旧路径配置，目录改名后事件只能解析出新路径，需全任务对账。
func hasDirRenameEvent(events []domain.OperationEvent) bool {
	for _, e := range events {
		if e.Type == domain.OperationEventRename && e.IsDir {
			return true
		}
	}
	return false
}

// uniqueParentIDs 收集事件中出现的唯一非空父目录 id。
func uniqueParentIDs(events []domain.OperationEvent) []string {
	seen := make(map[string]struct{}, len(events))
	var out []string
	for _, e := range events {
		pid := strings.TrimSpace(e.ParentID)
		if pid == "" || pid == "0" {
			continue
		}
		if _, dup := seen[pid]; dup {
			continue
		}
		seen[pid] = struct{}{}
		out = append(out, pid)
	}
	return out
}

// Status 是工具卡片展示的运行状态。
type Status struct {
	Enabled             bool      `json:"enabled"`
	Available           bool      `json:"available"`
	ListenerAccountID   int64     `json:"listener_account_id"`
	ListenerAccountName string    `json:"listener_account_name"`
	PollIntervalSec     int       `json:"poll_interval_sec"`
	CooldownMin         int       `json:"cooldown_min"`
	Cursor              string    `json:"cursor"`
	LastPollAt          time.Time `json:"last_poll_at"`
	LastEvents          int       `json:"last_events"`
	TriggerCount        int64     `json:"trigger_count"`
	LastTriggered       []string  `json:"last_triggered"`
}

// Status 返回当前运行状态。
func (s *Service) Status(ctx context.Context) (Status, error) {
	st := Status{
		Enabled:         s.enabled(),
		PollIntervalSec: s.pollIntervalSec(),
		CooldownMin:     s.cooldownMin(),
	}
	accountID := s.listenerAccountID()
	st.ListenerAccountID = accountID
	if accountID > 0 {
		if acc, err := s.accounts.Get(ctx, accountID); err == nil && acc != nil {
			st.ListenerAccountName = acc.Name
			st.Available = acc.IsActive && s.supportsEvents(acc.DriverType)
		}
		if c, err := s.cursors.Get(ctx, accountID); err == nil && c != nil {
			st.Cursor = c.LastEventID
		}
	}
	s.mu.Lock()
	st.LastPollAt = s.lastPollAt
	st.LastEvents = s.lastEvents
	st.TriggerCount = s.triggerCount
	st.LastTriggered = append([]string(nil), s.lastTriggered...)
	s.mu.Unlock()
	return st, nil
}

// Config 是工具卡片可写配置。
type Config struct {
	Enabled           bool  `json:"enabled"`
	ListenerAccountID int64 `json:"listener_account_id"`
	PollIntervalSec   int   `json:"poll_interval_sec"`
	CooldownMin       int   `json:"cooldown_min"`
}

// SetConfig 校验并保存配置；保存成功后请求立即重算。
func (s *Service) SetConfig(ctx context.Context, c Config) error {
	if c.ListenerAccountID <= 0 {
		return domain.Errorf(domain.CodeValidation, "请选择监听账号")
	}
	acc, err := s.accounts.Get(ctx, c.ListenerAccountID)
	if err != nil {
		return domain.Errorf(domain.CodeValidation, "监听账号不存在：%v", err)
	}
	if acc == nil || !acc.IsActive {
		return domain.Errorf(domain.CodeValidation, "监听账号「%s」已停用", acc.Name)
	}
	if !s.supportsEvents(acc.DriverType) {
		return domain.Errorf(domain.CodeValidation, "监听账号「%s」的驱动（%s）不支持操作事件", acc.Name, acc.DriverType)
	}
	if c.PollIntervalSec < 30 || c.PollIntervalSec > 600 {
		c.PollIntervalSec = defaultPollSec
	}
	if c.CooldownMin < 1 || c.CooldownMin > 60 {
		c.CooldownMin = 2
	}
	if s.settings == nil {
		return domain.Errorf(domain.CodeInternal, "设置服务未就绪")
	}
	if err := s.settings.Update(ctx, map[string]string{
		settings.KeyEventSyncEnabled:           strconv.FormatBool(c.Enabled),
		settings.KeyEventSyncListenerAccountID: strconv.FormatInt(c.ListenerAccountID, 10),
		settings.KeyEventSyncPollIntervalSec:   strconv.Itoa(c.PollIntervalSec),
		settings.KeyEventSyncCooldownMin:       strconv.Itoa(c.CooldownMin),
	}); err != nil {
		return err
	}
	s.TriggerRecalculation()
	return nil
}

// CleanupAccount 在账号被删除时清理游标与内部状态。
func (s *Service) CleanupAccount(ctx context.Context, accountID int64) {
	s.mu.Lock()
	delete(s.lastTrigger, accountID)
	delete(s.backoff, accountID)
	for id, p := range s.pending {
		if p.accountID == accountID {
			delete(s.pending, id)
		}
	}
	s.mu.Unlock()
	s.pathCache.clearAccount(accountID)
	if s.cursors != nil {
		_ = s.cursors.Delete(ctx, accountID)
	}
	if s.listenerAccountID() == accountID {
		s.TriggerRecalculation()
	}
}

func (s *Service) recordPoll(events []domain.OperationEvent) {
	s.mu.Lock()
	s.lastPollAt = time.Now()
	s.lastEvents = len(events)
	s.mu.Unlock()
}

func (s *Service) upsertCursor(ctx context.Context, accountID int64, lastEventID string) {
	if s.cursors == nil || lastEventID == "" {
		return
	}
	if err := s.cursors.Upsert(ctx, &domain.EventMonitorCursor{AccountID: accountID, LastEventID: lastEventID}); err != nil {
		s.log.Warn("事件同步游标保存失败", "account_id", accountID, "err", err)
	}
}

func (s *Service) listenerAccountID() int64 {
	if s.settings == nil {
		return 0
	}
	v, _ := strconv.ParseInt(s.settings.String(settings.KeyEventSyncListenerAccountID), 10, 64)
	return v
}

func (s *Service) listenerSupportsEvents(ctx context.Context, accountID int64) bool {
	acc, err := s.accounts.Get(ctx, accountID)
	if err != nil || acc == nil || !acc.IsActive {
		return false
	}
	return s.supportsEvents(acc.DriverType)
}

func (s *Service) supportsEvents(driverType string) bool {
	drv, ok := driver.New(driverType)
	if !ok {
		return false
	}
	_, ok = drv.(driver.OperationEventProvider)
	return ok
}

func (s *Service) enabled() bool {
	return s.settings != nil && s.settings.Bool(settings.KeyEventSyncEnabled)
}

func (s *Service) pollIntervalSec() int {
	sec := defaultPollSec
	if s.settings != nil {
		sec = s.settings.Int(settings.KeyEventSyncPollIntervalSec)
	}
	if sec < 30 {
		sec = 30
	}
	return sec
}

func (s *Service) pollInterval() time.Duration {
	return time.Duration(s.pollIntervalSec()) * time.Second
}

func (s *Service) cooldownMin() int {
	m := 2
	if s.settings != nil {
		if v := s.settings.Int(settings.KeyEventSyncCooldownMin); v >= 1 {
			m = v
		}
	}
	return m
}

func (s *Service) triggerCooldown() time.Duration {
	return time.Duration(s.cooldownMin()) * time.Minute
}

func (s *Service) inBackoff(accountID int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return time.Now().Before(s.backoff[accountID].until)
}

// setBackoff 按连续失败次数指数递增退避：1min、2min、4min…封顶 30min。
func (s *Service) setBackoff(accountID int64) {
	s.mu.Lock()
	st := s.backoff[accountID]
	if st.failures < 0 || st.failures > 20 {
		st.failures = 20 // 防移位溢出；此时延迟已封顶。
	}
	st.failures++
	delay := backoffBase << (st.failures - 1)
	if delay <= 0 || delay > backoffMax {
		delay = backoffMax
	}
	st.until = time.Now().Add(delay)
	s.backoff[accountID] = st
	s.mu.Unlock()
}

func (s *Service) clearBackoff(accountID int64) {
	s.mu.Lock()
	delete(s.backoff, accountID)
	s.mu.Unlock()
}

// isTransientError 判定轮询错误是否为可自动恢复的瞬态错误（非认证失效）。
func isTransientError(err error) bool {
	if ae, ok := domain.AsAppError(err); ok {
		return ae.Code != domain.CodeAuthExpired
	}
	return true
}

func (s *Service) logPollError(accountID int64, err error) {
	if ae, ok := domain.AsAppError(err); ok && ae.Code == domain.CodeAuthExpired {
		s.log.Debug("事件同步账号认证失效", "account_id", accountID, "err", err)
		return
	}
	s.log.Warn("事件同步轮询失败", "account_id", accountID, "err", err)
}

func (s *Service) drainRecalc() {
	for {
		select {
		case <-s.recalc:
		default:
			return
		}
	}
}

func (s *Service) waitStartup(ctx context.Context) {
	s.mu.Lock()
	readyAt := s.startupReadyAt
	s.mu.Unlock()
	if rem := time.Until(readyAt); rem > 0 {
		timer := time.NewTimer(rem)
		defer timer.Stop()
		select {
		case <-ctx.Done():
		case <-timer.C:
		}
	}
}
