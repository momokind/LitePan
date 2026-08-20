package eventmonitor

import (
	"context"
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
	startupDelay     = 30 * time.Second
	defaultPollSec   = 60
	defaultCooldown  = 2 * time.Minute
	transientBackoff = 5 * time.Minute
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
	backoffUntil   map[int64]time.Time // 监听账号级退避
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

// NewService 构造事件同步服务。
func NewService(opts Options) *Service {
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}
	return &Service{
		accounts:       opts.Accounts,
		cursors:        opts.Cursors,
		strmTasks:      opts.StrmTasks,
		drivers:        opts.Drivers,
		files:          opts.Files,
		strm:           opts.Strm,
		cache:          opts.Cache,
		settings:       opts.Settings,
		log:            log,
		lastTrigger:    make(map[int64]time.Time),
		backoffUntil:   make(map[int64]time.Time),
		recalc:         make(chan struct{}, 1),
		pathCache:      newPathCache(time.Minute),
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
		return
	}
	if !s.listenerSupportsEvents(ctx, accountID) {
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
		}
		return
	}
	if next != "" && next != fromID {
		s.upsertCursor(ctx, accountID, next)
	}
	if len(events) == 0 {
		return
	}
	s.recordPoll(events)
	s.matchAndTrigger(ctx, events)
}

// matchAndTrigger 把事件目录解析为路径，匹配 115 Open STRM 任务并触发同步。
func (s *Service) matchAndTrigger(ctx context.Context, events []domain.OperationEvent) {
	if s.strm == nil || s.files == nil || s.strmTasks == nil {
		return
	}
	parentIDs := uniqueParentIDs(events)
	if len(parentIDs) == 0 {
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
	groups := make(map[int64]*group)
	for _, t := range tasks {
		if t == nil || t.Status == domain.StrmStatusPaused {
			continue
		}
		acc := accByID[t.AccountID]
		if acc == nil || !acc.IsActive || acc.DriverType != targetDriverType {
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
		return
	}

	cooldown := s.triggerCooldown()
	now := time.Now()
	var triggeredNames []string
	for accountID, g := range groups {
		resolved := s.resolvePaths(ctx, accountID, parentIDs)
		if len(resolved) == 0 {
			continue
		}
		for _, t := range g.tasks {
			var matched []string
			for parentID, path := range resolved {
				if matchTaskScope(path, t) {
					matched = append(matched, parentID)
				}
			}
			if len(matched) == 0 {
				continue
			}
			if !s.acquireTrigger(t.ID, cooldown, now) {
				continue
			}
			// 失效该账号受影响目录缓存，确保重扫读取最新变更。
			for _, parentID := range matched {
				cache.InvalidateDirKeys(s.cache, accountID, parentID)
			}
			if _, err := s.strm.RunTaskNow(ctx, t.ID, domain.StrmRunModeAuto); err != nil {
				s.log.Warn("事件同步触发 STRM 任务失败", "task_id", t.ID, "task", t.Name, "err", err)
				continue
			}
			s.log.Info("事件同步触发 STRM 任务", "task_id", t.ID, "task", t.Name, "dirs", len(matched))
			triggeredNames = append(triggeredNames, t.Name)
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
func (s *Service) acquireTrigger(taskID int64, cooldown time.Duration, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if last, ok := s.lastTrigger[taskID]; ok && now.Sub(last) < cooldown {
		return false
	}
	s.lastTrigger[taskID] = now
	s.triggerCount++
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
	delete(s.backoffUntil, accountID)
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
	return time.Now().Before(s.backoffUntil[accountID])
}

func (s *Service) setBackoff(accountID int64) {
	s.mu.Lock()
	s.backoffUntil[accountID] = time.Now().Add(transientBackoff)
	s.mu.Unlock()
}

func (s *Service) clearBackoff(accountID int64) {
	s.mu.Lock()
	delete(s.backoffUntil, accountID)
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
