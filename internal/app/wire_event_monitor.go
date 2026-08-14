package app

import (
	"litepan/internal/eventmonitor"
	"litepan/internal/logx"
	"litepan/internal/strm"
)

func wireEventMonitor(st *storeBundle, strmSvc *strm.Service, core *coreBundle, logs *logx.Manager) *eventmonitor.Service {
	return eventmonitor.NewService(eventmonitor.Options{
		Accounts:  st.store.Accounts,
		Cursors:   st.store.EventMonitorCursors,
		StrmTasks: st.store.StrmTasks,
		Drivers:   core.drivers,
		Strm:      strmSvc,
		Cache:     core.cache,
		Settings:  st.settings,
		Log:       logs.For(logx.ModuleSystem),
	})
}
