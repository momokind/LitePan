package store

import (
	"context"
	"database/sql"

	"litepan/internal/domain"
)

type eventMonitorCursorRepo struct{ db *DB }

func (r *eventMonitorCursorRepo) Get(ctx context.Context, accountID int64) (*domain.EventMonitorCursor, error) {
	row := r.db.read.QueryRowContext(ctx,
		`SELECT account_id,last_event_id,updated_at FROM event_monitor_cursors WHERE account_id=?`, accountID)
	var (
		c         domain.EventMonitorCursor
		updatedAt sql.NullString
	)
	if err := row.Scan(&c.AccountID, &c.LastEventID, &updatedAt); err != nil {
		return nil, wrapDB(err)
	}
	c.UpdatedAt = parseTS(updatedAt)
	return &c, nil
}

func (r *eventMonitorCursorRepo) Upsert(ctx context.Context, c *domain.EventMonitorCursor) error {
	if c == nil {
		return domain.Errf(domain.CodeValidation)
	}
	_, err := r.db.write.ExecContext(ctx,
		`INSERT INTO event_monitor_cursors (account_id,last_event_id,updated_at)
		 VALUES (?,?,CURRENT_TIMESTAMP)
		 ON CONFLICT(account_id) DO UPDATE SET
		    last_event_id=excluded.last_event_id, updated_at=CURRENT_TIMESTAMP`,
		c.AccountID, c.LastEventID)
	return wrapDB(err)
}

func (r *eventMonitorCursorRepo) Delete(ctx context.Context, accountID int64) error {
	_, err := r.db.write.ExecContext(ctx, `DELETE FROM event_monitor_cursors WHERE account_id=?`, accountID)
	return wrapDB(err)
}
