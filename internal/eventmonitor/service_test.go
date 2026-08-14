package eventmonitor

import (
	"context"
	"testing"
	"time"
)

// 验证触发冷却：首次触发消费 pending，冷却期内再次触发被抑制。
func TestMaybeTriggerCooldown(t *testing.T) {
	s := NewService(Options{})

	s.mu.Lock()
	s.pendingTrigger[1] = true
	s.mu.Unlock()

	// 无上次触发记录 → 触发并消费 pending。
	s.maybeTrigger(context.Background(), 1)
	s.mu.Lock()
	consumed := !s.pendingTrigger[1]
	last := s.lastTrigger[1]
	s.mu.Unlock()
	if !consumed {
		t.Error("pending should be consumed on first trigger")
	}
	if last.IsZero() {
		t.Error("lastTrigger should be set after trigger")
	}

	// 重新置 pending，冷却期内再次触发 → 被抑制，pending 保留。
	s.mu.Lock()
	s.pendingTrigger[1] = true
	s.mu.Unlock()
	s.maybeTrigger(context.Background(), 1)
	s.mu.Lock()
	stillPending := s.pendingTrigger[1]
	s.mu.Unlock()
	if !stillPending {
		t.Error("pending should survive within cooldown")
	}
}

// 验证：冷却期已过（lastTrigger 久远）时 pending 被消费。
func TestMaybeTriggerAfterCooldown(t *testing.T) {
	s := NewService(Options{})
	s.mu.Lock()
	s.pendingTrigger[2] = true
	s.lastTrigger[2] = time.Now().Add(-time.Hour)
	s.mu.Unlock()

	s.maybeTrigger(context.Background(), 2)
	s.mu.Lock()
	consumed := !s.pendingTrigger[2]
	s.mu.Unlock()
	if !consumed {
		t.Error("pending should be consumed after cooldown elapsed")
	}
}
