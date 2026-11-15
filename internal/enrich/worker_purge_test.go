package enrich

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
)

type purgeTrackingStore struct {
	purgeCalls int
}

func (s *purgeTrackingStore) ListPendingEnrichment(context.Context, int) ([]PendingProgram, error) {
	return nil, nil
}
func (s *purgeTrackingStore) UpsertEnrichmentResult(context.Context, uuid.UUID, string, *Enrichment) error {
	return nil
}
func (s *purgeTrackingStore) MarkEnrichmentFailed(context.Context, uuid.UUID, string, string) error {
	return nil
}
func (s *purgeTrackingStore) DeferEnrichment(context.Context, uuid.UUID, string, time.Duration) error {
	return nil
}
func (s *purgeTrackingStore) LookupManualMatch(context.Context, uuid.UUID, string, bool) (int, int, string, bool, error) {
	return 0, 0, "", false, nil
}
func (s *purgeTrackingStore) PurgeExpiredCaches(context.Context) (int64, error) {
	s.purgeCalls++
	return 12, nil
}

func TestWorkerPurgesExpiredCachesEvenWhenNoPendingEnrichment(t *testing.T) {
	st := &purgeTrackingStore{}
	w := NewWorker(slog.New(slog.NewTextHandler(io.Discard, nil)), nil, st)
	w.PurgeInterval = time.Hour

	w.RunOnce(context.Background())
	if st.purgeCalls != 1 {
		t.Fatalf("first pass purgeCalls=%d, want 1", st.purgeCalls)
	}

	// Throttled within PurgeInterval.
	w.RunOnce(context.Background())
	if st.purgeCalls != 1 {
		t.Fatalf("throttled pass purgeCalls=%d, want 1", st.purgeCalls)
	}

	// Runs again once interval elapses.
	w.lastPurge = time.Now().Add(-2 * time.Hour)
	w.RunOnce(context.Background())
	if st.purgeCalls != 2 {
		t.Fatalf("post-interval pass purgeCalls=%d, want 2", st.purgeCalls)
	}
}
