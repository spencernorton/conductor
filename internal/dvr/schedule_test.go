package dvr_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/dvr"
	"github.com/spencercnorton/conductor/internal/store"
)

func TestScheduleHandlerScopedValidationAndExpiry(t *testing.T) {
	baseNow := time.Date(2026, 11, 15, 2, 0, 0, 0, time.UTC)
	tn := dvr.New(nil, "http://test.local:8409", "secret-indexer-key", "Test")
	hit := store.EPGSearchHit{
		ProgramID: uuid.New(),
		ChannelID: uuid.New(),
		StartAt:   baseNow.Add(30 * time.Minute),
		EndAt:     baseNow.Add(90 * time.Minute),
		Title:     "Evening News",
	}
	signedURL := tn.SignedScheduleURLForTest(hit)
	qIdx := strings.Index(signedURL, "?")
	if qIdx < 0 {
		t.Fatalf("missing query in signed URL: %s", signedURL)
	}
	rawQuery := signedURL[qIdx:]

	t.Run("unconfigured_handler_rejects_401", func(t *testing.T) {
		sched := &dvr.ScheduleHandler{Now: func() time.Time { return baseNow }}
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/dvr/schedule.torrent"+rawQuery, nil)
		sched.ServeHTTP(rr, req)
		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("got %d, want 401", rr.Code)
		}
	})

	t.Run("unsigned_request_without_apikey_rejects_400", func(t *testing.T) {
		sched := &dvr.ScheduleHandler{
			Torznab: tn, IndexerKey: "secret-indexer-key",
			Now: func() time.Time { return baseNow },
		}
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/dvr/schedule.torrent", nil)
		sched.ServeHTTP(rr, req)
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("got %d, want 400", rr.Code)
		}
	})

	t.Run("legacy_wrong_apikey_rejects_401", func(t *testing.T) {
		sched := &dvr.ScheduleHandler{
			Torznab: tn, IndexerKey: "secret-indexer-key",
			Now: func() time.Time { return baseNow },
		}
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/dvr/schedule.torrent"+rawQuery+"&apikey=wrong-key", nil)
		sched.ServeHTTP(rr, req)
		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("got %d, want 401", rr.Code)
		}
	})

	t.Run("expired_link_after_airing_end_rejects_410", func(t *testing.T) {
		sched := &dvr.ScheduleHandler{
			Torznab: tn, IndexerKey: "secret-indexer-key",
			Now: func() time.Time { return hit.EndAt },
		}
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/dvr/schedule.torrent"+rawQuery, nil)
		sched.ServeHTTP(rr, req)
		if rr.Code != http.StatusGone {
			t.Fatalf("expired link got %d, want 410; body=%q", rr.Code, rr.Body.String())
		}
	})
}
