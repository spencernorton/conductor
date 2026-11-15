package reconcile

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/store"
	"github.com/spencercnorton/conductor/internal/testdb"
)

func TestIntegrationReconcileExistingBookingsDoNotStarveMaxPerCycle(t *testing.T) {
	if os.Getenv("CONDUCTOR_INT_TEST") != "1" {
		t.Skip("integration test skipped (set CONDUCTOR_INT_TEST=1 to enable)")
	}
	base := os.Getenv("CONDUCTOR_TEST_DSN")
	if base == "" {
		t.Skip("CONDUCTOR_TEST_DSN is required for the reconcile integration test")
	}
	ctx := context.Background()
	db, err := store.Open(ctx, testdb.New(t, base))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	ch, err := db.CreateChannel(ctx, store.Channel{
		Number: 104, Name: "Cap Test", EpgChannelID: "cap.test", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	baseStart := time.Now().Add(4 * time.Hour).UTC().Truncate(time.Second)
	var records []sonarrEpisode
	for ep := 1; ep <= 4; ep++ {
		sub := fmt.Sprintf("Episode %d", ep)
		onscreen := fmt.Sprintf("S01E%02d", ep)
		st := baseStart.Add(time.Duration(ep) * 2 * time.Hour)
		if _, err := db.Pool.Exec(ctx, `
			INSERT INTO epg_program (channel_id, start_at, end_at, title, sub_title, episode_num_onscreen, source_hash)
			VALUES ($1, $2, $3, 'Frontier Dispatch', $4, $5, $6)`,
			ch.ID, st, st.Add(time.Hour), sub, onscreen, fmt.Sprintf("cap-test-%d", ep)); err != nil {
			t.Fatal(err)
		}
		records = append(records, sonarrEpisode{
			Title: sub, SeasonNumber: 1, EpisodeNumber: ep, Monitored: true,
			Series: &sonarrSeries{Title: "Frontier Dispatch", Year: 2024},
		})
	}

	sonarr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v3/wanted/missing" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(sonarrMissingPage{
			TotalRecords: len(records),
			Records:      records,
		})
	}))
	t.Cleanup(sonarr.Close)

	w := &Worker{
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		DB:          db,
		Sonarr:      NewSonarrClient(sonarr.URL, "test-key"),
		OutputDir:   t.TempDir(),
		MaxPerCycle: 2,
	}

	// Cycle 1 books the earliest 2 episodes up to MaxPerCycle=2.
	w.reconcile(ctx)
	rows, err := db.ListDVRRecordings(ctx, "scheduled", 10)
	if err != nil || len(rows) != 2 {
		t.Fatalf("after cycle 1: len=%d err=%v, want 2 scheduled recordings", len(rows), err)
	}

	// Cycle 2 re-encounters the first 2 already-booked episodes; they must not
	// consume MaxPerCycle, allowing episodes 3 and 4 to be scheduled.
	w.reconcile(ctx)
	rows, err = db.ListDVRRecordings(ctx, "scheduled", 10)
	if err != nil || len(rows) != 4 {
		t.Fatalf("after cycle 2: len=%d err=%v, want all 4 scheduled recordings", len(rows), err)
	}
}
