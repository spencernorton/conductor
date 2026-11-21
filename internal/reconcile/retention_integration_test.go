package reconcile

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/dvr"
	"github.com/spencercnorton/conductor/internal/postprocess"
	"github.com/spencercnorton/conductor/internal/store"
	"github.com/spencercnorton/conductor/internal/testdb"
)

func seedCompletedDVRArtifact(
	t *testing.T,
	ctx context.Context,
	db *store.DB,
	chID uuid.UUID,
	fsID string,
	title, subTitle, onscreen string,
	isMovie bool,
	requestedBy, outputPath string,
	completedAt time.Time,
) store.DVRRecording {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		t.Fatal(err)
	}
	payload := []byte("ts-media-" + uuid.NewString())
	if err := os.WriteFile(outputPath, payload, 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)

	schedStart := completedAt.Add(-time.Hour).UTC().Truncate(time.Second)
	schedEnd := completedAt.UTC().Truncate(time.Second)
	rec, err := db.CreateDVRRecording(ctx, store.DVRRecording{
		ChannelID:          chID,
		Title:              title,
		SubTitle:           subTitle,
		EpisodeNumOnscreen: onscreen,
		IsMovie:            isMovie,
		ScheduledStart:     schedStart,
		ScheduledEnd:       schedEnd,
		RequestedBy:        requestedBy,
		OutputPath:         outputPath,
	})
	if err != nil {
		t.Fatalf("CreateDVRRecording(%s): %v", title, err)
	}
	if _, err := db.Pool.Exec(ctx, `
		UPDATE dvr_recording
		   SET state = 'completed',
		       started_at = $2,
		       completed_at = $3,
		       bytes_written = $4,
		       artifact_state = 'validated',
		       artifact_stage = 'none',
		       artifact_current = true,
		       artifact_path = $5,
		       artifact_bytes = $4,
		       artifact_sha256 = $6,
		       artifact_filesystem_id = $7,
		       artifact_validated_at = $3,
		       artifact_validator_version = 'dvr-media-v1'
		 WHERE id = $1`,
		rec.ID, schedStart, completedAt, int64(len(payload)), outputPath, sum[:], fsID); err != nil {
		t.Fatalf("promote test recording %s: %v", title, err)
	}
	updated, err := db.GetDVRRecording(ctx, rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	return updated
}

func TestIntegrationDVRRetentionPolicyAndImportSelfHeal(t *testing.T) {
	if os.Getenv("CONDUCTOR_INT_TEST") != "1" {
		t.Skip("integration test skipped (set CONDUCTOR_INT_TEST=1 to enable)")
	}
	baseDSN := os.Getenv("CONDUCTOR_TEST_DSN")
	if baseDSN == "" {
		t.Skip("CONDUCTOR_TEST_DSN is required")
	}

	ctx := context.Background()
	db, err := store.Open(ctx, testdb.New(t, baseDSN))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	ch, err := db.CreateChannel(ctx, store.Channel{
		Number: 201, Name: "Retention Test HD", EpgChannelID: "ret.test", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	dvrRoot := t.TempDir()
	fsID, err := dvr.EnsureArtifactFilesystemMarker(dvrRoot)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 15, 16, 0, 0, 0, time.UTC)

	// 1. Sonarr-requested episode already imported (hasFile=true), 2 hours old -> prune staging copy.
	importedEpPath := filepath.Join(dvrRoot, "TV", "Paw Patrol", "Season 06", "Paw Patrol.S06E02.Pups Save a Herd.ts")
	importedEp := seedCompletedDVRArtifact(t, ctx, db, ch.ID, fsID,
		"Paw Patrol", "Pups Save a Herd", "S06E02", false, "reconcile:sonarr",
		importedEpPath, now.Add(-2*time.Hour))

	// 2. Sonarr-requested episode NOT yet imported (hasFile=false), 45 days old (> 30d) -> MUST KEEP & retry import!
	pendingEpPath := filepath.Join(dvrRoot, "TV", "Sister Wives", "Season 21", "Sister Wives.S21E01.Premiere.ts")
	pendingEp := seedCompletedDVRArtifact(t, ctx, db, ch.ID, fsID,
		"Sister Wives", "Premiere", "S21E01", false, "reconcile:sonarr",
		pendingEpPath, now.Add(-45*24*time.Hour))

	// 3. Radarr-requested movie already imported (hasFile=true), 1 hour old -> prune staging copy.
	importedMoviePath := filepath.Join(dvrRoot, "Movies", "Sing (2016)", "Sing (2016).ts")
	importedMovie := seedCompletedDVRArtifact(t, ctx, db, ch.ID, fsID,
		"Sing", "", "", true, "reconcile:radarr",
		importedMoviePath, now.Add(-1*time.Hour))

	// 4. Unrequested recording (plex-gap) at 29 days old -> KEEP until 30 days.
	recentUnreqPath := filepath.Join(dvrRoot, "TV", "Unmonitored Show", "Season 01", "Unmonitored Show.S01E01.ts")
	recentUnreq := seedCompletedDVRArtifact(t, ctx, db, ch.ID, fsID,
		"Unmonitored Show", "", "S01E01", false, "reconcile:plex-gap",
		recentUnreqPath, now.Add(-29*24*time.Hour))

	// 5. Unrequested recording (manual) at 31 days old -> DELETE after 30 days.
	expiredUnreqPath := filepath.Join(dvrRoot, "TV", "Manual Capture", "Manual Capture.20261015T010000.ts")
	expiredUnreq := seedCompletedDVRArtifact(t, ctx, db, ch.ID, fsID,
		"Manual Capture", "", "", false, "manual",
		expiredUnreqPath, now.Add(-31*24*time.Hour))

	// 6. Stale 48h-old .partial diagnostic file -> swept.
	stalePartial := filepath.Join(dvrRoot, "TV", "Old Fail", "Old Fail.S01E01.ts.capture."+uuid.NewString()+".partial")
	if err := os.MkdirAll(filepath.Dir(stalePartial), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stalePartial, []byte("aborted-capture"), 0o644); err != nil {
		t.Fatal(err)
	}
	oldMod := now.Add(-48 * time.Hour)
	if err := os.Chtimes(stalePartial, oldMod, oldMod); err != nil {
		t.Fatal(err)
	}

	var sonarrFail atomic.Bool
	var sisterWivesUnmonitored atomic.Bool
	var importScans atomic.Int32
	var lastScannedPath atomic.Value

	sonarrSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if sonarrFail.Load() {
			http.Error(w, "sonarr restarting", http.StatusServiceUnavailable)
			return
		}
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/series":
			_ = json.NewEncoder(w).Encode([]sonarrSeries{
				{ID: 10, Title: "Paw Patrol", Year: 2013, SeriesType: "standard", Monitored: true},
				{ID: 20, Title: "Sister Wives", Year: 2010, SeriesType: "standard", Monitored: true},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/episode":
			switch r.URL.Query().Get("seriesId") {
			case "10":
				_ = json.NewEncoder(w).Encode([]sonarrEpisode{
					{SeasonNumber: 6, EpisodeNumber: 2, Title: "Pups Save a Herd", HasFile: true, Monitored: true},
				})
			case "20":
				_ = json.NewEncoder(w).Encode([]sonarrEpisode{
					{SeasonNumber: 21, EpisodeNumber: 1, Title: "Premiere", HasFile: false, Monitored: !sisterWivesUnmonitored.Load()},
				})
			default:
				_ = json.NewEncoder(w).Encode([]sonarrEpisode{})
			}
		case r.Method == http.MethodPost && r.URL.Path == "/api/v3/command":
			body, _ := io.ReadAll(r.Body)
			var payload map[string]any
			_ = json.Unmarshal(body, &payload)
			if p, ok := payload["path"].(string); ok {
				lastScannedPath.Store(p)
			}
			importScans.Add(1)
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"id":1}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(sonarrSrv.Close)

	radarrSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/api/v3/movie" {
			_ = json.NewEncoder(w).Encode([]radarrMovie{
				{ID: 1, Title: "Sing", Year: 2016, TMDbID: 335797, HasFile: true, Monitored: true},
			})
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(radarrSrv.Close)

	worker := &RetentionWorker{
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		DB:        db,
		Sonarr:    NewSonarrClient(sonarrSrv.URL, "sonarr-key"),
		Radarr:    NewRadarrClient(radarrSrv.URL, "radarr-key"),
		ArrImport: postprocess.NewArrImportStage(postprocess.ArrTarget{BaseURL: sonarrSrv.URL, APIKey: "sonarr-key"}, postprocess.ArrTarget{BaseURL: radarrSrv.URL, APIKey: "radarr-key"}),
		OutputDir: dvrRoot,
		Now:       func() time.Time { return now },
	}

	// Pass 1: Sonarr returns 503 -> TV episodes MUST fail closed (none deleted!), while Radarr movie + stale .partial still process safely.
	sonarrFail.Store(true)
	sum1, err := worker.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce pass 1 failed: %v", err)
	}
	if sum1.SkippedFailClosed != 4 {
		t.Fatalf("expected 4 TV rows skipped fail-closed during Sonarr outage, got %+v", sum1)
	}
	if _, err := os.Stat(expiredUnreqPath); err != nil {
		t.Fatalf("expired TV recording must NOT be deleted while Sonarr API is unreachable: %v", err)
	}
	if sum1.PrunedImported != 1 || sum1.CleanedDiagnostics != 1 {
		t.Fatalf("expected Radarr imported movie pruned + 1 diagnostic partial cleaned, got %+v", sum1)
	}
	if _, err := os.Stat(importedMoviePath); !os.IsNotExist(err) {
		t.Fatalf("imported movie staging file still exists: %v", err)
	}
	if _, err := os.Stat(stalePartial); !os.IsNotExist(err) {
		t.Fatalf("stale diagnostic partial still exists: %v", err)
	}

	// Pass 2: Sonarr healthy -> imported Paw Patrol pruned, 45d-old unimported Sister Wives PRESERVED + import re-triggered, 29d unrequested kept, 31d unrequested deleted!
	sonarrFail.Store(false)
	sum2, err := worker.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce pass 2 failed: %v", err)
	}
	if sum2.PrunedImported != 1 || sum2.ExpiredUnrequested != 1 || sum2.PreservedRequested != 1 || sum2.RetriedImports != 1 {
		t.Fatalf("unexpected pass 2 summary: %+v", sum2)
	}

	// Verify disk state.
	for _, gone := range []string{importedEpPath, expiredUnreqPath} {
		if _, err := os.Stat(gone); !os.IsNotExist(err) {
			t.Fatalf("expected %s to be removed, stat err=%v", gone, err)
		}
	}
	for _, kept := range []string{pendingEpPath, recentUnreqPath, filepath.Join(dvrRoot, ".conductor-fs-id")} {
		if _, err := os.Stat(kept); err != nil {
			t.Fatalf("expected %s to be preserved, stat err=%v", kept, err)
		}
	}
	if gotPath, _ := lastScannedPath.Load().(string); gotPath != pendingEpPath || importScans.Load() != 1 {
		t.Fatalf("arr-import retry scans=%d path=%q, want 1 scan on %q", importScans.Load(), gotPath, pendingEpPath)
	}

	// Verify DB state transitions.
	for _, id := range []uuid.UUID{importedEp.ID, importedMovie.ID, expiredUnreq.ID} {
		rec, err := db.GetDVRRecording(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if rec.ArtifactCurrent || rec.ArtifactState != "historical" {
			t.Fatalf("pruned recording %s has current=%v state=%q, want false/historical", id, rec.ArtifactCurrent, rec.ArtifactState)
		}
	}
	for _, id := range []uuid.UUID{pendingEp.ID, recentUnreq.ID} {
		rec, err := db.GetDVRRecording(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if !rec.ArtifactCurrent || rec.ArtifactState != "validated" {
			t.Fatalf("preserved recording %s has current=%v state=%q, want true/validated", id, rec.ArtifactCurrent, rec.ArtifactState)
		}
	}

	// Pass 3: Operator unmonitors Sister Wives S21E01 in Sonarr while hasFile=false ->
	// 45d-old capture transitions to the 30d unrequested policy without firing ArrImport and expires cleanly.
	sisterWivesUnmonitored.Store(true)
	sum3, err := worker.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce pass 3 failed: %v", err)
	}
	if sum3.ExpiredUnrequested != 1 || sum3.RetriedImports != 0 || importScans.Load() != 1 {
		t.Fatalf("unexpected pass 3 summary=%+v scans=%d", sum3, importScans.Load())
	}
	if _, err := os.Stat(pendingEpPath); !os.IsNotExist(err) {
		t.Fatalf("unmonitored 45d capture should be expired, stat err=%v", err)
	}
	unmonRec, err := db.GetDVRRecording(ctx, pendingEp.ID)
	if err != nil {
		t.Fatal(err)
	}
	if unmonRec.ArtifactCurrent || unmonRec.ArtifactState != "historical" {
		t.Fatalf("unmonitored expired recording current=%v state=%q, want false/historical", unmonRec.ArtifactCurrent, unmonRec.ArtifactState)
	}
}

func TestIntegrationReconcileUpgradesScheduledMovieOutputPathWithYear(t *testing.T) {
	if os.Getenv("CONDUCTOR_INT_TEST") != "1" {
		t.Skip("integration test skipped (set CONDUCTOR_INT_TEST=1 to enable)")
	}
	baseDSN := os.Getenv("CONDUCTOR_TEST_DSN")
	if baseDSN == "" {
		t.Skip("CONDUCTOR_TEST_DSN is required")
	}

	ctx := context.Background()
	db, err := store.Open(ctx, testdb.New(t, baseDSN))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	ch, err := db.CreateChannel(ctx, store.Channel{
		Number: 202, Name: "Movies HD", EpgChannelID: "movies.test", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now().Add(4 * time.Hour).UTC().Truncate(time.Second)
	var progID uuid.UUID
	if err := db.Pool.QueryRow(ctx, `
		INSERT INTO epg_program (channel_id, start_at, end_at, title, is_movie, source_hash)
		VALUES ($1, $2, $3, 'Northern Lights of Christmas', true, 'movie-upgrade-test')
		RETURNING id`,
		ch.ID, start, start.Add(2*time.Hour)).Scan(&progID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx, `
		INSERT INTO epg_program_enrichment (program_id, tmdb_id)
		VALUES ($1, 562411)`, progID); err != nil {
		t.Fatal(err)
	}

	dvrRoot := t.TempDir()
	legacyPath := filepath.Join(dvrRoot, "Movies", "Northern Lights of Christmas", "Northern Lights of Christmas.ts")
	booked, err := db.CreateDVRRecording(ctx, store.DVRRecording{
		ChannelID:      ch.ID,
		ProgramID:      &progID,
		Title:          "Northern Lights of Christmas",
		IsMovie:        true,
		ScheduledStart: start,
		ScheduledEnd:   start.Add(2 * time.Hour),
		RequestedBy:    "reconcile:radarr",
		OutputPath:     legacyPath,
	})
	if err != nil {
		t.Fatal(err)
	}

	radarrSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v3/wanted/missing" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(radarrMissingPage{
			TotalRecords: 1,
			Records: []radarrMovie{{
				Title: "Northern Lights of Christmas", Year: 2018, TMDbID: 562411, Monitored: true,
			}},
		})
	}))
	t.Cleanup(radarrSrv.Close)

	w := &Worker{
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		DB:        db,
		Radarr:    NewRadarrClient(radarrSrv.URL, "test-key"),
		OutputDir: dvrRoot,
	}
	w.reconcile(ctx)

	refreshed, err := db.GetDVRRecording(ctx, booked.ID)
	if err != nil {
		t.Fatal(err)
	}
	wantPath := filepath.Join(dvrRoot, "Movies", "Northern Lights of Christmas (2018)", "Northern Lights of Christmas (2018).ts")
	if refreshed.OutputPath != wantPath {
		t.Fatalf("upgraded OutputPath = %q, want %q", refreshed.OutputPath, wantPath)
	}
	_ = bytes.MinRead
}
