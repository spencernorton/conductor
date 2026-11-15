package dvr

import (
	"context"
	"crypto/sha256"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/store"
	"github.com/spencercnorton/conductor/internal/testdb"
)

func TestEnsureArtifactFilesystemMarkerIsIdempotentAndUsedByIdentity(t *testing.T) {
	root := t.TempDir()
	volID, err := EnsureArtifactFilesystemMarker(root)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(volID, "vol:") {
		t.Fatalf("volID=%q, want vol:<uuid>", volID)
	}
	again, err := EnsureArtifactFilesystemMarker(root)
	if err != nil || again != volID {
		t.Fatalf("second EnsureArtifactFilesystemMarker=(%q,%v), want (%q,nil)", again, err, volID)
	}

	nested := filepath.Join(root, "TV", "Show", "Season 01")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatal(err)
	}
	media := filepath.Join(nested, "Show.S01E01.ts")
	if err := os.WriteFile(media, []byte("mpegts-payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	gotID, err := ArtifactFilesystemIdentity(media)
	if err != nil || gotID != volID {
		t.Fatalf("ArtifactFilesystemIdentity=(%q,%v), want (%q,nil)", gotID, err, volID)
	}
}

func TestIntegrationRebootLegacyDevToVolumeAliasRebindsAndDemotesMissing(t *testing.T) {
	if os.Getenv("CONDUCTOR_INT_TEST") != "1" {
		t.Skip("integration test skipped (set CONDUCTOR_INT_TEST=1 to enable)")
	}
	base := os.Getenv("CONDUCTOR_TEST_DSN")
	if base == "" {
		t.Skip("CONDUCTOR_TEST_DSN is required")
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
		Number: 201, Name: "Volume Test", EpgChannelID: "vol.test", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	root := t.TempDir()
	volID, err := EnsureArtifactFilesystemMarker(root)
	if err != nil {
		t.Fatal(err)
	}

	// Present validated recording previously bound to a pre-reboot kernel st_dev.
	presentDir := filepath.Join(root, "Movies", "Present Film")
	if err := os.MkdirAll(presentDir, 0o755); err != nil {
		t.Fatal(err)
	}
	presentPath := filepath.Join(presentDir, "Present Film.ts")
	presentBytes := []byte("validated-present-recording-across-reboot")
	if err := os.WriteFile(presentPath, presentBytes, 0o644); err != nil {
		t.Fatal(err)
	}
	presentSHA := sha256.Sum256(presentBytes)

	// Missing validated recording whose file AND leaf folder were removed by
	// an importer before the host rebooted and shifted st_dev.
	missingPath := filepath.Join(root, "Movies", "Imported Film", "Imported Film.ts")
	missingSHA := sha256.Sum256([]byte("validated-imported-recording"))

	now := time.Now().UTC().Truncate(time.Second)
	insertValidated := func(title, outPath string, sz int64, sha [32]byte, legacyDev string) store.DVRRecording {
		t.Helper()
		rec, err := db.CreateDVRRecording(ctx, store.DVRRecording{
			ChannelID:      ch.ID,
			Title:          title,
			IsMovie:        true,
			ScheduledStart: now.Add(-2 * time.Hour),
			ScheduledEnd:   now.Add(-time.Hour),
			RequestedBy:    "reconcile:radarr",
			OutputPath:     outPath,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Pool.Exec(ctx, `
			UPDATE dvr_recording
			   SET state = 'completed', completed_at = now(),
			       bytes_written = $2, artifact_state = 'validated',
			       artifact_stage = 'none', artifact_current = true,
			       artifact_path = $3, artifact_bytes = $2,
			       artifact_sha256 = $4, artifact_filesystem_id = $5,
			       artifact_validated_at = now(), artifact_validator_version = 'v1'
			 WHERE id = $1`,
			rec.ID, sz, outPath, sha[:], legacyDev); err != nil {
			t.Fatal(err)
		}
		got, err := db.GetDVRRecording(ctx, rec.ID)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}

	presentRec := insertValidated("Present Film", presentPath, int64(len(presentBytes)), presentSHA, "dev:36")
	missingRec := insertValidated("Imported Film", missingPath, 24, missingSHA, "dev:36")

	summary, err := ReconcileCurrentDVRArtifacts(
		ctx, db, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if summary.Demoted != 1 || summary.Indeterminate != 0 || summary.InspectionBudgetExhausted {
		t.Fatalf("startup summary=%+v, want Demoted=1 Indeterminate=0", summary)
	}

	reloadedPresent, err := db.GetDVRRecording(ctx, presentRec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reloadedPresent.ArtifactCurrent || reloadedPresent.ArtifactFilesystemID != volID ||
		reloadedPresent.ArtifactStatSig == "" {
		t.Fatalf("present recording not migrated to %s with stat_sig: %+v", volID, reloadedPresent)
	}

	reloadedMissing, err := db.GetDVRRecording(ctx, missingRec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reloadedMissing.ArtifactCurrent || reloadedMissing.ArtifactState != "missing" {
		t.Fatalf("imported missing recording not demoted: %+v", reloadedMissing)
	}

	// Scheduling a new airing into missingPath must now succeed without
	// indeterminate st_dev preflight errors.
	rebooked, err := CreateDVRRecordingChecked(ctx, db, store.DVRRecording{
		ChannelID:      ch.ID,
		Title:          "Imported Film",
		IsMovie:        true,
		ScheduledStart: now.Add(3 * time.Hour),
		ScheduledEnd:   now.Add(4 * time.Hour),
		RequestedBy:    "reconcile:radarr",
		OutputPath:     missingPath,
	})
	if err != nil || rebooked.State != "scheduled" {
		t.Fatalf("rebook after demotion failed: rec=%+v err=%v", rebooked, err)
	}
}
