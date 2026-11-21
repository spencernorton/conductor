package reconcile

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/store"
)

func TestRemoveArtifactFileAndEmptyParents_PreservesRootAndVolumeMarker(t *testing.T) {
	root := t.TempDir()
	marker := filepath.Join(root, ".conductor-fs-id")
	if err := os.WriteFile(marker, []byte("vol:"+uuid.NewString()+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	target := filepath.Join(root, "TV", "Sample Show", "Season 01", "Sample Show.S01E01.ts")
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("mpegts-payload"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := removeArtifactFileAndEmptyParents(root, target); err != nil {
		t.Fatalf("removeArtifactFileAndEmptyParents failed: %v", err)
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("target still exists: err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "TV")); !os.IsNotExist(err) {
		t.Fatalf("empty TV subdir should be pruned, stat err=%v", err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("volume marker must remain intact: %v", err)
	}
	if err := removeArtifactFileAndEmptyParents(root, marker); err == nil {
		t.Fatal("refused marker removal must return an error")
	}
	outside := filepath.Join(t.TempDir(), "outside.ts")
	if err := os.WriteFile(outside, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := removeArtifactFileAndEmptyParents(root, outside); err == nil {
		t.Fatal("outside-root path removal must be refused")
	}
}

func TestIsDiagnosticArtifactName(t *testing.T) {
	id := uuid.NewString()
	cases := []struct {
		name string
		want bool
	}{
		{"Show.S01E01.ts.capture." + id + ".partial", true},
		{"Show.S01E01.ts.normalized." + id + ".partial", true},
		{"Show.S01E01.ts.superseded." + id, true},
		{"Show.S01E01.ts.normalized." + id + ".partial.publish-exchange", true},
		{"Show.S01E01.ts", false},
		{".conductor-fs-id", false},
		{"Show.S01E01.ts.superseded.not-a-uuid", false},
	}
	for _, tc := range cases {
		if got := isDiagnosticArtifactName(tc.name); got != tc.want {
			t.Errorf("isDiagnosticArtifactName(%q) = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestEvaluateRetention_UnmonitoredArrCaptureFallsBackToTTL(t *testing.T) {
	movieItem := store.DVRCompletedArtifact{
		Recording: store.DVRRecording{
			Title:       "Sample Movie",
			IsMovie:     true,
			RequestedBy: "reconcile:radarr",
		},
		TMDbID: 42,
	}
	dec, _ := evaluateMovieRetention(
		movieItem, "Sample Movie (2024)", 2024, true,
		[]radarrMovie{{ID: 1, Title: "Sample Movie", Year: 2024, TMDbID: 42, Monitored: false, HasFile: false}},
		nil,
	)
	if dec != decisionUnrequestedTTL {
		t.Fatalf("unmonitored Radarr movie got decision %v, want decisionUnrequestedTTL", dec)
	}

	epItem := store.DVRCompletedArtifact{
		Recording: store.DVRRecording{
			Title:              "Sample Show",
			EpisodeNumOnscreen: "S01E02",
			RequestedBy:        "reconcile:sonarr",
		},
		TVDBID: 99,
	}
	dec, _ = evaluateEpisodeRetention(
		epItem, "Sample Show", 0, true,
		[]sonarrSeries{{ID: 7, Title: "Sample Show", TVDBID: 99, Monitored: true}},
		nil,
		func(seriesID int) ([]sonarrEpisode, error) {
			return []sonarrEpisode{{SeasonNumber: 1, EpisodeNumber: 2, Monitored: false, HasFile: false}}, nil
		},
	)
	if dec != decisionUnrequestedTTL {
		t.Fatalf("unmonitored Sonarr episode got decision %v, want decisionUnrequestedTTL", dec)
	}

	// Unmatched episode on a requested capture still fails safe to preservation.
	dec, _ = evaluateEpisodeRetention(
		epItem, "Sample Show", 0, true,
		[]sonarrSeries{{ID: 7, Title: "Sample Show", TVDBID: 99, Monitored: true}},
		nil,
		func(seriesID int) ([]sonarrEpisode, error) {
			return nil, nil
		},
	)
	if dec != decisionKeepRequested {
		t.Fatalf("unmatched Sonarr-requested episode got decision %v, want decisionKeepRequested", dec)
	}
}
