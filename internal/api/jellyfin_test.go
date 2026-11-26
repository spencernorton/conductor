package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/spencercnorton/conductor/internal/hdhr"
	"github.com/spencercnorton/conductor/internal/store"
)

func TestJellyfinDiscoverPointsAtJellyfinLineup(t *testing.T) {
	h := NewRouter(Deps{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Device: hdhr.Device{DeviceID: "12345678", BaseURL: "http://tuner.test:8409",
			TunerCount: hdhr.StaticTunerCount(4)},
	})
	for path, want := range map[string]string{
		"/discover.json":          "http://tuner.test:8409",
		"/jellyfin/discover.json": "http://tuner.test:8409/jellyfin",
	} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		var got struct{ BaseURL, LineupURL, DeviceID string }
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatalf("%s: %v: %s", path, err, rec.Body.String())
		}
		if got.BaseURL != want || got.LineupURL != want+"/lineup.json" || got.DeviceID != "12345678" {
			t.Errorf("%s = %+v, want BaseURL %s", path, got, want)
		}
	}
}

// Jellyfin copies any response body into its live buffer as media, so its
// tuner must see a bare status; Plex keeps the readable reason.
func TestJellyfinStreamErrorsCarryNoBody(t *testing.T) {
	jellyfin := httptest.NewRecorder()
	if !writeStreamStartupError(jellyfin, store.ErrNoSlot, bodylessError) {
		t.Fatal("ErrNoSlot not mapped")
	}
	if jellyfin.Code != http.StatusServiceUnavailable || jellyfin.Body.Len() != 0 {
		t.Errorf("jellyfin: %d %q, want 503 with no body", jellyfin.Code, jellyfin.Body.String())
	}
	plex := httptest.NewRecorder()
	writeStreamStartupError(plex, store.ErrNoSlot, http.Error)
	if plex.Code != http.StatusServiceUnavailable || plex.Body.String() != "no upstream slots available\n" {
		t.Errorf("plex: %d %q", plex.Code, plex.Body.String())
	}
}
