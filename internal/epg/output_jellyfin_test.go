package epg

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/store"
)

// Pins the provider-id lines both guides emit: Plex keeps its spelling, and
// the Jellyfin guide uses the forms Jellyfin's XMLTV reader parses.
func TestProviderIDSpellingPerGuide(t *testing.T) {
	base := store.ProgramOutputRow{
		ChannelID:   uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		ChannelName: "Test Channel", ChannelNumber: 1,
		StartAt: time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC),
		EndAt:   time.Date(2026, 5, 8, 14, 0, 0, 0, time.UTC),
	}
	movie, episode := base, base
	movie.Title, movie.IsMovie, movie.EnrichmentTMDbID, movie.EnrichmentIMDbID = "The Matrix", true, 603, "tt0133093"
	episode.Title, episode.EnrichmentTMDbID, episode.EnrichmentIMDbID = "Seinfeld", 1400, "tt0098904"

	cases := []struct {
		name     string
		row      store.ProgramOutputRow
		jellyfin bool
		want     string
	}{
		{"plex movie", movie, false, `    <episode-num system="imdb.com">title/tt0133093</episode-num>
    <episode-num system="themoviedb.org">movie/603</episode-num>
`},
		{"plex episode", episode, false, `    <episode-num system="imdb.com">title/tt0098904</episode-num>
    <episode-num system="themoviedb.org">tv/1400</episode-num>
`},
		{"jellyfin movie", movie, true, `    <episode-num system="themoviedb.org">603</episode-num>
    <episode-num system="imdb.com">episode/tt0133093</episode-num>
`},
		{"jellyfin episode", episode, true, `    <episode-num system="themoviedb.org">series/1400</episode-num>
    <episode-num system="imdb.com">series/tt0098904</episode-num>
`},
	}
	for _, tc := range cases {
		var buf bytes.Buffer
		writeProgramme(&buf, tc.row, "", tc.jellyfin)
		var got strings.Builder
		for _, line := range strings.SplitAfter(buf.String(), "\n") {
			if strings.Contains(line, `system="imdb.com"`) || strings.Contains(line, `system="themoviedb.org"`) {
				got.WriteString(line)
			}
		}
		if got.String() != tc.want {
			t.Errorf("%s:\n%s\nwant:\n%s", tc.name, got.String(), tc.want)
		}
		if !tc.jellyfin {
			var plex bytes.Buffer
			emitProgramme(&plex, tc.row, "")
			if plex.String() != buf.String() {
				t.Errorf("%s: emitProgramme differs from the Plex guide", tc.name)
			}
		}
	}
}
