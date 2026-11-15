package epg

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/store"
)

func TestRedactURLStripsEmbeddedCredentials(t *testing.T) {
	cases := []struct {
		name      string
		raw       string
		forbidden []string
		wantSub   string
	}{
		{
			name:      "query_username_password",
			raw:       "http://provider.example.test:8080/xmltv.php?username=alice_acct&password=s3cr3t_pw99&type=xmltv",
			forbidden: []string{"alice_acct", "s3cr3t_pw99"},
			wantSub:   "password=REDACTED&type=xmltv&username=REDACTED",
		},
		{
			name:      "userinfo_basic",
			raw:       "https://" + "guide_user" + ":" + "top_secret_token" + "@guide.example.test/epg/guide.xml",
			forbidden: []string{"guide_user", "top_secret_token"},
			wantSub:   "https://REDACTED@guide.example.test/epg/guide.xml",
		},
		{
			name:      "xtream_path_credentials",
			raw:       "http://panel.example.test:8080/xmltv/acct_42/pw_hunter2/epg.xml",
			forbidden: []string{"acct_42", "pw_hunter2"},
			wantSub:   "/xmltv/REDACTED/REDACTED/epg.xml",
		},
		{
			name:      "token_query_case_insensitive",
			raw:       "https://epg.example.test/feed.xml?API_KEY=key_abc123&format=xml",
			forbidden: []string{"key_abc123"},
			wantSub:   "API_KEY=REDACTED&format=xml",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := RedactURL(tc.raw)
			for _, secret := range tc.forbidden {
				if strings.Contains(got, secret) {
					t.Fatalf("redacted URL %q still contains secret %q", got, secret)
				}
			}
			if !strings.Contains(got, tc.wantSub) {
				t.Fatalf("redacted URL %q missing expected %q", got, tc.wantSub)
			}
		})
	}
}

func TestIngestScrubsCredentialsFromTransportAndHTTPErrors(t *testing.T) {
	const secretUser = "epg_member_77"
	const secretPass = "ultra_secret_pass_88"

	t.Run("transport_error_scrubs_url", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		closedURL := srv.URL + "/xmltv.php?username=" + secretUser + "&password=" + secretPass
		srv.Close()

		src := store.EPGSource{Name: "test-src", URL: closedURL}
		_, _, _, status, _, _, err := Ingest(context.Background(), nil, src, &http.Client{Timeout: time.Second})
		if err == nil {
			t.Fatal("expected transport error against closed server")
		}
		for _, text := range []string{err.Error(), status} {
			if strings.Contains(text, secretUser) || strings.Contains(text, secretPass) {
				t.Fatalf("credential leaked in ingest failure output: %q", text)
			}
		}
	})

	t.Run("http_error_body_echoing_url_is_scrubbed", func(t *testing.T) {
		var targetURL string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte("denied for " + targetURL))
		}))
		defer srv.Close()
		targetURL = srv.URL + "/xmltv.php?username=" + secretUser + "&password=" + secretPass

		src := store.EPGSource{Name: "test-src", URL: targetURL}
		_, _, _, status, _, _, err := Ingest(context.Background(), nil, src, srv.Client())
		if err == nil {
			t.Fatal("expected HTTP 403 error")
		}
		for _, text := range []string{err.Error(), status} {
			if strings.Contains(text, secretUser) || strings.Contains(text, secretPass) {
				t.Fatalf("credential leaked in HTTP status output: %q", text)
			}
		}
	})

	t.Run("preserves_concurrency_and_context_sentinels", func(t *testing.T) {
		raw := "http://panel.example.test/xmltv.php?password=" + secretPass
		wrappedStale := fmt.Errorf("fetch %s: %w", raw, store.ErrStaleEPGSnapshot)
		scrubbed := ScrubError(wrappedStale, raw)
		if !errors.Is(scrubbed, store.ErrStaleEPGSnapshot) {
			t.Fatalf("lost ErrStaleEPGSnapshot sentinel: %v", scrubbed)
		}
		if strings.Contains(scrubbed.Error(), secretPass) {
			t.Fatalf("secret survived ScrubError: %q", scrubbed.Error())
		}

		wrappedCancel := fmt.Errorf("fetch %s: %w", raw, context.Canceled)
		scrubbedCancel := ScrubError(wrappedCancel, raw)
		if !errors.Is(scrubbedCancel, context.Canceled) {
			t.Fatalf("lost context.Canceled sentinel: %v", scrubbedCancel)
		}
	})
}

func TestOutputCachedXMLTVMatchesExactProjectionAndInvalidatesOnChange(t *testing.T) {
	now := time.Date(2026, 11, 14, 18, 0, 0, 0, time.UTC)
	rows := []store.ProgramOutputRow{{
		ChannelName:  "News HD",
		ChannelEPGID: "news.hd",
		Title:        "Evening Report",
		StartAt:      now,
		EndAt:        now.Add(time.Hour),
		IsNew:        true,
	}}
	out := NewOutput(nil, "http://conductor.example.test:8409")

	rec1 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodGet, "/epg.xml", nil)
	out.serveCachedXMLTV(rec1, req1, rows, out.baseURL, now, now)
	etag1 := rec1.Header().Get("ETag")
	if rec1.Code != http.StatusOK || etag1 == "" {
		t.Fatalf("first render status=%d etag=%q", rec1.Code, etag1)
	}

	// Warm unconditional GET hits the cached XML buffer and returns identical bytes.
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/epg.xml", nil)
	out.serveCachedXMLTV(rec2, req2, rows, out.baseURL, now, now)
	if !bytes.Equal(rec1.Body.Bytes(), rec2.Body.Bytes()) || rec2.Header().Get("ETag") != etag1 {
		t.Fatal("warm cached GET diverged from initial render")
	}

	// Conditional GET with matching ETag still returns 304 with empty body.
	rec304 := httptest.NewRecorder()
	req304 := httptest.NewRequest(http.MethodGet, "/epg.xml", nil)
	req304.Header.Set("If-None-Match", etag1)
	out.serveCachedXMLTV(rec304, req304, rows, out.baseURL, now, now)
	if rec304.Code != http.StatusNotModified || rec304.Body.Len() != 0 {
		t.Fatalf("conditional GET status=%d bodyLen=%d", rec304.Code, rec304.Body.Len())
	}

	// Mutating programme data invalidates ETag and refreshes cached XML immediately.
	rows[0].Title = "Special Report"
	rec3 := httptest.NewRecorder()
	req3 := httptest.NewRequest(http.MethodGet, "/epg.xml", nil)
	out.serveCachedXMLTV(rec3, req3, rows, out.baseURL, now, now)
	if rec3.Header().Get("ETag") == etag1 || !strings.Contains(rec3.Body.String(), "Special Report") {
		t.Fatalf("updated guide did not invalidate cached XML: etag=%q body=%s", rec3.Header().Get("ETag"), rec3.Body.String())
	}
}
