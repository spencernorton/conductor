package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/store"
)

type fakeGaugeSource struct {
	activeCalls    int
	tunerCalls     int
	horizonCalls   int
	canonicalCalls int
	freshnessCalls int

	activeErr    error
	canonicalErr error

	active     int
	tuners     int
	horizon    float64
	canonical  int64
	suppressed int64
	overlaps   int64
	freshness  []store.EPGSourceFreshness
}

func (f *fakeGaugeSource) CountActiveStreams(context.Context) (int, error) {
	f.activeCalls++
	return f.active, f.activeErr
}

func (f *fakeGaugeSource) CountTuners(context.Context) (int, error) {
	f.tunerCalls++
	return f.tuners, nil
}

func (f *fakeGaugeSource) EPGHorizonDays(context.Context) (float64, error) {
	f.horizonCalls++
	return f.horizon, nil
}

func (f *fakeGaugeSource) EPGCanonicalHealth(context.Context) (int64, int64, int64, error) {
	f.canonicalCalls++
	return f.canonical, f.suppressed, f.overlaps, f.canonicalErr
}

func (f *fakeGaugeSource) ListEPGSourceFreshness(context.Context) ([]store.EPGSourceFreshness, error) {
	f.freshnessCalls++
	return f.freshness, nil
}

func TestDBGaugeCacheCoalescesScrapesAndSharesCanonicalHealth(t *testing.T) {
	base := time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC)
	now := base
	lastOK := base.Add(-30 * time.Second)

	src := &fakeGaugeSource{
		active:     2,
		tuners:     6,
		horizon:    7.5,
		canonical:  84718,
		suppressed: 4039,
		overlaps:   0,
		freshness: []store.EPGSourceFreshness{
			{Name: "primary", LastOKAt: &lastOK},
			{Name: "never", LastOKAt: nil},
		},
	}

	cache := newDBGaugeCache(src, 10*time.Second, func() time.Time { return now })

	if v, ok := cache.activeStreams(); !ok || v != 2 {
		t.Fatalf("activeStreams=(%v,%v), want (2,true)", v, ok)
	}
	if v, ok := cache.credentialSlots(); !ok || v != 6 {
		t.Fatalf("credentialSlots=(%v,%v), want (6,true)", v, ok)
	}
	if v, ok := cache.epgHorizonDays(); !ok || v != 7.5 {
		t.Fatalf("epgHorizonDays=(%v,%v), want (7.5,true)", v, ok)
	}
	if v, ok := cache.epgProgramsTotal(); !ok || v != 84718 {
		t.Fatalf("epgProgramsTotal=(%v,%v), want (84718,true)", v, ok)
	}
	if v, ok := cache.epgSuppressedCandidates(); !ok || v != 4039 {
		t.Fatalf("epgSuppressedCandidates=(%v,%v), want (4039,true)", v, ok)
	}
	if v, ok := cache.epgCanonicalOverlaps(); !ok || v != 0 {
		t.Fatalf("epgCanonicalOverlaps=(%v,%v), want (0,true)", v, ok)
	}
	samples := cache.epgSourceFreshnessSamples()
	if len(samples) != 2 || samples[0].Value != 30 || samples[1].Value != -1 {
		t.Fatalf("freshness samples=%+v", samples)
	}

	// All 7 gauge evaluations within one scrape must hit EPGCanonicalHealth once.
	if src.activeCalls != 1 || src.tunerCalls != 1 || src.horizonCalls != 1 ||
		src.canonicalCalls != 1 || src.freshnessCalls != 1 {
		t.Fatalf("unexpected query counts: %+v", src)
	}

	// Warm scrape 4s later makes zero DB queries while advancing source age.
	now = base.Add(4 * time.Second)
	samples = cache.epgSourceFreshnessSamples()
	if len(samples) != 2 || samples[0].Value != 34 {
		t.Fatalf("warm freshness age=%+v, want 34s", samples)
	}
	if src.canonicalCalls != 1 || src.activeCalls != 1 {
		t.Fatalf("warm scrape queried DB: %+v", src)
	}

	// After TTL expiry, failed individual queries omit only their own samples.
	now = base.Add(11 * time.Second)
	src.active = 4
	src.canonicalErr = errors.New("db busy")
	if v, ok := cache.activeStreams(); !ok || v != 4 {
		t.Fatalf("refreshed activeStreams=(%v,%v), want (4,true)", v, ok)
	}
	if _, ok := cache.epgProgramsTotal(); ok {
		t.Fatal("expected failed canonical health query to omit gauge sample")
	}
	if src.canonicalCalls != 2 || src.activeCalls != 2 {
		t.Fatalf("expected second refresh after TTL: %+v", src)
	}
}
