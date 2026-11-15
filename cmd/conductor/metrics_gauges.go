// metrics_gauges.go — DB-backed gauges for /metrics (audit Q1). Evaluated
// through a short-lived snapshot cache (10s TTL) so frequent Prometheus
// scrapes do not issue repeated full-table epg_program scans per scrape, and
// EPGCanonicalHealth supplies canonical, suppressed, and overlap counts in one
// call. On query error the affected sample is omitted so Prometheus records
// absence rather than a stale or zero value.
package main

import (
	"context"
	"sync"
	"time"

	"github.com/spencercnorton/conductor/internal/metrics"
	"github.com/spencercnorton/conductor/internal/store"
)

const (
	gaugeQueryTimeout      = 2 * time.Second
	defaultDBGaugeCacheTTL = 10 * time.Second
)

type gaugeSource interface {
	CountActiveStreams(ctx context.Context) (int, error)
	CountTuners(ctx context.Context) (int, error)
	EPGHorizonDays(ctx context.Context) (float64, error)
	EPGCanonicalHealth(ctx context.Context) (canonical, suppressed, overlaps int64, err error)
	ListEPGSourceFreshness(ctx context.Context) ([]store.EPGSourceFreshness, error)
}

type dbGaugeSnapshot struct {
	expiresAt time.Time

	activeStreams float64
	activeOK      bool

	tunerSlots float64
	tunersOK   bool

	horizonDays float64
	horizonOK   bool

	canonicalPrograms    float64
	suppressedCandidates float64
	canonicalOverlaps    float64
	canonicalOK          bool

	freshness   []store.EPGSourceFreshness
	freshnessOK bool
}

type dbGaugeCache struct {
	src gaugeSource
	ttl time.Duration
	now func() time.Time

	mu   sync.Mutex
	snap dbGaugeSnapshot
}

func newDBGaugeCache(src gaugeSource, ttl time.Duration, now func() time.Time) *dbGaugeCache {
	if ttl <= 0 {
		ttl = defaultDBGaugeCacheTTL
	}
	if now == nil {
		now = time.Now
	}
	return &dbGaugeCache{src: src, ttl: ttl, now: now}
}

func (c *dbGaugeCache) snapshot() dbGaugeSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	if !c.snap.expiresAt.IsZero() && now.Before(c.snap.expiresAt) {
		return c.snap
	}

	var next dbGaugeSnapshot
	next.expiresAt = now.Add(c.ttl)

	run := func(fn func(ctx context.Context)) {
		ctx, cancel := context.WithTimeout(context.Background(), gaugeQueryTimeout)
		defer cancel()
		fn(ctx)
	}

	run(func(ctx context.Context) {
		if n, err := c.src.CountActiveStreams(ctx); err == nil {
			next.activeStreams = float64(n)
			next.activeOK = true
		}
	})
	run(func(ctx context.Context) {
		if n, err := c.src.CountTuners(ctx); err == nil {
			next.tunerSlots = float64(n)
			next.tunersOK = true
		}
	})
	run(func(ctx context.Context) {
		if v, err := c.src.EPGHorizonDays(ctx); err == nil {
			next.horizonDays = v
			next.horizonOK = true
		}
	})
	run(func(ctx context.Context) {
		if canonical, suppressed, overlaps, err := c.src.EPGCanonicalHealth(ctx); err == nil {
			next.canonicalPrograms = float64(canonical)
			next.suppressedCandidates = float64(suppressed)
			next.canonicalOverlaps = float64(overlaps)
			next.canonicalOK = true
		}
	})
	run(func(ctx context.Context) {
		if rows, err := c.src.ListEPGSourceFreshness(ctx); err == nil {
			next.freshness = append([]store.EPGSourceFreshness(nil), rows...)
			next.freshnessOK = true
		}
	})

	c.snap = next
	return next
}

func (c *dbGaugeCache) activeStreams() (float64, bool) {
	s := c.snapshot()
	return s.activeStreams, s.activeOK
}

func (c *dbGaugeCache) credentialSlots() (float64, bool) {
	s := c.snapshot()
	return s.tunerSlots, s.tunersOK
}

func (c *dbGaugeCache) epgHorizonDays() (float64, bool) {
	s := c.snapshot()
	return s.horizonDays, s.horizonOK
}

func (c *dbGaugeCache) epgProgramsTotal() (float64, bool) {
	s := c.snapshot()
	return s.canonicalPrograms, s.canonicalOK
}

func (c *dbGaugeCache) epgCanonicalOverlaps() (float64, bool) {
	s := c.snapshot()
	return s.canonicalOverlaps, s.canonicalOK
}

func (c *dbGaugeCache) epgSuppressedCandidates() (float64, bool) {
	s := c.snapshot()
	return s.suppressedCandidates, s.canonicalOK
}

func (c *dbGaugeCache) epgSourceFreshnessSamples() []metrics.Sample {
	s := c.snapshot()
	if !s.freshnessOK {
		return nil
	}
	now := c.now()
	out := make([]metrics.Sample, 0, len(s.freshness))
	for _, r := range s.freshness {
		age := -1.0
		if r.LastOKAt != nil {
			age = now.Sub(*r.LastOKAt).Seconds()
		}
		out = append(out, metrics.Sample{
			Labels: metrics.Label("source", r.Name),
			Value:  age,
		})
	}
	return out
}

func registerDBGauges(db *store.DB) {
	cache := newDBGaugeCache(db, defaultDBGaugeCacheTTL, time.Now)

	metrics.RegisterGauge("conductor_active_streams",
		"Upstream slots in use (active_stream rows in starting/running).",
		cache.activeStreams)

	metrics.RegisterGauge("conductor_credential_slots_total",
		"Total tuner capacity (sum of enabled credentials' max_streams).",
		cache.credentialSlots)

	metrics.RegisterGauge("conductor_epg_horizon_days",
		"Days of future guide data remaining (0 = guide has run dry).",
		cache.epgHorizonDays)

	metrics.RegisterGauge("conductor_epg_programs_total",
		"Canonical programme rows visible to guide consumers.",
		cache.epgProgramsTotal)

	metrics.RegisterGauge("conductor_epg_canonical_overlaps",
		"Overlapping canonical programme pairs (must remain zero).",
		cache.epgCanonicalOverlaps)

	metrics.RegisterGauge("conductor_epg_suppressed_candidates",
		"Retained non-canonical programme candidates available for fallback promotion.",
		cache.epgSuppressedCandidates)

	metrics.RegisterGaugeVec("conductor_epg_source_last_ok_age_seconds",
		"Seconds since each enabled EPG source last fetched successfully (-1 = never).",
		cache.epgSourceFreshnessSamples)
}
