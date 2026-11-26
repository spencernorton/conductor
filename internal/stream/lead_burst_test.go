package stream

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/spencercnorton/conductor/internal/metrics"
)

// leadBurstPacerIntervals replays a startup on a fake clock: a 10-chunk
// validated prefix sharing one release edge, then queued reservoir chunks
// dequeued back to back, every chunk carrying a PCR 50ms after the last. It
// returns the wall interval before each chunk after the first.
func leadBurstPacerIntervals(t *testing.T, burst bool, allowed func(chunk int) bool) []time.Duration {
	t.Helper()
	const step = 50 * time.Millisecond
	wall := time.Unix(1_700_000_000, 0)
	pacer := testTransportPCRPacer(false)
	pacer.now = func() time.Time { return wall }
	pacer.wait = func(_ context.Context, delay time.Duration) error {
		wall = wall.Add(delay)
		return nil
	}
	chunks := pcrSparseTestChunks(70, step)
	const prefixChunks = 10
	if err := pacer.PrepareBufferedPrefix(bytes.Join(chunks[:prefixChunks], nil), chunkSize); err != nil {
		t.Fatal(err)
	}
	current := 0
	if burst {
		pacer.BurstLead(40*step, func() bool { return allowed(current) })
	}
	release := wall
	last := wall
	var intervals []time.Duration
	for current = range chunks {
		arrival := wall // the read loop restamps a queued chunk at dequeue
		if current < prefixChunks {
			arrival = release
		}
		if err := pacer.Wait(context.Background(), chunks[current], arrival); err != nil {
			t.Fatalf("chunk %d: %v", current, err)
		}
		if current > 0 {
			intervals = append(intervals, wall.Sub(last))
		}
		last = wall
	}
	return intervals
}

func assertIntervals(t *testing.T, intervals []time.Duration, from, to int, want time.Duration) {
	t.Helper()
	for i := from; i < to; i++ {
		if intervals[i] != want {
			t.Fatalf("interval before chunk %d = %s, want %s (all: %v)", i+1, intervals[i], want, intervals)
		}
	}
}

func TestTransportPCRPacerLeadBurst(t *testing.T) {
	const cadence = 50 * time.Millisecond
	const burst = cadence / maxTransportRecoveryRate

	t.Run("without a burst the reservoir stays queued at media cadence", func(t *testing.T) {
		// Plex's path: nothing calls BurstLead, so every chunk after the
		// validated prefix leaves at its media cadence.
		intervals := leadBurstPacerIntervals(t, false, nil)
		assertIntervals(t, intervals, 0, len(intervals), cadence)
	})

	t.Run("a burst releases the held lead at the maximum recovery rate", func(t *testing.T) {
		before := metrics.RelayLeadBursts.Value()
		intervals := leadBurstPacerIntervals(t, true, func(int) bool { return true })
		// Prefix (10 chunks) plus the 40-chunk lead leave at 16x, then the
		// rest of the reservoir returns to media cadence.
		assertIntervals(t, intervals, 0, 45, burst)
		assertIntervals(t, intervals, 52, len(intervals), cadence)
		if delta := metrics.RelayLeadBursts.Value() - before; delta != 1 {
			t.Fatalf("lead burst metric delta=%d, want 1", delta)
		}
	})

	t.Run("a plain subscriber attaching ends the burst at once", func(t *testing.T) {
		before := metrics.RelayLeadBurstsEnded.Value()
		intervals := leadBurstPacerIntervals(t, true, func(chunk int) bool { return chunk < 20 })
		assertIntervals(t, intervals, 0, 19, burst)
		assertIntervals(t, intervals, 19, len(intervals), cadence)
		if delta := metrics.RelayLeadBurstsEnded.Value() - before; delta != 1 {
			t.Fatalf("lead burst ended metric delta=%d, want 1", delta)
		}
	})

	t.Run("a plain subscriber before the first emission restores the prefix epoch", func(t *testing.T) {
		intervals := leadBurstPacerIntervals(t, true, func(int) bool { return false })
		assertIntervals(t, intervals, 0, len(intervals), cadence)
	})
}

func TestTransportPCRPacerBurstLeadIgnoredOutsideStartup(t *testing.T) {
	finite := testTransportPCRPacer(true)
	_ = finite.PrepareBufferedPrefix(bytes.Join(pcrSparseTestChunks(10, 50*time.Millisecond), nil), chunkSize)
	finite.BurstLead(time.Second, func() bool { return true })
	if finite.leadBurstAllowed != nil {
		t.Fatal("finite replay armed a lead burst")
	}
	live := testTransportPCRPacer(false)
	if err := live.PrepareBufferedPrefix(bytes.Join(pcrSparseTestChunks(10, 50*time.Millisecond), nil), chunkSize); err != nil {
		t.Fatal(err)
	}
	live.BurstLead(0, func() bool { return true })
	if live.leadBurstAllowed != nil || live.pendingRate != 0 {
		t.Fatal("a zero hold armed a lead burst")
	}
}

func TestSubscriberSetAllLeadBurst(t *testing.T) {
	var ss subscriberSet
	ss.init("lead-burst", testLogger())
	if ss.allLeadBurst() {
		t.Fatal("an empty set bursts")
	}
	_, _, _, leaveJellyfin := ss.SubscribeForStartupLeadBurst("jellyfin")
	if !ss.allLeadBurst() {
		t.Fatal("a Jellyfin-only set does not burst")
	}
	_, _, _, leavePlex := ss.SubscribeForStartup("plex")
	if ss.allLeadBurst() {
		t.Fatal("a set with a Plex subscriber bursts")
	}
	leavePlex()
	if !ss.allLeadBurst() {
		t.Fatal("the set did not return to bursting after Plex left")
	}
	leaveJellyfin()
	ss.closeAll()
}

func TestAttachForStartupMarksOnlyLeadBurstTunes(t *testing.T) {
	for _, tc := range []struct {
		name string
		ctx  context.Context
		want bool
	}{
		{"plex", context.Background(), false},
		{"jellyfin", WithLeadBurst(context.Background()), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := NewStreamer("attach-"+tc.name, "http://127.0.0.1:1", testLogger(), nil)
			_, _, _, leave := attachForStartup(tc.ctx, s, tc.name)
			defer leave()
			s.mu.Lock()
			got := s.subs[tc.name].leadBurst.Load()
			s.mu.Unlock()
			if got != tc.want {
				t.Fatalf("lead burst=%v, want %v", got, tc.want)
			}
		})
	}
}

// realtimeLiveStreamServer delivers the first live probe window at once (a
// provider's connect burst) and then one 50ms-PCR chunk every 50ms.
func realtimeLiveStreamServer(t *testing.T) *httptest.Server {
	t.Helper()
	const step = 50 * time.Millisecond
	chunks := pcrSparseTestChunks(delayedLivePrefixChunkCount+100, step)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp2t")
		flusher, _ := w.(http.Flusher)
		for _, chunk := range chunks[:delayedLivePrefixChunkCount] {
			_, _ = w.Write(chunk)
		}
		flusher.Flush()
		ticker := time.NewTicker(step)
		defer ticker.Stop()
		for _, chunk := range chunks[delayedLivePrefixChunkCount:] {
			select {
			case <-ticker.C:
			case <-r.Context().Done():
				return
			}
			_, _ = w.Write(chunk)
			flusher.Flush()
		}
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	return srv
}

// timeToLead runs one startup with a 600ms lead and returns how long the
// measured subscriber took, from its first byte, to receive the prefix plus
// that lead.
func timeToLead(t *testing.T, attach func(s *Streamer) (<-chan []byte, func())) time.Duration {
	t.Helper()
	const lead = 600 * time.Millisecond
	s := NewStreamer("lead-burst", realtimeLiveStreamServer(t).URL, testLogger(), nil)
	s.prefixValidator = &thresholdPrefixValidator{
		readyAt: delayedLivePrefixChunkCount * chunkSize, mediaDuration: 1600 * time.Millisecond,
		pcrPID: 0x100, havePCRPID: true,
	}
	s.sourceStartupTimeout = 5 * time.Second
	s.startupLead = lead
	media, leave := attach(s)
	defer leave()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := s.runOnce(ctx)
		done <- err
	}()
	want := (delayedLivePrefixChunkCount + int(lead/(50*time.Millisecond))) * chunkSize
	var first time.Time
	got := 0
	deadline := time.After(10 * time.Second)
	for got < want {
		select {
		case data := <-media:
			if first.IsZero() {
				first = time.Now()
			}
			got += len(data)
		case <-deadline:
			t.Fatalf("received %d of %d bytes", got, want)
		}
	}
	elapsed := time.Since(first)
	t.Logf("prefix+lead delivered %s after the first byte", elapsed)
	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run: %v", err)
	}
	return elapsed
}

func startupMedia(first, rest <-chan startupChunk) <-chan []byte {
	out := make(chan []byte, 1024)
	go func() {
		if chunk, ok := <-first; ok {
			out <- chunk.data
		}
		for chunk := range rest {
			out <- chunk.data
		}
	}()
	return out
}

func TestStreamerLeadBurstOnlyForLeadBurstSubscribers(t *testing.T) {
	plain := func(s *Streamer) (<-chan []byte, func()) {
		first, rest, _, leave := s.SubscribeForStartup("plex")
		return startupMedia(first, rest), leave
	}
	jellyfin := func(s *Streamer) (<-chan []byte, func()) {
		first, rest, _, leave := s.SubscribeForStartupLeadBurst("jellyfin")
		return startupMedia(first, rest), leave
	}

	t.Run("plex keeps the lead queued as reservoir", func(t *testing.T) {
		// 1.6s prefix at its bounded 2x, then the 600ms lead at media cadence.
		if elapsed := timeToLead(t, plain); elapsed < time.Second {
			t.Fatalf("plain subscriber got prefix+lead in %s, want the lead paced (>1s)", elapsed)
		}
	})

	t.Run("jellyfin gets the lead as a burst", func(t *testing.T) {
		if elapsed := timeToLead(t, jellyfin); elapsed > 500*time.Millisecond {
			t.Fatalf("lead-burst subscriber got prefix+lead in %s, want a burst (<500ms)", elapsed)
		}
	})

	t.Run("a plex viewer on the pump keeps jellyfin paced too", func(t *testing.T) {
		elapsed := timeToLead(t, func(s *Streamer) (<-chan []byte, func()) {
			plex, leavePlex := s.Subscribe("plex")
			go func() {
				for range plex {
				}
			}()
			media, leaveJellyfin := jellyfin(s)
			return media, func() { leaveJellyfin(); leavePlex() }
		})
		if elapsed < time.Second {
			t.Fatalf("mixed pump delivered prefix+lead in %s, want paced (>1s)", elapsed)
		}
	})
}
