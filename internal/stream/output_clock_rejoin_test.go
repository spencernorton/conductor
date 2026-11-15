package stream

import (
	"bytes"
	"errors"
	"os/exec"
	"testing"
	"time"
)

// outputClockDriftGroups is a delivered epoch whose contiguous AAC clock loses
// ground on its video: the audio is aligned at the first group and ends lag
// ticks behind the last picture, as a transcode's output does when it drops
// sub-frame audio or an attempt is cut off mid-interleave.
func outputClockDriftGroups(count int, start, lag int64) [][]byte {
	var groups [][]byte
	audioIndex := int64(0)
	for i := 0; i < count; i++ {
		dts := start + int64(i)*3600
		g := outputClockTestPES(0x100, dts, dts, byte(i), true)
		due := int64(i+1)*3600 - lag*int64(i+1)/int64(count)
		for audioIndex*1920 < due {
			pts := start + audioIndex*1920
			g = append(g, outputClockTestPES(0x101, pts, pts, byte(audioIndex), false)...)
			audioIndex++
		}
		groups = append(groups, g)
	}
	return groups
}

// Recorded refusal loops. Each began when an attempt ended with its delivered
// audio behind its delivered video. Every later attempt, on the same origin and
// on a distinct backup, started with its audio 9-71 ms from its video, and the
// output clock still refused them all until the 90 s reconnect budget ran out.
// The recordings on those channels failed, and so did the slate, which is just
// as aligned. The incoming epochs here start 37 ms apart, the median of the
// 2026-09-26 respawns.
func TestOutputClockAlignedEpochRejoinsDeliveredImbalance(t *testing.T) {
	p := outputClockTestProgram(t)
	for _, tc := range []struct {
		name string
		lag  int64 // delivered audio end behind delivered video end
	}{
		{"cut_mid_interleave_170ms", 15300}, // 2026-09-23, unexpected EOF
		{"cut_mid_interleave_239ms", 21510}, // 2026-09-17, unexpected EOF
		{"output_av_drift_3075ms", 276750},  // 2026-09-26, output A/V drift fault
	} {
		t.Run(tc.name, func(t *testing.T) {
			delivered := outputClockDriftGroups(240, 900000, tc.lag)
			e := newOutputClockEpoch(p, true, false, delivered)
			var c pumpOutputClock
			for _, g := range delivered {
				_, next, err := c.prepare(g, e, true)
				if err != nil {
					t.Fatal(err)
				}
				c = next
			}
			if !c.enabled {
				t.Fatal("delivered epoch did not enable the output clock")
			}
			imbalance := c.maxPTS + c.cadence - c.audioEnd
			if imbalance < tc.lag-1920 || imbalance > tc.lag+1920 {
				t.Fatalf("fixture delivered imbalance=%d ticks, want %d", imbalance, tc.lag)
			}

			for attempt, incoming := range [][][]byte{
				outputClockTestGroups(8, 0, 0, -3330),         // the restarted transcode
				outputClockTestGroups(8, 5_000_000, 0, -3330), // the next one, or the slate
			} {
				ie := newOutputClockEpoch(p, true, false, incoming)
				g, err := inspectOutputClockGroup(incoming[0], p, outputClockNative{})
				if err != nil {
					t.Fatal(err)
				}
				old := c
				_, next, err := c.prepare(incoming[0], ie, true)
				if err != nil {
					t.Fatalf("attempt %d: aligned epoch refused after a %d-tick delivered imbalance: %v", attempt, tc.lag, err)
				}
				// Video continues on the delivered picture grid: no new hole.
				if next.maxPTS != old.maxPTS+old.cadence {
					t.Fatalf("attempt %d: video joined at %d, want %d", attempt, next.maxPTS, old.maxPTS+old.cadence)
				}
				// The audio hole is the delivered imbalance plus the incoming
				// epoch's own 37 ms lead, nothing more.
				if hole := g.firstAudio + next.offset - old.audioEnd; hole != old.maxPTS+old.cadence-old.audioEnd-3330 {
					t.Fatalf("attempt %d: audio hole=%d ticks, want %d", attempt, hole, old.maxPTS+old.cadence-old.audioEnd-3330)
				}
				c = next
				for _, rest := range incoming[1:] {
					if _, next, err = c.prepare(rest, ie, true); err != nil {
						t.Fatal(err)
					}
					c = next
				}
			}
		})
	}
}

// The credit is only for an incoming epoch that is itself aligned: a source
// whose own audio sits seconds from its video must still be refused, however
// far apart the delivered endpoints ended.
func TestOutputClockMisalignedEpochGetsNoImbalanceCredit(t *testing.T) {
	p := outputClockTestProgram(t)
	delivered := outputClockDriftGroups(240, 900000, 276750)
	e := newOutputClockEpoch(p, true, false, delivered)
	var c pumpOutputClock
	for _, g := range delivered {
		_, next, err := c.prepare(g, e, true)
		if err != nil {
			t.Fatal(err)
		}
		c = next
	}
	for _, audioOffset := range []int64{180000, -180000} {
		bad := outputClockTestGroups(8, 0, 0, audioOffset)
		if _, _, err := c.prepare(bad[0], newOutputClockEpoch(p, true, false, bad), true); !errors.Is(err, errOutputClockBoundary) {
			t.Fatalf("epoch with its own %d-tick A/V offset accepted: %v", audioOffset, err)
		}
	}
}

// Bounded media-continuity canary: splice a real FFmpeg-generated H.264/AAC
// MPEG-TS stream whose audio ended ~1.2 s early into a fresh aligned epoch,
// translate through pumpOutputClock, and verify continuous monotonic output
// timestamps and clean FFmpeg remux/decode with zero errors.
func TestOutputClockRealCaptureCanaryDecodesCleanlyAcrossImbalanceRejoin(t *testing.T) {
	ffmpegPath, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("ffmpeg unavailable")
	}
	renderEpoch := func(videoDur, audioDur string) ([]byte, avProgramPIDs, [][]byte) {
		t.Helper()
		cmd := exec.Command(
			ffmpegPath, "-hide_banner", "-loglevel", "error",
			"-f", "lavfi", "-i", "testsrc2=size=320x180:rate=25:duration="+videoDur,
			"-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000:duration="+audioDur,
			"-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p", "-bf", "0", "-g", "25",
			"-c:a", "aac", "-b:a", "96k",
			"-muxdelay", "0", "-muxpreload", "0", "-mpegts_flags", "resend_headers",
			"-f", "mpegts", "pipe:1",
		)
		raw, runErr := cmd.CombinedOutput()
		if runErr != nil {
			t.Fatalf("ffmpeg render (%s/%s): %v\n%s", videoDur, audioDur, runErr, string(raw))
		}
		sel, ready, selErr := selectCheckedInputProgram(raw)
		if selErr != nil || !ready {
			t.Fatalf("selectCheckedInputProgram: ready=%v err=%v", ready, selErr)
		}
		var gate pesPublicationGate
		gate.configure(sel.program, true)
		groups, gateErr := gate.push(raw[sel.start:])
		if gateErr != nil {
			t.Fatalf("pesPublicationGate.push: %v", gateErr)
		}
		tail, tailErr := gate.finishCleanEOF()
		if tailErr != nil {
			t.Fatalf("pesPublicationGate.finishCleanEOF: %v", tailErr)
		}
		groups = append(groups, tail...)
		if len(groups) < 2 {
			t.Fatalf("expected >=2 complete PES groups, got %d", len(groups))
		}
		return append([]byte(nil), sel.prepend...), sel.program, groups
	}

	psi1, prog1, groups1 := renderEpoch("3.0", "1.8")
	_, prog2, groups2 := renderEpoch("2.0", "2.0")
	if prog1.videoPID != prog2.videoPID || prog1.firstAudioPID() != prog2.firstAudioPID() {
		t.Fatalf("PID layout mismatch between FFmpeg epochs: %+v vs %+v", prog1, prog2)
	}

	e1 := newOutputClockEpoch(prog1, true, false, groups1)
	e2 := newOutputClockEpoch(prog1, true, false, groups2)
	if !e1.supported || !e2.supported {
		t.Fatalf("epochs not supported: e1=%+v e2=%+v", e1, e2)
	}

	var c pumpOutputClock
	var joined bytes.Buffer
	joined.Write(psi1)
	for i, g := range groups1 {
		out, next, prepErr := c.prepare(g, e1, true)
		if prepErr != nil {
			t.Fatalf("epoch1 group %d: %v", i, prepErr)
		}
		joined.Write(out)
		c = next
	}
	if !c.enabled {
		t.Fatalf("epoch1 did not enable output clock: %+v", c)
	}
	if deficit := c.maxPTS + c.cadence - c.audioEnd; deficit < int64(800*time.Millisecond/time.Second*90000) {
		// Verify real ~1.2s audio deficit was produced before rejoin.
		if deficit < 70000 {
			t.Fatalf("expected >=70000 ticks of audio deficit before rejoin, got %d", deficit)
		}
	}
	beforeRejoin := c
	for i, g := range groups2 {
		out, next, prepErr := c.prepare(g, e2, true)
		if prepErr != nil {
			t.Fatalf("epoch2 rejoin group %d failed: %v", i, prepErr)
		}
		if i == 0 && (next.lastDTS <= beforeRejoin.lastDTS || next.maxPTS <= beforeRejoin.maxPTS || next.audioEnd <= beforeRejoin.audioEnd) {
			t.Fatalf("rejoin did not advance output floors monotonically: before=%+v after=%+v", beforeRejoin, next)
		}
		joined.Write(out)
		c = next
	}

	// Normalize per-PID MPEG-TS continuity counters across the splice just as
	// subscriber delivery does, then decode with strict FFmpeg error detection.
	wire := append([]byte(nil), joined.Bytes()...)
	ccByPID := map[int]byte{}
	for off := 0; off+tsPacketSize <= len(wire); off += tsPacketSize {
		pkt := wire[off : off+tsPacketSize]
		pid := int(pkt[1]&0x1f)<<8 | int(pkt[2])
		if pid == 0x1fff || pkt[3]&0x10 == 0 {
			continue
		}
		nextCC, seen := ccByPID[pid]
		if seen {
			nextCC = (nextCC + 1) & 0x0f
		} else {
			nextCC = pkt[3] & 0x0f
		}
		ccByPID[pid] = nextCC
		pkt[3] = (pkt[3] & 0xf0) | nextCC
	}
	decodeCmd := exec.Command(
		ffmpegPath, "-hide_banner", "-v", "error", "-xerror",
		"-f", "mpegts", "-i", "pipe:0",
		"-f", "null", "-",
	)
	decodeCmd.Stdin = bytes.NewReader(wire)
	if out, decErr := decodeCmd.CombinedOutput(); decErr != nil {
		t.Fatalf("strict ffmpeg decode across rejoined imbalance failed: %v\n%s", decErr, string(out))
	}
}
