# Changelog

All notable changes to Conductor are recorded here. Versions follow
[semantic versioning](https://semver.org/); each release is a tag on `main`.

## 0.61.2 — 2026-10-30

- A recording gets until shortly after its programme starts to receive
  media, not the 9 seconds a Plex tune gets. The recorder starts before the
  programme and nobody waits on it, so a source that is refused or slow for a
  few seconds no longer loses the whole recording with "upstream did not
  start in time".
- The startup gate's probe interval counts from the end of a probe. When a
  whole-buffer probe took longer than the interval, every following read
  started another probe, which starved a recording attaching to a running
  stream until it was disconnected.
- The startup gate's ffprobe searches the whole buffered prefix for codec
  configuration. A high-bitrate, long-GOP feed joined mid-GOP carries its
  parameter sets past ffprobe's default 5 MB probe window and was refused
  with `codec_configuration_missing` on every probe.

## 0.61.1 — 2026-10-29

- A lost runtime-lock session no longer stops Conductor. The lock's session
  can end because PostgreSQL is briefly unreachable, or because a host memory
  stall times out the 1-second probe, which makes the driver close the
  connection. Conductor now wins the lock back on a new session within a
  minute, and live streams keep playing while it does. It still stops when
  another runtime holds the lock or the database stays unreachable for the
  whole minute. The probe timeout rises from 1 to 5 seconds. A re-acquire is
  logged as a warning with its cause and gap.
- The TVmaze response cache works. The client stores its search and show
  responses in the TVDb cache table under kinds that table's type did not
  allow, so every read and write failed silently and nothing was ever cached.
  Every enrichment pass re-queried TVmaze, and PostgreSQL logged about 4,000
  errors a day. Migration 0028 adds the two kinds.

## 0.61.0 — 2026-10-29

This release also carries 0.60.2, which was versioned but never published.

- Publish the exact runtime-tested container candidate, verifying its checksum,
  source metadata and image ID instead of rebuilding after the tests.

- Require both container variants to start, provision a disposable provider,
  tune and decode real synthetic video/audio, release leases and retain their
  identity and usable credentials after restart. Save runtime diagnostics on
  failure and clean up test resources.

- Clarify the contribution workflow and guide references in the public documentation.

- The commskip post-record stage now uses commskip-auto's batch API: it
  submits the recording as a one-file batch (`POST /api/v1/batches`), polls
  the batch until it finishes, and cancels it if it is still waiting after
  25 minutes so it cannot run against a file the *arr import has moved. The
  old `/api/v1/process` endpoint never existed, so the stage could not work
  before. New `CONDUCTOR_COMMSKIP_DRY_RUN` (default `true`) controls whether
  commskip only reports or actually cuts. The stage stays off unless
  `CONDUCTOR_COMMSKIP_URL` is set.
- The DVR reconciler now cancels a recording it booked from a Sonarr or Radarr
  wanted list once that list stops claiming it — the episode or movie was
  grabbed elsewhere, or unmonitored — instead of recording it anyway. Such a
  recording held a scarce provider slot for its whole window and was then
  thrown away, because the *arr will not import a copy it no longer wants.
  Only a list that was read completely is judged, only airings the guide still
  shows, only recordings more than 15 minutes from their start, and a booking
  must go unclaimed for two consecutive cycles.
- Remember a provider's black "off air" placeholder for a minute per upstream
  URL, shared by every stream pump. Retries, re-tunes and other viewers of an
  off-air channel inside that window reuse the verdict instead of sending
  another panel request, downloading the placeholder again and re-running the
  classifier. One channel left on black used to cost 10-20 provider requests
  a minute, which counts against a provider's per-client rate limit alongside
  every real stream. A channel that comes back on air is noticed within a
  minute.

## 0.60.1 — 2026-09-27

- Establish GitHub pull requests as the development workflow, with privacy checks.
- Add deployment, configuration, security, upgrade and recovery documentation.
- Require quickstart database, encryption and administration secrets and bind
  to loopback initially; provider traffic may use an explicitly configured proxy.

## v0.60.0

- `CONDUCTOR_UPSTREAM_PROXY` sends provider traffic — the panel request, the
  origin it redirects to, and the ppvsync catalogue read — through an HTTP
  proxy, so all of it leaves from one IP. Everything else (Plex, the arrs,
  the ops bot, EPG sources) stays direct. Empty keeps the direct path.

## v0.59.0

- `CONDUCTOR_LOGO_BASE_URL` moves channel logos in the XMLTV guide onto a
  separate origin, so they can be served over HTTPS. The redesigned Plex apps
  load guide images themselves and will not load plain-HTTP LAN URLs, which
  left every self-hosted logo blank. Stream and lineup URLs stay on
  `CONDUCTOR_BASE_URL`. Logos stored as absolute URLs on the base URL are
  rebased too. Changing the setting changes the guide's ETag, so Plex picks
  it up on its next fetch.

## v0.58.3 — first public release

Conductor's first published release. The project has been running in
production since early 2026; this is the point at which the tree became
public, so everything below is what the first public tag contains rather than
a list of changes against an earlier public version.

- HDHomeRun emulator: discovery, lineup and tuner endpoints that Plex pairs
  with as a network tuner.
- Credential-aware stream pool: per-credential slot budgets, transactional
  slot leasing, refcounted upstream sharing, and TCP-close, heartbeat and
  idle-sweep watchdogs for orphaned streams.
- XMLTV guide output with `xmltv_ns` and `onscreen` episode numbers,
  IMDb/TMDb/TVDB ids, repeat and premiere state, self-hosted logos and
  artwork.
- Optional enrichment from TMDb, TVDB, TheSportsDB and Schedules Direct.
- Pay-per-view slot handling: the current tile advertises the next event and
  its local kickoff, and a catalogue gap retains guide presence.
- DVR recording with passthrough (no transcode) and an optional hand-off to
  an external commercial-detection service before import.
- Admin API with Bearer-key and SSO-principal authentication, and AES-GCM
  encryption of provider credentials at rest.
