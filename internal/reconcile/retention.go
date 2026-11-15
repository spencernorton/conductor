package reconcile

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/postprocess"
	"github.com/spencercnorton/conductor/internal/store"
)

const (
	DefaultRetentionInterval     = time.Hour
	DefaultUnrequestedRetention  = 30 * 24 * time.Hour
	DefaultImportedGrace         = 15 * time.Minute
	DefaultDiagnosticRetention   = 24 * time.Hour
	DefaultImportRetryCooldown   = 6 * time.Hour
	artifactVolumeMarkerBasename = ".conductor-fs-id"
)

// RetentionSummary reports the outcome of one DVR retention & import pass.
type RetentionSummary struct {
	PrunedImported     int
	ExpiredUnrequested int
	PreservedRequested int
	PreservedRecent    int
	RetriedImports     int
	CleanedDiagnostics int
	SkippedFailClosed  int
}

// RetentionWorker enforces Conductor's two-tier DVR storage policy:
//
//  1. Recordings requested via Sonarr/Radarr (or currently monitored in
//     Sonarr/Radarr) are preserved on disk until Sonarr/Radarr confirms
//     hasFile=true in the permanent library ( retrying missed import scans as
//     needed), after which the duplicate staging .ts copy in OutputDir is
//     pruned and its dvr_recording row transitions to historical/non-current.
//  2. Recordings NOT requested via Sonarr/Radarr (manual DVR captures,
//     plex-gap captures, or items removed from Sonarr/Radarr) remain on disk
//     for UnrequestedRetention (default 30 days) and are then deleted.
//  3. Abandoned .partial and .superseded.<uuid> diagnostic files older than
//     DiagnosticRetention (default 24h) with no active DB dependency are swept.
type RetentionWorker struct {
	Logger    *slog.Logger
	DB        *store.DB
	Sonarr    *SonarrClient
	Radarr    *RadarrClient
	ArrImport *postprocess.ArrImportStage
	OutputDir string

	Interval             time.Duration
	UnrequestedRetention time.Duration // 0 => DefaultUnrequestedRetention; <0 disables
	ImportedGrace        time.Duration // 0 => DefaultImportedGrace
	DiagnosticRetention  time.Duration // 0 => DefaultDiagnosticRetention; <0 disables
	ImportRetryCooldown  time.Duration // 0 => DefaultImportRetryCooldown
	Now                  func() time.Time

	mu                sync.Mutex
	lastImportAttempt map[uuid.UUID]time.Time
}

func (w *RetentionWorker) now() time.Time {
	if w.Now != nil {
		return w.Now()
	}
	return time.Now()
}

func (w *RetentionWorker) applyDefaults() {
	if w.Logger == nil {
		w.Logger = slog.Default()
	}
	if w.Interval <= 0 {
		w.Interval = DefaultRetentionInterval
	}
	if w.UnrequestedRetention == 0 {
		w.UnrequestedRetention = DefaultUnrequestedRetention
	}
	if w.ImportedGrace == 0 {
		w.ImportedGrace = DefaultImportedGrace
	}
	if w.DiagnosticRetention == 0 {
		w.DiagnosticRetention = DefaultDiagnosticRetention
	}
	if w.ImportRetryCooldown <= 0 {
		w.ImportRetryCooldown = DefaultImportRetryCooldown
	}
}

// Run executes an initial retention pass and then ticks every Interval until
// ctx is cancelled.
func (w *RetentionWorker) Run(ctx context.Context) {
	w.applyDefaults()
	w.Logger.Info("dvr retention worker starting",
		"interval", w.Interval,
		"unrequested_retention", w.UnrequestedRetention,
		"imported_grace", w.ImportedGrace,
		"diagnostic_retention", w.DiagnosticRetention,
		"sonarr", w.Sonarr != nil,
		"radarr", w.Radarr != nil,
		"arr_import_retry", w.ArrImport != nil)

	t := time.NewTicker(w.Interval)
	defer t.Stop()

	_, _ = w.RunOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			w.Logger.Info("dvr retention worker stopping")
			return
		case <-t.C:
			_, _ = w.RunOnce(ctx)
		}
	}
}

type retentionDecision int

const (
	decisionSkipFailClosed retentionDecision = iota
	decisionPruneImported
	decisionKeepRequested
	decisionUnrequestedTTL
)

// RunOnce performs one complete diagnostic sweep + completed-artifact pass.
func (w *RetentionWorker) RunOnce(ctx context.Context) (RetentionSummary, error) {
	w.applyDefaults()
	var sum RetentionSummary
	if strings.TrimSpace(w.OutputDir) == "" || w.DB == nil {
		return sum, nil
	}

	now := w.now()
	sum.CleanedDiagnostics = w.cleanDiagnosticFiles(ctx, now)

	items, err := w.DB.ListCurrentCompletedDVRRecordings(ctx)
	if err != nil {
		w.Logger.Warn("dvr retention: list completed recordings failed", "err", err)
		return sum, err
	}
	if len(items) == 0 {
		return sum, nil
	}

	var eligible []store.DVRCompletedArtifact
	needSonarr, needRadarr := false, false
	for _, item := range items {
		rec := item.Recording
		if rec.CompletedAt == nil || now.Sub(*rec.CompletedAt) < w.ImportedGrace {
			sum.PreservedRecent++
			continue
		}
		eligible = append(eligible, item)
		if rec.IsMovie {
			needRadarr = true
		} else {
			needSonarr = true
		}
	}
	if len(eligible) == 0 {
		return sum, nil
	}

	var (
		radarrMovies []radarrMovie
		radarrErr    error
		sonarrSeries []sonarrSeries
		sonarrErr    error
		episodesByID = make(map[int][]sonarrEpisode)
		epErrByID    = make(map[int]error)
	)
	if needRadarr && w.Radarr != nil {
		radarrMovies, radarrErr = w.Radarr.LibraryMovies(ctx)
		if radarrErr != nil {
			w.Logger.Warn("dvr retention: radarr library read failed; failing closed for movies", "err", radarrErr)
		}
	}
	if needSonarr && w.Sonarr != nil {
		sonarrSeries, sonarrErr = w.Sonarr.LibrarySeries(ctx)
		if sonarrErr != nil {
			w.Logger.Warn("dvr retention: sonarr series read failed; failing closed for episodes", "err", sonarrErr)
		}
	}

	loadEpisodes := func(seriesID int) ([]sonarrEpisode, error) {
		if eps, ok := episodesByID[seriesID]; ok {
			return eps, nil
		}
		if err, ok := epErrByID[seriesID]; ok {
			return nil, err
		}
		eps, err := w.Sonarr.SeriesEpisodes(ctx, seriesID)
		if err != nil {
			epErrByID[seriesID] = err
			w.Logger.Warn("dvr retention: sonarr episodes read failed; failing closed for series",
				"series_id", seriesID, "err", err)
			return nil, err
		}
		episodesByID[seriesID] = eps
		return eps, nil
	}

	activeIDs := make(map[uuid.UUID]struct{}, len(items))
	for _, item := range items {
		activeIDs[item.Recording.ID] = struct{}{}
	}
	w.pruneRetryState(activeIDs)

	for _, item := range eligible {
		if ctx.Err() != nil {
			return sum, ctx.Err()
		}
		rec := item.Recording
		_, folderTitle, folderYear := folderTitleFromOutputPath(w.OutputDir, rec.OutputPath)

		var dec retentionDecision
		var reason string
		if rec.IsMovie {
			dec, reason = evaluateMovieRetention(item, folderTitle, folderYear, w.Radarr != nil, radarrMovies, radarrErr)
		} else {
			dec, reason = evaluateEpisodeRetention(item, folderTitle, folderYear, w.Sonarr != nil, sonarrSeries, sonarrErr, loadEpisodes)
		}

		switch dec {
		case decisionSkipFailClosed:
			sum.SkippedFailClosed++

		case decisionPruneImported:
			pruned, err := w.pruneArtifact(ctx, rec, reason)
			if err != nil {
				w.Logger.Warn("dvr retention: prune imported staging file failed",
					"id", rec.ID, "path", rec.OutputPath, "err", err)
				continue
			}
			if pruned {
				sum.PrunedImported++
			}

		case decisionKeepRequested:
			sum.PreservedRequested++
			if w.maybeRetryArrImport(ctx, rec, now) {
				sum.RetriedImports++
			}

		case decisionUnrequestedTTL:
			if w.UnrequestedRetention < 0 || now.Sub(*rec.CompletedAt) < w.UnrequestedRetention {
				sum.PreservedRecent++
				continue
			}
			pruned, err := w.pruneArtifact(ctx, rec, reason)
			if err != nil {
				w.Logger.Warn("dvr retention: expire unrequested recording failed",
					"id", rec.ID, "path", rec.OutputPath, "err", err)
				continue
			}
			if pruned {
				sum.ExpiredUnrequested++
			}
		}
	}

	if sum.PrunedImported > 0 || sum.ExpiredUnrequested > 0 || sum.RetriedImports > 0 || sum.CleanedDiagnostics > 0 {
		w.Logger.Info("dvr retention pass complete",
			"pruned_imported", sum.PrunedImported,
			"expired_unrequested", sum.ExpiredUnrequested,
			"preserved_requested", sum.PreservedRequested,
			"retried_imports", sum.RetriedImports,
			"cleaned_diagnostics", sum.CleanedDiagnostics,
			"skipped_fail_closed", sum.SkippedFailClosed)
	}
	return sum, nil
}

func evaluateMovieRetention(
	item store.DVRCompletedArtifact,
	folderTitle string,
	folderYear int,
	radarrConfigured bool,
	movies []radarrMovie,
	radarrErr error,
) (retentionDecision, string) {
	rec := item.Recording
	if radarrConfigured {
		if radarrErr != nil {
			return decisionSkipFailClosed, ""
		}
		m, ambiguous := matchRadarrMovie(item, folderTitle, folderYear, movies)
		if ambiguous {
			return decisionKeepRequested, ""
		}
		if m == nil {
			return decisionUnrequestedTTL, "movie not present in Radarr"
		}
		if m.HasFile {
			return decisionPruneImported, "movie confirmed in Radarr library (hasFile=true)"
		}
		if m.Monitored || isArrRequested(rec.RequestedBy) {
			return decisionKeepRequested, ""
		}
		return decisionUnrequestedTTL, "unrequested and unmonitored in Radarr"
	}
	if isArrRequested(rec.RequestedBy) {
		return decisionKeepRequested, ""
	}
	return decisionUnrequestedTTL, "unrequested DVR movie expired"
}

func evaluateEpisodeRetention(
	item store.DVRCompletedArtifact,
	folderTitle string,
	folderYear int,
	sonarrConfigured bool,
	allSeries []sonarrSeries,
	sonarrErr error,
	loadEpisodes func(int) ([]sonarrEpisode, error),
) (retentionDecision, string) {
	rec := item.Recording
	if sonarrConfigured {
		if sonarrErr != nil {
			return decisionSkipFailClosed, ""
		}
		series, ambiguous := matchSonarrSeries(item, folderTitle, folderYear, allSeries)
		if ambiguous {
			return decisionKeepRequested, ""
		}
		if series == nil {
			return decisionUnrequestedTTL, "series not present in Sonarr"
		}
		episodes, err := loadEpisodes(series.ID)
		if err != nil {
			return decisionSkipFailClosed, ""
		}
		ep := matchSonarrEpisode(rec, episodes)
		if ep != nil {
			if ep.HasFile {
				return decisionPruneImported, "episode confirmed in Sonarr library (hasFile=true)"
			}
			if (series.Monitored && ep.Monitored) || isArrRequested(rec.RequestedBy) {
				return decisionKeepRequested, ""
			}
			return decisionUnrequestedTTL, "unrequested and unmonitored in Sonarr"
		}
		if isArrRequested(rec.RequestedBy) {
			return decisionKeepRequested, ""
		}
		return decisionUnrequestedTTL, "unrequested episode not matched in Sonarr"
	}
	if isArrRequested(rec.RequestedBy) {
		return decisionKeepRequested, ""
	}
	return decisionUnrequestedTTL, "unrequested DVR episode expired"
}

func (w *RetentionWorker) pruneArtifact(
	ctx context.Context,
	rec store.DVRRecording,
	reason string,
) (bool, error) {
	busy, err := w.DB.HasActiveDVRDependency(ctx, rec.ID, rec.OutputPath)
	if err != nil {
		return false, err
	}
	if busy {
		return false, nil
	}
	if err := removeArtifactFileAndEmptyParents(w.OutputDir, rec.OutputPath); err != nil {
		return false, err
	}
	archived, err := w.DB.ArchivePrunedDVRArtifact(ctx, rec.ID, rec.OutputPath)
	if err != nil {
		return false, err
	}
	if archived {
		w.Logger.Info("dvr retention: pruned completed artifact",
			"id", rec.ID, "title", rec.Title, "requested_by", rec.RequestedBy,
			"reason", reason, "path", rec.OutputPath)
	}
	return archived, nil
}

func (w *RetentionWorker) maybeRetryArrImport(
	ctx context.Context,
	rec store.DVRRecording,
	now time.Time,
) bool {
	if w.ArrImport == nil {
		return false
	}
	info, err := os.Stat(rec.OutputPath)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 {
		return false
	}
	w.mu.Lock()
	if w.lastImportAttempt == nil {
		w.lastImportAttempt = make(map[uuid.UUID]time.Time)
	}
	if last, ok := w.lastImportAttempt[rec.ID]; ok && now.Sub(last) < w.ImportRetryCooldown {
		w.mu.Unlock()
		return false
	}
	w.lastImportAttempt[rec.ID] = now
	w.mu.Unlock()

	res := w.ArrImport.Run(ctx, &postprocess.Input{
		RecordingID:     rec.ID.String(),
		FilePath:        rec.OutputPath,
		Title:           rec.Title,
		SubTitle:        rec.SubTitle,
		EpisodeOnscreen: rec.EpisodeNumOnscreen,
		IsMovie:         rec.IsMovie,
		BytesWritten:    info.Size(),
	})
	if !res.Success {
		w.Logger.Warn("dvr retention: arr-import retry failed",
			"id", rec.ID, "path", rec.OutputPath, "detail", res.Detail)
		return false
	}
	if strings.HasPrefix(res.Detail, "skipped:") {
		return false
	}
	w.Logger.Info("dvr retention: re-queued arr-import for unimported recording",
		"id", rec.ID, "path", rec.OutputPath, "detail", res.Detail)
	return true
}

func (w *RetentionWorker) pruneRetryState(active map[uuid.UUID]struct{}) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for id := range w.lastImportAttempt {
		if _, ok := active[id]; !ok {
			delete(w.lastImportAttempt, id)
		}
	}
}

func (w *RetentionWorker) cleanDiagnosticFiles(ctx context.Context, now time.Time) int {
	if w.DiagnosticRetention <= 0 {
		return 0
	}
	absRoot, err := filepath.Abs(w.OutputDir)
	if err != nil {
		return 0
	}
	if info, statErr := os.Lstat(absRoot); statErr != nil || !info.IsDir() {
		return 0
	}

	activePaths, err := w.DB.ListActiveDVRArtifactPaths(ctx)
	if err != nil {
		w.Logger.Warn("dvr retention: list active artifact paths failed; skipping diagnostic sweep", "err", err)
		return 0
	}
	activeExact := make(map[string]bool, len(activePaths))
	var activePrefixes []string
	for _, p := range activePaths {
		if ap, absErr := filepath.Abs(p); absErr == nil {
			activeExact[ap] = true
			activePrefixes = append(activePrefixes, ap+".")
		}
	}

	var candidates []string
	_ = filepath.WalkDir(absRoot, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil
		}
		if path != absRoot && d.Type()&os.ModeSymlink != 0 {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		if strings.HasPrefix(name, ".conductor-") {
			return nil
		}
		if !isDiagnosticArtifactName(name) {
			return nil
		}
		info, infoErr := d.Info()
		if infoErr != nil || !info.Mode().IsRegular() {
			return nil
		}
		if now.Sub(info.ModTime()) < w.DiagnosticRetention {
			return nil
		}
		if activeExact[path] {
			return nil
		}
		for _, pref := range activePrefixes {
			if strings.HasPrefix(path, pref) {
				return nil
			}
		}
		candidates = append(candidates, path)
		return nil
	})

	cleaned := 0
	for _, p := range candidates {
		if err := removeArtifactFileAndEmptyParents(absRoot, p); err == nil {
			cleaned++
			w.Logger.Info("dvr retention: removed stale diagnostic artifact", "path", p)
		}
	}
	return cleaned
}

func isDiagnosticArtifactName(name string) bool {
	if strings.HasSuffix(name, ".partial") || strings.HasSuffix(name, ".publish-exchange") {
		return true
	}
	idx := strings.LastIndex(name, ".superseded.")
	if idx <= 0 {
		return false
	}
	_, err := uuid.Parse(name[idx+len(".superseded."):])
	return err == nil
}

func isArrRequested(requestedBy string) bool {
	switch strings.ToLower(strings.TrimSpace(requestedBy)) {
	case "reconcile:sonarr", "reconcile:radarr",
		"torznab:sonarr", "torznab:radarr", "torznab:prowlarr":
		return true
	default:
		return false
	}
}

func matchRadarrMovie(
	item store.DVRCompletedArtifact,
	folderTitle string,
	folderYear int,
	movies []radarrMovie,
) (*radarrMovie, bool) {
	if item.TMDbID > 0 {
		for i := range movies {
			if movies[i].TMDbID == item.TMDbID {
				return &movies[i], false
			}
		}
	}
	if c := canonicalIMDb(item.IMDbID); c != "" {
		for i := range movies {
			if canonicalIMDb(movies[i].IMDbID) == c {
				return &movies[i], false
			}
		}
	}

	for _, candidateTitle := range []string{folderTitle, item.Recording.Title} {
		key := titleKey(candidateTitle)
		if key == "" {
			continue
		}
		var hits []radarrMovie
		for _, m := range movies {
			if radarrTitleMatches(m, key) {
				hits = append(hits, m)
			}
		}
		if len(hits) == 0 {
			continue
		}
		if folderYear > 0 {
			for i := range hits {
				if hits[i].Year == folderYear {
					return &hits[i], false
				}
			}
			continue
		}
		if len(hits) == 1 {
			return &hits[0], false
		}
		allHaveFiles := true
		for i := range hits {
			if !hits[i].HasFile {
				allHaveFiles = false
				break
			}
		}
		if allHaveFiles {
			return &hits[0], false
		}
		return nil, true
	}
	return nil, false
}

func radarrTitleMatches(m radarrMovie, key string) bool {
	if titleKey(m.Title) == key {
		return true
	}
	for _, alt := range m.AlternateTitles {
		if titleKey(alt.Title) == key {
			return true
		}
	}
	return false
}

func matchSonarrSeries(
	item store.DVRCompletedArtifact,
	folderTitle string,
	folderYear int,
	allSeries []sonarrSeries,
) (*sonarrSeries, bool) {
	if item.TVDBID > 0 {
		for i := range allSeries {
			if allSeries[i].TVDBID == item.TVDBID {
				return &allSeries[i], false
			}
		}
	}

	for _, candidateTitle := range []string{folderTitle, item.Recording.Title} {
		key := titleKey(candidateTitle)
		if key == "" {
			continue
		}
		var hits []sonarrSeries
		for _, s := range allSeries {
			if sonarrTitleMatches(s, key) {
				hits = append(hits, s)
			}
		}
		if len(hits) == 0 {
			continue
		}
		if len(hits) == 1 {
			return &hits[0], false
		}
		if folderYear > 0 {
			for i := range hits {
				if hits[i].Year == folderYear {
					return &hits[i], false
				}
			}
		}
		var exact []sonarrSeries
		for _, s := range hits {
			if safeFilename(s.Title) == folderTitle {
				exact = append(exact, s)
			}
		}
		if len(exact) == 1 {
			return &exact[0], false
		}
		return nil, true
	}
	return nil, false
}

func sonarrTitleMatches(s sonarrSeries, key string) bool {
	if titleKey(s.Title) == key {
		return true
	}
	for _, alt := range s.AlternateTitles {
		if titleKey(alt.Title) == key {
			return true
		}
	}
	return false
}

func matchSonarrEpisode(rec store.DVRRecording, episodes []sonarrEpisode) *sonarrEpisode {
	season, episode, dailyDate := parseEpisodeMarkers(rec)
	if dailyDate != "" {
		for i := range episodes {
			if episodes[i].AirDate == dailyDate {
				return &episodes[i]
			}
		}
	}
	if season > 0 && episode > 0 {
		for i := range episodes {
			if episodes[i].SeasonNumber == season && episodes[i].EpisodeNumber == episode {
				return &episodes[i]
			}
		}
	}
	if sub := normalizeTitle(rec.SubTitle); sub != "" {
		var matched *sonarrEpisode
		count := 0
		for i := range episodes {
			if normalizeTitle(episodes[i].Title) == sub {
				matched = &episodes[i]
				count++
			}
		}
		if count == 1 {
			return matched
		}
	}
	return nil
}

func parseEpisodeMarkers(rec store.DVRRecording) (season, episode int, dailyDate string) {
	season, episode = parseOnscreen(rec.EpisodeNumOnscreen)
	base := strings.TrimSuffix(filepath.Base(rec.OutputPath), filepath.Ext(rec.OutputPath))
	for _, tok := range strings.FieldsFunc(base, func(r rune) bool {
		return r == '.' || r == '_' || r == ' '
	}) {
		if dailyDate == "" && isISODate(tok) {
			dailyDate = tok
		}
		if season == 0 && episode == 0 {
			if s, e := parseOnscreen(tok); s > 0 && e > 0 {
				season, episode = s, e
			}
		}
	}
	return season, episode, dailyDate
}

func folderTitleFromOutputPath(rootDir, outputPath string) (mediaClass, folderTitle string, folderYear int) {
	absRoot, err1 := filepath.Abs(rootDir)
	absOut, err2 := filepath.Abs(outputPath)
	if err1 != nil || err2 != nil {
		return "", "", 0
	}
	rel, err := filepath.Rel(absRoot, absOut)
	if err != nil || rel == "." || strings.HasPrefix(rel, "..") {
		return "", "", 0
	}
	parts := strings.Split(rel, string(filepath.Separator))
	if len(parts) < 3 {
		return "", "", 0
	}
	if parts[0] != "TV" && parts[0] != "Movies" {
		return "", "", 0
	}
	folder := parts[1]
	year := parseTrailingYearParens(folder)
	return parts[0], folder, year
}

func parseTrailingYearParens(s string) int {
	s = strings.TrimSpace(s)
	if !hasTrailingYearParens(s) {
		return 0
	}
	n, err := strconv.Atoi(s[len(s)-5 : len(s)-1])
	if err != nil {
		return 0
	}
	return n
}

func removeArtifactFileAndEmptyParents(rootDir, targetPath string) error {
	absRoot, err := filepath.Abs(rootDir)
	if err != nil {
		return err
	}
	absTarget, err := filepath.Abs(targetPath)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(absRoot, absTarget)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("refusing to remove path %q outside DVR output root %q", absTarget, absRoot)
	}
	base := filepath.Base(absTarget)
	if base == artifactVolumeMarkerBasename || strings.HasPrefix(base, ".conductor-") {
		return fmt.Errorf("refusing to remove Conductor volume marker %q", absTarget)
	}

	// Verify no directory component inside absRoot is a symlink escaping root.
	parts := strings.Split(filepath.Dir(rel), string(filepath.Separator))
	cur := absRoot
	for _, p := range parts {
		if p == "" || p == "." {
			continue
		}
		cur = filepath.Join(cur, p)
		di, lErr := os.Lstat(cur)
		if errors.Is(lErr, os.ErrNotExist) {
			break
		}
		if lErr != nil {
			return lErr
		}
		if di.Mode()&os.ModeSymlink != 0 || !di.IsDir() {
			return fmt.Errorf("refusing to traverse non-directory ancestor %q", cur)
		}
	}

	info, err := os.Lstat(absTarget)
	switch {
	case errors.Is(err, os.ErrNotExist):
		// Already absent; still prune empty parent directories if any remain.
	case err != nil:
		return err
	case !info.Mode().IsRegular():
		return fmt.Errorf("refusing to remove non-regular artifact %q (mode %s)", absTarget, info.Mode())
	default:
		if err := os.Remove(absTarget); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}

	for dir := filepath.Dir(absTarget); dir != absRoot && strings.HasPrefix(dir, absRoot+string(filepath.Separator)); dir = filepath.Dir(dir) {
		if err := os.Remove(dir); err != nil {
			break
		}
	}
	return nil
}
