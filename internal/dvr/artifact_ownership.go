package dvr

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/spencercnorton/conductor/internal/metrics"
	"github.com/spencercnorton/conductor/internal/store"
)

const (
	artifactVolumeMarkerFile = ".conductor-fs-id"
	maxVolumeMarkerAncestors = 8
)

// CurrentArtifactCheck distinguishes a usable owner from the exact owner this
// call demoted. Demoted is important to a recorder that must refresh its local
// replacement provenance before choosing the no-replace publish primitive;
// Reason preserves the exact deterministic evidence used for that transition.
type CurrentArtifactCheck struct {
	Usable  bool
	Demoted bool
	Reason  string
}

// IndeterminateArtifactInspectionError means filesystem state prevented a
// conclusive integrity decision. Callers must retain the current owner: the
// canonical entry may still be valid, while authorizing a replacement could
// overwrite media that merely lives on a temporarily unavailable mount.
//
// Database and ownership-transition errors deliberately use other types so
// startup can continue past only this safe, fail-closed condition.
type IndeterminateArtifactInspectionError struct {
	RecordingID uuid.UUID
	Path        string
	Operation   string
	Err         error
}

func (e *IndeterminateArtifactInspectionError) Error() string {
	return fmt.Sprintf(
		"indeterminate DVR artifact inspection recording=%s path=%q operation=%q: %v",
		e.RecordingID, e.Path, e.Operation, e.Err)
}

func (e *IndeterminateArtifactInspectionError) Unwrap() error { return e.Err }

// CurrentDVRArtifactReconcileSummary exposes both startup outcomes. Demoted
// owners no longer suppress later airings; Indeterminate owners are retained
// and surfaced for operator action while reconciliation continues.
type CurrentDVRArtifactReconcileSummary struct {
	Demoted                   int
	Indeterminate             int
	InspectionBudgetExhausted bool
	InspectionWorkerMayRemain bool
}

// DefaultStartupDVRArtifactInspectionBudget bounds aggregate startup
// filesystem inspection. Hashing is best-effort at startup; synchronous
// schedule/re-record checks remain unbounded and fail closed.
const DefaultStartupDVRArtifactInspectionBudget = 15 * time.Second

type currentDVRArtifactStore interface {
	ListCurrentDVRArtifacts(context.Context) ([]store.DVRRecording, error)
	BindCurrentDVRArtifactFilesystem(context.Context, uuid.UUID, string, string) error
	DemoteMissingDVRArtifact(context.Context, uuid.UUID, string, string, string) (bool, error)
}

type volumeAliasArtifactStore interface {
	ListDVRFilesystemAliases(context.Context) (map[string]string, error)
	RecordDVRFilesystemAlias(context.Context, string, string, uuid.UUID) error
	MigrateCurrentDVRArtifactFilesystem(context.Context, uuid.UUID, string, string, string, string) error
	UpdateCurrentDVRArtifactStatSig(context.Context, uuid.UUID, string, string, string) error
}

type artifactReadFile interface {
	io.Reader
	Stat() (os.FileInfo, error)
	Close() error
}

type artifactFilesystem interface {
	Stat(string) (os.FileInfo, error)
	Lstat(string) (os.FileInfo, error)
	Open(string) (artifactReadFile, error)
}

type osArtifactFilesystem struct{}

func (osArtifactFilesystem) Stat(path string) (os.FileInfo, error)  { return os.Stat(path) }
func (osArtifactFilesystem) Lstat(path string) (os.FileInfo, error) { return os.Lstat(path) }
func (osArtifactFilesystem) Open(path string) (artifactReadFile, error) {
	return os.Open(path)
}

func parseVolumeMarkerID(raw string) (string, bool) {
	id := strings.TrimSpace(raw)
	if !strings.HasPrefix(id, "vol:") {
		return "", false
	}
	parsed, err := uuid.Parse(strings.TrimPrefix(id, "vol:"))
	if err != nil || parsed == uuid.Nil {
		return "", false
	}
	return "vol:" + parsed.String(), true
}

// EnsureArtifactFilesystemMarker creates or reads a durable volume identity
// marker (<rootDir>/.conductor-fs-id) inside an existing DVR output root.
// Because kernel st_dev minor numbers can shift across host reboots, binding
// mounted DVR storage to an on-volume UUID prevents false filesystem-identity
// mismatches while still failing closed if the volume is unmounted.
func EnsureArtifactFilesystemMarker(rootDir string) (string, error) {
	rootDir = strings.TrimSpace(rootDir)
	if rootDir == "" {
		return "", errors.New("DVR output directory is required")
	}
	info, err := os.Stat(rootDir)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("DVR output path %q is not a directory", rootDir)
	}
	markerPath := filepath.Join(rootDir, artifactVolumeMarkerFile)
	if existing, err := os.ReadFile(markerPath); err == nil {
		if id, ok := parseVolumeMarkerID(string(existing)); ok {
			return id, nil
		}
		return "", fmt.Errorf("invalid DVR volume marker in %q", markerPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}

	candidate := "vol:" + uuid.NewString()
	tmp, err := os.CreateTemp(rootDir, ".conductor-fs-id.tmp.*")
	if err != nil {
		return "", err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()

	if _, err := tmp.WriteString(candidate + "\n"); err != nil {
		_ = tmp.Close()
		return "", err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	_ = os.Chmod(tmpName, 0o644)

	if err := os.Link(tmpName, markerPath); err != nil {
		if errors.Is(err, os.ErrExist) {
			existing, readErr := os.ReadFile(markerPath)
			if readErr != nil {
				return "", readErr
			}
			if id, ok := parseVolumeMarkerID(string(existing)); ok {
				return id, nil
			}
			return "", fmt.Errorf("invalid DVR volume marker in %q", markerPath)
		}
		return "", err
	}
	return candidate, nil
}

// resolveArtifactFilesystemID returns a durable volume marker (vol:<uuid>) when
// an ancestor directory on the exact same st_dev contains .conductor-fs-id;
// otherwise it falls back to the kernel st_dev identity (dev:<hex>). Stopping
// at any st_dev change prevents an unmounted mountpoint or submount from
// inheriting a parent directory's volume marker.
func resolveArtifactFilesystemID(
	targetPath string,
	targetInfo os.FileInfo,
	fsys artifactFilesystem,
) (string, bool) {
	rawID, ok := artifactFilesystemID(targetInfo)
	if !ok {
		return "", false
	}
	if strings.TrimSpace(targetPath) == "" || fsys == nil {
		return rawID, true
	}
	dir := targetPath
	if !targetInfo.IsDir() {
		dir = filepath.Dir(targetPath)
	}
	dir = filepath.Clean(dir)

	for depth := 0; depth < maxVolumeMarkerAncestors; depth++ {
		dirInfo, err := fsys.Stat(dir)
		if err != nil || !dirInfo.IsDir() {
			break
		}
		dirDev, devOK := artifactFilesystemID(dirInfo)
		if !devOK || dirDev != rawID {
			break
		}
		markerPath := filepath.Join(dir, artifactVolumeMarkerFile)
		if markerInfo, mErr := fsys.Stat(markerPath); mErr == nil &&
			markerInfo.Mode().IsRegular() &&
			markerInfo.Size() > 0 && markerInfo.Size() <= 128 {
			if markerDev, mDevOK := artifactFilesystemID(markerInfo); mDevOK && markerDev == rawID {
				if f, openErr := fsys.Open(markerPath); openErr == nil {
					var buf [128]byte
					n, _ := io.ReadFull(f, buf[:])
					_ = f.Close()
					if volID, valid := parseVolumeMarkerID(string(buf[:n])); valid {
						return volID, true
					}
				}
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return rawID, true
}

func artifactStatSignature(info os.FileInfo) (string, bool) {
	ino, inoOK := artifactInodeOf(info)
	chg, chgOK := artifactChangeVersionOf(info)
	if !inoOK || !chgOK {
		return "", false
	}
	return fmt.Sprintf("v1:ino=%x:sz=%d:mt=%d:ct=%d.%d:mode=%o",
		ino, info.Size(), info.ModTime().UnixNano(),
		chg.seconds, chg.nanoseconds, uint32(info.Mode())), true
}

// ArtifactFilesystemIdentity returns the portable durable identity used to
// bind current DVR ownership to the filesystem containing a canonical path.
// It intentionally identifies the volume/device, not the file inode.
func ArtifactFilesystemIdentity(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", errors.New("DVR artifact path is required")
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	filesystemID, ok := resolveArtifactFilesystemID(path, info, osArtifactFilesystem{})
	if !ok {
		return "", errors.New("filesystem device identity is unavailable")
	}
	return filesystemID, nil
}

type currentArtifactInspection struct {
	usable           bool
	demotionReason   string
	filesystemID     string
	statSig          string
	provenLegacyFSID string
	migratedFromFSID string
}

type artifactInspectionOptions struct {
	aliases              map[string]string
	allowStatSigFastPath bool
}

func indeterminateArtifactInspection(
	rec store.DVRRecording,
	operation string,
	err error,
) error {
	if err == nil {
		err = errors.New("filesystem identity or metadata changed during inspection")
	}
	return &IndeterminateArtifactInspectionError{
		RecordingID: rec.ID,
		Path:        rec.OutputPath,
		Operation:   operation,
		Err:         err,
	}
}

func stableArtifactFileInfo(before, after os.FileInfo) bool {
	if !os.SameFile(before, after) ||
		before.Mode() != after.Mode() ||
		before.Size() != after.Size() ||
		!before.ModTime().Equal(after.ModTime()) {
		return false
	}
	beforeChange, beforeOK := artifactChangeVersionOf(before)
	afterChange, afterOK := artifactChangeVersionOf(after)
	return beforeOK && afterOK && beforeChange == afterChange
}

type artifactContextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r artifactContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

// inspectCurrentDVRArtifact performs the filesystem-only half of ownership
// reconciliation. Deterministic absence or integrity mismatch returns a
// demotion reason. Access/read errors and any path or inode race return a
// typed indeterminate error so ownership stays fail-closed.
func inspectCurrentDVRArtifact(
	ctx context.Context,
	rec store.DVRRecording,
	fsys artifactFilesystem,
) (currentArtifactInspection, error) {
	return inspectCurrentDVRArtifactWithOptions(ctx, rec, fsys, artifactInspectionOptions{})
}

func inspectCurrentDVRArtifactWithOptions(
	ctx context.Context,
	rec store.DVRRecording,
	fsys artifactFilesystem,
	opts artifactInspectionOptions,
) (currentArtifactInspection, error) {
	if err := ctx.Err(); err != nil {
		return currentArtifactInspection{}, err
	}
	if rec.ArtifactState == "validated" && len(rec.ArtifactSHA256) != sha256.Size {
		return currentArtifactInspection{}, fmt.Errorf(
			"validated current DVR artifact %s has persisted SHA-256 length %d, want %d",
			rec.ID, len(rec.ArtifactSHA256), sha256.Size)
	}

	pathInfo, statErr := fsys.Stat(rec.OutputPath)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return currentArtifactInspection{}, ctxErr
	}
	observedFilesystemID := ""
	migratedFromFSID := ""
	provingUnverifiedAlias := false
	if statErr == nil {
		filesystemID, ok := resolveArtifactFilesystemID(rec.OutputPath, pathInfo, fsys)
		if !ok {
			return currentArtifactInspection{}, indeterminateArtifactInspection(
				rec, "derive canonical artifact filesystem identity",
				errors.New("filesystem device identity is unavailable"))
		}
		if rec.ArtifactFilesystemID != "" && rec.ArtifactFilesystemID != filesystemID {
			switch {
			case opts.aliases != nil && opts.aliases[rec.ArtifactFilesystemID] == filesystemID:
				migratedFromFSID = rec.ArtifactFilesystemID
			case rec.ArtifactState == "validated" &&
				strings.HasPrefix(rec.ArtifactFilesystemID, "dev:") &&
				strings.HasPrefix(filesystemID, "vol:"):
				// A validated artifact can cryptographically prove that dev:<old>
				// was an earlier st_dev of vol:<uuid>, but ONLY if full SHA-256
				// verification succeeds below.
				provingUnverifiedAlias = true
				migratedFromFSID = rec.ArtifactFilesystemID
			default:
				return currentArtifactInspection{}, indeterminateArtifactInspection(
					rec, "verify canonical artifact filesystem identity",
					fmt.Errorf("filesystem=%q database=%q",
						filesystemID, rec.ArtifactFilesystemID))
			}
		}
		observedFilesystemID = filesystemID
	}
	switch {
	case statErr == nil && !pathInfo.Mode().IsRegular():
		if provingUnverifiedAlias {
			return currentArtifactInspection{}, indeterminateArtifactInspection(
				rec, "verify canonical artifact filesystem identity",
				fmt.Errorf("filesystem=%q database=%q",
					observedFilesystemID, rec.ArtifactFilesystemID))
		}
		return currentArtifactInspection{
			demotionReason: fmt.Sprintf(
				"canonical DVR artifact is not a regular file: mode=%s", pathInfo.Mode()),
			filesystemID:     observedFilesystemID,
			migratedFromFSID: migratedFromFSID,
		}, nil
	case statErr == nil && pathInfo.Size() <= 0:
		if provingUnverifiedAlias {
			return currentArtifactInspection{}, indeterminateArtifactInspection(
				rec, "verify canonical artifact filesystem identity",
				fmt.Errorf("filesystem=%q database=%q",
					observedFilesystemID, rec.ArtifactFilesystemID))
		}
		return currentArtifactInspection{
			demotionReason:   "canonical DVR artifact is empty",
			filesystemID:     observedFilesystemID,
			migratedFromFSID: migratedFromFSID,
		}, nil
	case statErr == nil:
		if rec.ArtifactBytes > 0 && pathInfo.Size() != rec.ArtifactBytes {
			if provingUnverifiedAlias {
				return currentArtifactInspection{}, indeterminateArtifactInspection(
					rec, "verify canonical artifact filesystem identity",
					fmt.Errorf("filesystem=%q database=%q",
						observedFilesystemID, rec.ArtifactFilesystemID))
			}
			return currentArtifactInspection{
				demotionReason: fmt.Sprintf(
					"canonical DVR artifact byte evidence changed: filesystem=%d database=%d",
					pathInfo.Size(), rec.ArtifactBytes),
				filesystemID:     observedFilesystemID,
				migratedFromFSID: migratedFromFSID,
			}, nil
		}
		// A legacy owner has no trustworthy digest. Preserve the migration's
		// deliberately limited size-only behavior until an explicit re-record
		// replaces it with a validated artifact.
		if rec.ArtifactState == "legacy" {
			return currentArtifactInspection{
				usable:           true,
				filesystemID:     observedFilesystemID,
				migratedFromFSID: migratedFromFSID,
			}, nil
		}
		if opts.allowStatSigFastPath && !provingUnverifiedAlias &&
			migratedFromFSID == "" && rec.ArtifactFilesystemID == observedFilesystemID &&
			rec.ArtifactStatSig != "" {
			if sig, sigOK := artifactStatSignature(pathInfo); sigOK && sig == rec.ArtifactStatSig {
				return currentArtifactInspection{
					usable:       true,
					filesystemID: observedFilesystemID,
					statSig:      sig,
				}, nil
			}
		}
	case errors.Is(statErr, os.ErrNotExist):
		// os.Stat also returns ENOENT for a dangling symlink. Its directory entry
		// would make no-replace publication fail, so do not call it absent.
		_, lstatErr := fsys.Lstat(rec.OutputPath)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return currentArtifactInspection{}, ctxErr
		}
		if lstatErr == nil {
			return currentArtifactInspection{}, indeterminateArtifactInspection(
				rec, "resolve canonical filesystem entry",
				errors.New("dangling or unreadable filesystem entry"))
		} else if !errors.Is(lstatErr, os.ErrNotExist) {
			return currentArtifactInspection{}, indeterminateArtifactInspection(
				rec, "inspect canonical filesystem entry", lstatErr)
		}
		// ENOENT is conclusive only when an earlier successful inspection bound
		// this owner to a durable filesystem identity and the containing volume
		// is still reachable on that exact filesystem.
		if rec.ArtifactFilesystemID == "" {
			return currentArtifactInspection{}, indeterminateArtifactInspection(
				rec, "verify missing canonical artifact filesystem identity",
				errors.New("owner has no durable filesystem identity evidence"))
		}
		aliasedVolumeID := ""
		if opts.aliases != nil {
			aliasedVolumeID = opts.aliases[rec.ArtifactFilesystemID]
		}
		parentDir := filepath.Dir(rec.OutputPath)
		parentInfo, parentErr := fsys.Stat(parentDir)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return currentArtifactInspection{}, ctxErr
		}
		// When the owner is bound (directly or via proven alias) to an on-volume
		// vol:<uuid> marker and an importer removed both the media file and its
		// empty leaf folder, walk up to the nearest existing directory; it still
		// proves absence only if that ancestor resolves to the exact vol:<uuid>.
		if errors.Is(parentErr, os.ErrNotExist) &&
			(strings.HasPrefix(rec.ArtifactFilesystemID, "vol:") ||
				strings.HasPrefix(aliasedVolumeID, "vol:")) {
			cur := parentDir
			for depth := 0; depth < maxVolumeMarkerAncestors && errors.Is(parentErr, os.ErrNotExist); depth++ {
				if _, lErr := fsys.Lstat(cur); lErr == nil {
					parentErr = errors.New("dangling symlink on ancestor directory")
					break
				}
				next := filepath.Dir(cur)
				if next == cur {
					break
				}
				cur = next
				parentInfo, parentErr = fsys.Stat(cur)
				if parentErr == nil {
					parentDir = cur
				}
			}
		}
		if parentErr != nil {
			return currentArtifactInspection{}, indeterminateArtifactInspection(
				rec, "verify canonical artifact parent directory", parentErr)
		}
		if !parentInfo.IsDir() {
			return currentArtifactInspection{}, indeterminateArtifactInspection(
				rec, "verify canonical artifact parent directory",
				fmt.Errorf("parent mode=%s", parentInfo.Mode()))
		}
		parentFilesystemID, ok := resolveArtifactFilesystemID(parentDir, parentInfo, fsys)
		if !ok {
			return currentArtifactInspection{}, indeterminateArtifactInspection(
				rec, "derive canonical artifact parent filesystem identity",
				errors.New("filesystem device identity is unavailable"))
		}
		if parentFilesystemID == rec.ArtifactFilesystemID {
			return currentArtifactInspection{
				demotionReason: "canonical DVR artifact is missing from the filesystem",
				filesystemID:   rec.ArtifactFilesystemID,
			}, nil
		}
		if aliasedVolumeID != "" && parentFilesystemID == aliasedVolumeID {
			return currentArtifactInspection{
				demotionReason:   "canonical DVR artifact is missing from the filesystem",
				filesystemID:     aliasedVolumeID,
				migratedFromFSID: rec.ArtifactFilesystemID,
			}, nil
		}
		return currentArtifactInspection{}, indeterminateArtifactInspection(
			rec, "verify missing canonical artifact parent filesystem identity",
			fmt.Errorf("parent=%q database=%q",
				parentFilesystemID, rec.ArtifactFilesystemID))
	default:
		return currentArtifactInspection{}, indeterminateArtifactInspection(
			rec, "stat canonical artifact", statErr)
	}

	file, err := fsys.Open(rec.OutputPath)
	if ctxErr := ctx.Err(); ctxErr != nil {
		if err == nil {
			_ = file.Close()
		}
		return currentArtifactInspection{}, ctxErr
	}
	if err != nil {
		return currentArtifactInspection{}, indeterminateArtifactInspection(
			rec, "open canonical artifact for SHA-256", err)
	}
	defer file.Close()

	openedInfo, err := file.Stat()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return currentArtifactInspection{}, ctxErr
	}
	if err != nil {
		return currentArtifactInspection{}, indeterminateArtifactInspection(
			rec, "inspect opened canonical artifact", err)
	}
	if !stableArtifactFileInfo(pathInfo, openedInfo) {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return currentArtifactInspection{}, ctxErr
		}
		return currentArtifactInspection{}, indeterminateArtifactInspection(
			rec, "verify opened canonical artifact identity", nil)
	}

	hash := sha256.New()
	readBytes, err := io.CopyBuffer(
		hash,
		artifactContextReader{ctx: ctx, r: file},
		make([]byte, 256*1024),
	)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return currentArtifactInspection{}, ctxErr
		}
		return currentArtifactInspection{}, indeterminateArtifactInspection(
			rec, "read canonical artifact for SHA-256", err)
	}
	if err := ctx.Err(); err != nil {
		return currentArtifactInspection{}, err
	}
	if readBytes != pathInfo.Size() {
		return currentArtifactInspection{}, indeterminateArtifactInspection(
			rec, "verify canonical artifact byte count after hashing",
			fmt.Errorf("read=%d stat=%d", readBytes, pathInfo.Size()))
	}

	afterReadInfo, err := file.Stat()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return currentArtifactInspection{}, ctxErr
	}
	if err != nil {
		return currentArtifactInspection{}, indeterminateArtifactInspection(
			rec, "reinspect opened canonical artifact after hashing", err)
	}
	if !stableArtifactFileInfo(openedInfo, afterReadInfo) {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return currentArtifactInspection{}, ctxErr
		}
		return currentArtifactInspection{}, indeterminateArtifactInspection(
			rec, "verify opened canonical artifact stability after hashing", nil)
	}
	afterPathInfo, err := fsys.Stat(rec.OutputPath)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return currentArtifactInspection{}, ctxErr
	}
	if err != nil {
		return currentArtifactInspection{}, indeterminateArtifactInspection(
			rec, "reinspect canonical path after hashing", err)
	}
	if !stableArtifactFileInfo(pathInfo, afterPathInfo) ||
		!os.SameFile(afterReadInfo, afterPathInfo) {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return currentArtifactInspection{}, ctxErr
		}
		return currentArtifactInspection{}, indeterminateArtifactInspection(
			rec, "verify canonical path stability after hashing", nil)
	}

	actualSHA := hash.Sum(nil)
	if !bytes.Equal(actualSHA, rec.ArtifactSHA256) {
		if provingUnverifiedAlias {
			return currentArtifactInspection{}, indeterminateArtifactInspection(
				rec, "verify canonical artifact filesystem identity",
				fmt.Errorf("filesystem=%q database=%q",
					observedFilesystemID, rec.ArtifactFilesystemID))
		}
		return currentArtifactInspection{
			demotionReason: fmt.Sprintf(
				"canonical DVR artifact SHA-256 evidence changed: filesystem=%x database=%x",
				actualSHA, rec.ArtifactSHA256),
			filesystemID:     observedFilesystemID,
			migratedFromFSID: migratedFromFSID,
		}, nil
	}
	sig, _ := artifactStatSignature(afterPathInfo)
	provenLegacy := ""
	if strings.HasPrefix(rec.ArtifactFilesystemID, "dev:") &&
		strings.HasPrefix(observedFilesystemID, "vol:") {
		provenLegacy = rec.ArtifactFilesystemID
	}
	return currentArtifactInspection{
		usable:           true,
		filesystemID:     observedFilesystemID,
		statSig:          sig,
		provenLegacyFSID: provenLegacy,
		migratedFromFSID: migratedFromFSID,
	}, nil
}

// ReconcileCurrentDVRArtifact checks filesystem existence, size, and persisted
// SHA-256 evidence before trusting a validated database current-owner marker.
// Legacy owners retain their migration-era size-only treatment. It deliberately
// demotes only a proven absent/invalid artifact; permission, read, dangling-link,
// and path-race errors remain fail-closed instead of authorizing replacement.
func ReconcileCurrentDVRArtifact(
	ctx context.Context,
	db *store.DB,
	rec store.DVRRecording,
) (CurrentArtifactCheck, error) {
	if db == nil {
		return CurrentArtifactCheck{}, errors.New("DVR artifact database and owner are required")
	}
	return reconcileCurrentDVRArtifactWith(
		ctx, db, rec, osArtifactFilesystem{})
}

func validateCurrentDVRArtifactOwner(rec store.DVRRecording) (bool, error) {
	if rec.ID == uuid.Nil {
		return false, errors.New("DVR artifact owner identity is required")
	}
	if !rec.ArtifactCurrent ||
		(rec.ArtifactState != "legacy" && rec.ArtifactState != "validated") {
		return false, nil
	}
	if strings.TrimSpace(rec.OutputPath) == "" ||
		(rec.ArtifactPath != "" && rec.ArtifactPath != rec.OutputPath) {
		return false, fmt.Errorf(
			"current DVR artifact %s has inconsistent canonical paths output=%q artifact=%q",
			rec.ID, rec.OutputPath, rec.ArtifactPath)
	}
	return true, nil
}

func applyCurrentDVRArtifactInspection(
	ctx context.Context,
	db currentDVRArtifactStore,
	rec store.DVRRecording,
	inspection currentArtifactInspection,
) (CurrentArtifactCheck, error) {
	if strings.TrimSpace(inspection.filesystemID) == "" {
		return CurrentArtifactCheck{}, fmt.Errorf(
			"current DVR artifact %s inspection has no filesystem identity evidence",
			rec.ID)
	}
	aliasStore, _ := db.(volumeAliasArtifactStore)
	if inspection.provenLegacyFSID != "" && aliasStore != nil {
		if err := aliasStore.RecordDVRFilesystemAlias(
			ctx, inspection.provenLegacyFSID, inspection.filesystemID, rec.ID,
		); err != nil {
			return CurrentArtifactCheck{}, err
		}
	}
	if rec.ArtifactFilesystemID == "" {
		if !inspection.usable {
			return CurrentArtifactCheck{}, indeterminateArtifactInspection(
				rec, "verify invalid artifact against durable filesystem identity",
				errors.New("owner has no previously trusted filesystem identity"))
		}
		if err := db.BindCurrentDVRArtifactFilesystem(
			ctx, rec.ID, rec.OutputPath, inspection.filesystemID,
		); err != nil {
			return CurrentArtifactCheck{}, err
		}
		rec.ArtifactFilesystemID = inspection.filesystemID
	} else if rec.ArtifactFilesystemID != inspection.filesystemID {
		if inspection.migratedFromFSID == rec.ArtifactFilesystemID && aliasStore != nil {
			if err := aliasStore.MigrateCurrentDVRArtifactFilesystem(
				ctx, rec.ID, rec.OutputPath, rec.ArtifactFilesystemID,
				inspection.filesystemID, inspection.statSig,
			); err != nil {
				return CurrentArtifactCheck{}, err
			}
			rec.ArtifactFilesystemID = inspection.filesystemID
			if inspection.statSig != "" {
				rec.ArtifactStatSig = inspection.statSig
			}
		} else {
			return CurrentArtifactCheck{}, store.ErrDVRArtifactFilesystemConflict
		}
	}
	if inspection.usable {
		if aliasStore != nil && inspection.statSig != "" && inspection.statSig != rec.ArtifactStatSig {
			if err := aliasStore.UpdateCurrentDVRArtifactStatSig(
				ctx, rec.ID, rec.OutputPath, rec.ArtifactFilesystemID, inspection.statSig,
			); err != nil {
				return CurrentArtifactCheck{}, err
			}
		}
		return CurrentArtifactCheck{Usable: true}, nil
	}
	if strings.TrimSpace(inspection.demotionReason) == "" {
		return CurrentArtifactCheck{}, fmt.Errorf(
			"current DVR artifact %s inspection produced no decision", rec.ID)
	}

	demoted, err := db.DemoteMissingDVRArtifact(
		ctx, rec.ID, rec.OutputPath, rec.ArtifactFilesystemID,
		inspection.demotionReason)
	if err != nil {
		return CurrentArtifactCheck{}, err
	}
	check := CurrentArtifactCheck{Demoted: demoted}
	if demoted {
		check.Reason = inspection.demotionReason
	}
	return check, nil
}

func reconcileCurrentDVRArtifactWith(
	ctx context.Context,
	db currentDVRArtifactStore,
	rec store.DVRRecording,
	fsys artifactFilesystem,
) (CurrentArtifactCheck, error) {
	eligible, err := validateCurrentDVRArtifactOwner(rec)
	if err != nil || !eligible {
		return CurrentArtifactCheck{}, err
	}
	var opts artifactInspectionOptions
	if aliasStore, ok := db.(volumeAliasArtifactStore); ok {
		aliases, aliasErr := aliasStore.ListDVRFilesystemAliases(ctx)
		if aliasErr != nil {
			return CurrentArtifactCheck{}, aliasErr
		}
		opts.aliases = aliases
	}
	inspection, err := inspectCurrentDVRArtifactWithOptions(ctx, rec, fsys, opts)
	if err != nil {
		return CurrentArtifactCheck{}, err
	}
	return applyCurrentDVRArtifactInspection(ctx, db, rec, inspection)
}

// ReconcileCurrentDVRArtifacts runs after runtime ownership and interrupted
// publish recovery at startup. Missing imports cannot remain durable owners
// that suppress every future airing or force replacement publication to fail.
func ReconcileCurrentDVRArtifacts(
	ctx context.Context,
	db *store.DB,
	logger *slog.Logger,
) (CurrentDVRArtifactReconcileSummary, error) {
	if db == nil {
		return CurrentDVRArtifactReconcileSummary{},
			errors.New("DVR artifact database is required")
	}
	return reconcileStartupCurrentDVRArtifacts(
		ctx, db, logger, osArtifactFilesystem{},
		DefaultStartupDVRArtifactInspectionBudget)
}

// reconcileStartupCurrentDVRArtifacts proves legacy dev:* -> vol:<uuid> aliases
// using one SHA-256-verified file per distinct dev:*, immediately reconciles
// stat-only rows (ENOENT missing imports, legacy owners, and stat-sig-matched
// validated owners) so missing imports are never blocked behind multi-gigabyte
// first-time hashing, and then hashes remaining validated owners within budget.
func reconcileStartupCurrentDVRArtifacts(
	ctx context.Context,
	db *store.DB,
	logger *slog.Logger,
	fsys artifactFilesystem,
	budget time.Duration,
) (CurrentDVRArtifactReconcileSummary, error) {
	if budget <= 0 {
		return CurrentDVRArtifactReconcileSummary{},
			errors.New("startup DVR artifact inspection budget must be positive")
	}
	startedAt := time.Now()
	aliases, err := db.ListDVRFilesystemAliases(ctx)
	if err != nil {
		return CurrentDVRArtifactReconcileSummary{}, err
	}
	rows, err := db.ListCurrentDVRArtifacts(ctx)
	if err != nil {
		return CurrentDVRArtifactReconcileSummary{}, err
	}
	if len(rows) == 0 {
		return CurrentDVRArtifactReconcileSummary{}, nil
	}

	// Step 1: Prove at most one existing validated file per distinct unproven
	// legacy dev:* identity so missing/legacy rows sharing that dev:* can be
	// cleanly migrated and demoted immediately.
	aliasBudget := min(5*time.Second, budget/2)
	for _, rec := range rows {
		if time.Since(startedAt) >= aliasBudget {
			break
		}
		if rec.ArtifactState != "validated" ||
			!strings.HasPrefix(rec.ArtifactFilesystemID, "dev:") ||
			aliases[rec.ArtifactFilesystemID] != "" {
			continue
		}
		info, statErr := fsys.Stat(rec.OutputPath)
		if statErr != nil || !info.Mode().IsRegular() || info.Size() != rec.ArtifactBytes {
			continue
		}
		fsID, ok := resolveArtifactFilesystemID(rec.OutputPath, info, fsys)
		if !ok || !strings.HasPrefix(fsID, "vol:") {
			continue
		}
		rem := budget - time.Since(startedAt)
		if rem <= 0 {
			break
		}
		probeCtx, cancel := context.WithTimeout(ctx, rem)
		inspection, inspectErr := inspectCurrentDVRArtifactWithOptions(
			probeCtx, rec, fsys, artifactInspectionOptions{aliases: aliases})
		cancel()
		if inspectErr != nil || !inspection.usable || inspection.provenLegacyFSID == "" {
			continue
		}
		if _, applyErr := applyCurrentDVRArtifactInspection(ctx, db, rec, inspection); applyErr != nil {
			return CurrentDVRArtifactReconcileSummary{}, applyErr
		}
		aliases[inspection.provenLegacyFSID] = inspection.filesystemID
	}

	// Step 2: Fast non-hashing sweep for ENOENT missing files, legacy owners,
	// size mismatches, and already stat-sig-stamped validated owners.
	var fastSummary CurrentDVRArtifactReconcileSummary
	currentRows, err := db.ListCurrentDVRArtifacts(ctx)
	if err != nil {
		return CurrentDVRArtifactReconcileSummary{}, err
	}
	for _, rec := range currentRows {
		eligible, validateErr := validateCurrentDVRArtifactOwner(rec)
		if validateErr != nil {
			return CurrentDVRArtifactReconcileSummary{}, validateErr
		}
		if !eligible {
			continue
		}
		pathInfo, statErr := fsys.Stat(rec.OutputPath)
		needsFullHash := false
		if statErr == nil && pathInfo.Mode().IsRegular() && pathInfo.Size() > 0 &&
			(rec.ArtifactBytes <= 0 || pathInfo.Size() == rec.ArtifactBytes) &&
			rec.ArtifactState == "validated" {
			fsID, ok := resolveArtifactFilesystemID(rec.OutputPath, pathInfo, fsys)
			sig, sigOK := artifactStatSignature(pathInfo)
			if !ok || !sigOK || rec.ArtifactFilesystemID != fsID ||
				rec.ArtifactStatSig == "" || rec.ArtifactStatSig != sig {
				needsFullHash = true
			}
		}
		if needsFullHash {
			continue
		}
		inspection, inspectErr := inspectCurrentDVRArtifactWithOptions(
			ctx, rec, fsys, artifactInspectionOptions{
				aliases:              aliases,
				allowStatSigFastPath: true,
			})
		if inspectErr != nil {
			var indErr *IndeterminateArtifactInspectionError
			if errors.As(inspectErr, &indErr) {
				continue
			}
			return fastSummary, inspectErr
		}
		check, applyErr := applyCurrentDVRArtifactInspection(ctx, db, rec, inspection)
		if applyErr != nil {
			var indErr *IndeterminateArtifactInspectionError
			if errors.As(applyErr, &indErr) {
				continue
			}
			return fastSummary, applyErr
		}
		if check.Demoted {
			fastSummary.Demoted++
			if logger != nil {
				logger.Warn("demoted invalid current DVR artifact",
					"recording_id", rec.ID, "path", rec.OutputPath,
					"artifact_state", rec.ArtifactState,
					"reason", check.Reason)
			}
		}
	}

	remainingBudget := max(50*time.Millisecond, budget-time.Since(startedAt))
	hashSummary, err := reconcileCurrentDVRArtifactsWithOptions(
		ctx, db, logger, fsys, remainingBudget, artifactInspectionOptions{
			aliases:              aliases,
			allowStatSigFastPath: true,
		})
	hashSummary.Demoted += fastSummary.Demoted
	return hashSummary, err
}

type startupArtifactInspectionResult struct {
	index      int
	inspection currentArtifactInspection
	err        error
}

func retainStartupArtifactsAfterInspectionDeadline(
	ctx context.Context,
	logger *slog.Logger,
	rows []store.DVRRecording,
	processed int,
	summary CurrentDVRArtifactReconcileSummary,
	budget time.Duration,
	workerDone <-chan struct{},
) (CurrentDVRArtifactReconcileSummary, error) {
	if err := ctx.Err(); err != nil {
		return summary, err
	}
	remaining := len(rows) - processed
	if remaining < 0 {
		remaining = 0
	}
	summary.Indeterminate += remaining
	summary.InspectionBudgetExhausted = true
	select {
	case <-workerDone:
		// The worker returned before the coordinator observed its deadline.
	default:
		// Never join here: a regular-file syscall can be uninterruptible. The
		// single FS-only worker may remain, but it owns no database capability.
		summary.InspectionWorkerMayRemain = true
		metrics.DVRArtifactStartupInspectionDetachments.Inc()
	}
	if logger != nil && remaining > 0 {
		logger.Warn("retained current DVR artifacts after startup inspection budget exhausted",
			"budget", budget, "retained_count", remaining,
			"inspection_worker_may_remain", summary.InspectionWorkerMayRemain,
			"first_unreconciled_recording_id", rows[processed].ID,
			"first_unreconciled_path", rows[processed].OutputPath)
	}
	return summary, nil
}

func reconcileCurrentDVRArtifactsWith(
	ctx context.Context,
	db currentDVRArtifactStore,
	logger *slog.Logger,
	fsys artifactFilesystem,
	budget time.Duration,
) (CurrentDVRArtifactReconcileSummary, error) {
	return reconcileCurrentDVRArtifactsWithOptions(
		ctx, db, logger, fsys, budget, artifactInspectionOptions{})
}

func reconcileCurrentDVRArtifactsWithOptions(
	ctx context.Context,
	db currentDVRArtifactStore,
	logger *slog.Logger,
	fsys artifactFilesystem,
	budget time.Duration,
	opts artifactInspectionOptions,
) (CurrentDVRArtifactReconcileSummary, error) {
	if budget <= 0 {
		return CurrentDVRArtifactReconcileSummary{},
			errors.New("startup DVR artifact inspection budget must be positive")
	}
	rows, err := db.ListCurrentDVRArtifacts(ctx)
	if err != nil {
		return CurrentDVRArtifactReconcileSummary{}, err
	}
	for _, rec := range rows {
		eligible, validateErr := validateCurrentDVRArtifactOwner(rec)
		if validateErr != nil {
			return CurrentDVRArtifactReconcileSummary{}, validateErr
		}
		if !eligible {
			return CurrentDVRArtifactReconcileSummary{}, fmt.Errorf(
				"database listed non-current DVR artifact %s for startup inspection", rec.ID)
		}
	}
	if len(rows) == 0 {
		return CurrentDVRArtifactReconcileSummary{}, nil
	}

	var workerAliases map[string]string
	if len(opts.aliases) > 0 {
		workerAliases = make(map[string]string, len(opts.aliases))
		for k, v := range opts.aliases {
			workerAliases[k] = v
		}
	}
	workerOpts := artifactInspectionOptions{
		aliases:              workerAliases,
		allowStatSigFastPath: opts.allowStatSigFastPath,
	}

	inspectionCtx, cancelInspection := context.WithTimeout(ctx, budget)
	defer cancelInspection()
	results := make(chan startupArtifactInspectionResult)
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		for index, rec := range rows {
			inspection, inspectErr := inspectCurrentDVRArtifactWithOptions(
				inspectionCtx, rec, fsys, workerOpts)
			select {
			case results <- startupArtifactInspectionResult{
				index: index, inspection: inspection, err: inspectErr,
			}:
			case <-inspectionCtx.Done():
				return
			}
		}
	}()

	var summary CurrentDVRArtifactReconcileSummary
	processed := 0
	for processed < len(rows) {
		if inspectionCtx.Err() != nil {
			return retainStartupArtifactsAfterInspectionDeadline(
				ctx, logger, rows, processed, summary, budget, workerDone)
		}
		select {
		case <-inspectionCtx.Done():
			return retainStartupArtifactsAfterInspectionDeadline(
				ctx, logger, rows, processed, summary, budget, workerDone)
		case result := <-results:
			// A result racing the aggregate deadline must not authorize a late
			// database transition. Retain it and every unstarted row instead.
			if inspectionCtx.Err() != nil {
				return retainStartupArtifactsAfterInspectionDeadline(
					ctx, logger, rows, processed, summary, budget, workerDone)
			}
			if result.index != processed {
				return summary, fmt.Errorf(
					"startup DVR artifact inspection order changed: got=%d want=%d",
					result.index, processed)
			}
			rec := rows[processed]
			if result.err != nil {
				var inspectionErr *IndeterminateArtifactInspectionError
				if errors.As(result.err, &inspectionErr) {
					summary.Indeterminate++
					if logger != nil {
						logger.Warn("retained current DVR artifact after indeterminate inspection",
							"recording_id", rec.ID, "path", rec.OutputPath,
							"artifact_state", rec.ArtifactState, "err", result.err)
					}
					processed++
					continue
				}
				return summary, result.err
			}
			// Only this coordinator performs PostgreSQL ownership transitions.
			// It deliberately uses the parent context, never the expiring
			// filesystem-inspection context, so a timed-out goroutine cannot leave
			// a database mutation running after this function returns.
			check, applyErr := applyCurrentDVRArtifactInspection(
				ctx, db, rec, result.inspection)
			if applyErr != nil {
				var inspectionErr *IndeterminateArtifactInspectionError
				if errors.As(applyErr, &inspectionErr) {
					summary.Indeterminate++
					if logger != nil {
						logger.Warn("retained current DVR artifact after indeterminate inspection",
							"recording_id", rec.ID, "path", rec.OutputPath,
							"artifact_state", rec.ArtifactState, "err", applyErr)
					}
					processed++
					continue
				}
				return summary, applyErr
			}
			if check.Demoted {
				summary.Demoted++
				if logger != nil {
					logger.Warn("demoted invalid current DVR artifact",
						"recording_id", rec.ID, "path", rec.OutputPath,
						"artifact_state", rec.ArtifactState,
						"reason", check.Reason)
				}
			}
			processed++
		}
	}
	return summary, nil
}

// CreateDVRRecordingChecked wraps automated scheduling with filesystem
// reconciliation. The store first discovers any current owner under the
// output-path lock without inserting a runnable successor. After this function
// reconciles that exact owner, the store atomically requires it to remain
// current before insertion. This keeps inspection failures, process exits, and
// owner races from leaving an unverified scheduled row for the DVR scheduler to
// claim.
func CreateDVRRecordingChecked(
	ctx context.Context,
	db *store.DB,
	intent store.DVRRecording,
	policies ...store.DVRAdmissionPolicy,
) (store.DVRRecording, error) {
	var checkedOwner *store.DVRArtifactOwnerProof
	for attempt := 0; attempt < 4; attempt++ {
		rec, err := db.CreateDVRRecordingWithArtifactPreflight(
			ctx, intent, checkedOwner, policies...)
		switch {
		case errors.Is(err, store.ErrDVRArtifactOwnerChanged):
			// Ownership moved after inspection but before the transactional
			// insertion check. Restart against the fresh current owner; the
			// failed attempt inserted nothing.
			checkedOwner = nil
			continue
		case errors.Is(err, store.ErrDVRArtifactOwnerPreflightRequired):
			check, checkErr := ReconcileCurrentDVRArtifact(ctx, db, rec)
			if checkErr != nil {
				return rec, checkErr
			}
			if check.Demoted {
				checkedOwner = nil
				continue
			}
			if !check.Usable {
				return rec, store.ErrArtifactReplacementMismatch
			}
			owner, ownerErr := db.GetDVRRecording(ctx, rec.ID)
			if ownerErr != nil {
				return rec, ownerErr
			}
			proof := store.NewDVRArtifactOwnerProof(owner)
			checkedOwner = &proof
			continue
		default:
			return rec, err
		}
	}
	return store.DVRRecording{}, errors.New("DVR artifact owner changed repeatedly while scheduling")
}
