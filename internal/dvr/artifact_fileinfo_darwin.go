//go:build darwin

package dvr

import (
	"fmt"
	"os"
	"syscall"
)

type artifactChangeVersion struct {
	seconds     int64
	nanoseconds int64
}

func artifactFilesystemID(info os.FileInfo) (string, bool) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat == nil {
		return "", false
	}
	return fmt.Sprintf("dev:%x", uint64(uint32(stat.Dev))), true
}

func artifactInodeOf(info os.FileInfo) (uint64, bool) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat == nil {
		return 0, false
	}
	return uint64(stat.Ino), true
}

func artifactChangeVersionOf(info os.FileInfo) (artifactChangeVersion, bool) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat == nil {
		return artifactChangeVersion{}, false
	}
	return artifactChangeVersion{
		seconds:     stat.Ctimespec.Sec,
		nanoseconds: stat.Ctimespec.Nsec,
	}, true
}
