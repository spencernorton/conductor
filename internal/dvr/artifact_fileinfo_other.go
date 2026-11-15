//go:build !linux && !darwin

package dvr

import "os"

type artifactChangeVersion struct{}

func artifactFilesystemID(os.FileInfo) (string, bool) {
	return "", false
}

func artifactInodeOf(os.FileInfo) (uint64, bool) {
	return 0, false
}

func artifactChangeVersionOf(os.FileInfo) (artifactChangeVersion, bool) {
	return artifactChangeVersion{}, false
}
