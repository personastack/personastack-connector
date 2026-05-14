package buildinfo

import "strings"

var (
	Version        = "0.1.0-dev"
	GitCommit      = "unknown"
	BuildDate      = "unknown"
	ReleaseChannel = "dev"
)

func VersionString() string {
	return strings.TrimSpace(Version)
}

func GitCommitString() string {
	return strings.TrimSpace(GitCommit)
}

func ReleaseChannelString() string {
	return strings.TrimSpace(ReleaseChannel)
}
