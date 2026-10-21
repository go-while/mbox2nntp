package main

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

// buildVersion is set by the release build:
//
//	go build -ldflags "-X main.buildVersion=v0.1.0"
//
// iafetch is a module of its own, so that only it depends on SQLite, and
// keeps this copy of mbox2nntp's internal/version instead of requiring
// mbox2nntp. Without -X, versionString falls back to what Go recorded: the
// module version for "go install ...@v0.1.0", else the commit.
var buildVersion = ""

// versionString returns "NAME VERSION (GOVERSION GOOS/GOARCH)".
func versionString(name string) string {
	return fmt.Sprintf("%s %s (%s %s/%s)", name, getVersion(), runtime.Version(), runtime.GOOS, runtime.GOARCH)
}

func getVersion() string {
	if buildVersion != "" {
		return buildVersion
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "dev"
	}
	if v := info.Main.Version; v != "" && v != "(devel)" {
		return v
	}
	rev, dirty := "", false
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			dirty = s.Value == "true"
		}
	}
	if rev == "" {
		return "dev"
	}
	if len(rev) > 12 {
		rev = rev[:12]
	}
	if dirty {
		rev += "+dirty"
	}
	return "dev-" + rev
}
