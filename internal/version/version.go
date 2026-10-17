// Package version says which release a binary is.
package version

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

// Version is set by the release build:
//
//	go build -ldflags "-X github.com/go-while/mbox2nntp/internal/version.Version=v0.1.0"
//
// Without it, String falls back to what Go recorded: the module version for
// "go install github.com/go-while/mbox2nntp@v0.1.0", else the commit.
var Version = ""

// String returns "NAME VERSION (GOVERSION GOOS/GOARCH)".
func String(name string) string {
	return fmt.Sprintf("%s %s (%s %s/%s)", name, get(), runtime.Version(), runtime.GOOS, runtime.GOARCH)
}

func get() string {
	if Version != "" {
		return Version
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
