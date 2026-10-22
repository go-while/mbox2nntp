// Package unpack runs the external programs that unpack what Go's standard
// library cannot read: 7-Zip for .7z, xz for .xz, and zstd for the message
// store of a UAT archive. The module takes no third-party code, so these
// formats are left to the programs. Each is looked up with exec.LookPath,
// which also finds the .exe on Windows.
package unpack

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

// The programs to run, set by -7z, -xz and -zstd. Empty means: look for the
// usual names in PATH.
var SevenZip, Xz, Zstd string

func find(set, flag string, names ...string) (string, error) {
	if set != "" {
		return exec.LookPath(set)
	}
	for _, name := range names {
		if path, err := exec.LookPath(name); err == nil {
			return path, nil
		}
	}
	return "", fmt.Errorf("no %s in PATH: install it, or give its path with %s", strings.Join(names, ", "), flag)
}

// Find7z returns the 7-Zip program: SevenZip, else the first of 7z, 7zz and
// 7za in PATH, and on Windows 7-Zip's install directory, which its installer
// does not add to PATH.
func Find7z() (string, error) {
	names := []string{"7z", "7zz", "7za"}
	if runtime.GOOS == "windows" {
		names = append(names, filepath.Join(os.Getenv("ProgramFiles"), "7-Zip", "7z.exe"))
	}
	path, err := find(SevenZip, "-7z", names...)
	if err != nil && SevenZip == "" {
		return "", errors.New("no 7z, 7zz or 7za in PATH: install 7-Zip, or give its path with -7z")
	}
	return path, err
}

// FindXz returns the xz program: Xz, else xz in PATH.
func FindXz() (string, error) { return find(Xz, "-xz", "xz") }

// FindZstd returns the zstd program: Zstd, else zstd in PATH.
func FindZstd() (string, error) { return find(Zstd, "-zstd", "zstd") }

// Un7zTo extracts the entry named like dst from a 7z archive to dst. It
// lists the archive first, because 7z succeeds and writes nothing when asked
// for a name that is not there, then streams that one entry to dst. "--"
// ends the switches, so an archive named "-x" or "@x" is a file name, and
// -spd turns off wildcards in the entry name. 7z checks the CRC and exits 2
// on a mismatch.
func Un7zTo(archive, dst string) error {
	prog, err := Find7z()
	if err != nil {
		return err
	}
	name := filepath.Base(dst)
	var stderr bytes.Buffer
	list := exec.Command(prog, "l", "-slt", "-spd", "--", archive)
	list.Stderr = &stderr
	out, err := list.Output()
	if err != nil {
		return fmt.Errorf("%s l: %v: %s", prog, err, strings.TrimSpace(stderr.String()))
	}
	if !Has7zEntry(string(out), name) {
		return fmt.Errorf("no entry '%s' in the archive", name)
	}
	return RunTo(dst, prog, "e", "-so", "-bd", "-spd", "--", archive, name)
}

// UnXzTo decompresses an .xz file to dst with xz, or with 7-Zip when there
// is no xz, as on Windows. An xz stream holds one file and no name, so
// there is no entry to look for.
func UnXzTo(archive, dst string) error {
	if prog, err := FindXz(); err == nil {
		return RunTo(dst, prog, "-dc", "--", archive)
	} else if Xz != "" {
		return err
	}
	prog, err := Find7z()
	if err != nil {
		return errors.New("no xz or 7-Zip in PATH: install one, or give its path with -xz or -7z")
	}
	return RunTo(dst, prog, "e", "-so", "-bd", "-txz", "--", archive)
}

// RunTo runs prog with args and writes its standard output to dst. A
// failing program is an error even when it wrote something. When writing
// dst fails, as on a full disk, that is the error: the program is killed
// then, and its "signal: killed" says nothing.
func RunTo(dst, prog string, args ...string) error {
	var stderr bytes.Buffer
	cmd := exec.Command(prog, args...)
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	if werr := WriteTo(dst, stdout); werr != nil {
		cmd.Process.Kill() // it would block on a full pipe
		cmd.Wait()
		return fmt.Errorf("writing %s: %w", dst, werr)
	}
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("%s %s: %v: %s", filepath.Base(prog), args[0], err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// Has7zEntry reports whether the output of "7z l -slt" lists a top-level
// entry called name. Entries follow the "----------" line, one
// "Path = ..." line each; the "Path = " lines above it are the archive's.
func Has7zEntry(listing, name string) bool {
	entries := false
	for _, line := range strings.Split(listing, "\n") {
		line = strings.TrimRight(line, "\r")
		if line == "----------" {
			entries = true
		} else if entries && line == "Path = "+name {
			return true
		}
	}
	return false
}

// WriteTo copies r to a new file dst, replacing one that is there.
func WriteTo(dst string, r io.Reader) error {
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, r); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}
