package main

import (
	"bytes"
	"io"
	"os"
)

// isMboxText tells whether a .txt input is an mbox: its first line must
// begin with "From ". archive.org items made from Google Groups
// (comp_sys_atari and its siblings) hold the group as NAME.txt; many other
// .txt files on archive.org are listings or notes, which are refused.
func isMboxText(path string) (bool, error) {
	fh, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer fh.Close()
	head := make([]byte, 5)
	if _, err := io.ReadFull(fh, head); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return false, nil
		}
		return false, err
	}
	return bytes.Equal(head, []byte("From ")), nil
}
