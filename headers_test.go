package main

import (
	"io"
	"log"
	"testing"
)

// The expected values were recorded from the nntp-overview original
// (v0.0.0-20250520152009-98ae78251105) before the copy in headers.go
// replaced it; the copy was compared with it on every header block in the
// gna-clari, usenet-clari and akita-inu corpus as well (10,555,466
// comparisons, lax and strict, none different). The strict columns have
// since changed: the original rejected every Message-ID in strict mode,
// and now one in <> with an "@" and no space passes.
func TestParseHeaderKeysAsOverview(t *testing.T) {
	w := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(w)
	cases := []struct {
		head     []string
		laxID    string
		laxOK    bool
		strictID string
		strictOK bool
		keys     int
	}{
		{nil, "", false, "", false, 0},
		{[]string{}, "", true, "", true, 0},
		{[]string{"Message-ID: <a@b>"}, "<a@b>", true, "<a@b>", true, 1},
		{[]string{"Subject: x", "Message-ID: <a@b>", "Date: today"}, "<a@b>", true, "<a@b>", true, 3},
		{[]string{"Subject: x", "message-id: <a@b>"}, "<a@b>", true, "<a@b>", true, 2},
		{[]string{"Subject: x", "MESSAGE-ID:<a@b>"}, "<a@b>", true, "<a@b>", true, 2},
		{[]string{"Subject: x", "Message-ID: <a b@c>"}, "<a b@c>", true, "", false, 2},
		{[]string{"Subject: x", "Message-ID: <a@b>#1/1"}, "<a@b>", true, "", false, 2},
		{[]string{"Subject: x", "Message-ID: <a@b> (comment)"}, "<a@b>", true, "", false, 2},
		{[]string{"Subject: x", "Message-ID: <a@b>?(x)"}, "<a@b>", true, "", false, 2},
		{[]string{"Subject: x", "Message-ID: <a@b>(UMass-1)"}, "<a@b>", true, "", false, 2},
		{[]string{"Subject: x", "Message-ID: <a@b>-(UMass-1)"}, "<a@b>", true, "", false, 2},
		{[]string{"Subject: x", "Message-ID: a@b"}, "", false, "", false, 0},
		{[]string{"Subject: x", "Message-ID: <a@b"}, "", false, "", false, 0},
		{[]string{"Subject: x", "Message-ID: "}, "", false, "", false, 0},
		{[]string{"Subject: x", "Message-ID:", " <a@b>"}, "<a@b>", true, "<a@b>", true, 2},
		{[]string{"Subject: x", "Message-ID: <a@", " b>"}, "<a@ b>", true, "", false, 2},
		{[]string{"Subject: x", "Message-ID: <a@b>", "Message-ID: <c@d>"}, "<a@b> <c@d>", true, "", false, 3},
		{[]string{"Subject: a", "\tcontinued", "Message-ID: <a@b>", "Path: x"}, "<a@b>", true, "<a@b>", true, 3},
		{[]string{"\tleading continuation", "Message-ID: <a@b>"}, "", false, "", false, 0},
		{[]string{"Subject: x", "no colon", "Message-ID: <a@b>"}, "", false, "", false, 0},
		{[]string{"Subject: x", ":no key", "Message-ID: <a@b>"}, "", false, "", false, 0},
		{[]string{"Subject: x", "x", "Message-ID: <a@b>"}, "", false, "", false, 0},
		{[]string{"Subject: x", "", "Message-ID: <a@b>"}, "", false, "", false, 0},
		{[]string{"Subject: x", "Message-ID: <a@b>", ""}, "", false, "", false, 0},
		{[]string{"Subject: x", "Message-ID: <a@b>", "\tlast continuation"}, "", false, "", false, 0},
		{[]string{"Subject: x", "Message-ID: <msgid@b>"}, "<msgid@b>", true, "<msgid@b>", true, 2},
		{[]string{"Subject: x", "Message-ID: <msgid>"}, "<msgid>", true, "", false, 2},
	}
	for _, c := range cases {
		_, k1, id1, e1 := ParseHeaderKeys(c.head, true)
		_, k2, id2, e2 := ParseHeaderKeys(c.head, false)
		if id1 != c.laxID || (e1 == nil) != c.laxOK || id2 != c.strictID || (e2 == nil) != c.strictOK || max(len(k1), len(k2)) != c.keys {
			t.Errorf("%q: lax %q %v, strict %q %v, keys %d %d", c.head, id1, e1, id2, e2, len(k1), len(k2))
		}
	}
	ids := []struct {
		in       string
		lax      string
		laxOK    bool
		strict   string
		strictOK bool
	}{
		{"", "", false, "", false},
		{"<a@b>", "<a@b>", true, "<a@b>", true},
		{" <a@b> ", "<a@b>", true, "<a@b>", true},
		{"<a b>", "<a b>", true, "", false},
		{"<a@b>#1/1", "<a@b>", true, "", false},
		{"<a@b> (x)", "<a@b>", true, "", false},
		{"<a@b>?(x)", "<a@b>", true, "", false},
		{"<a@b>(UMass-2)", "<a@b>", true, "", false},
		{"a@b", "", false, "", false},
		{"<a@b", "", false, "", false},
		{"<msgid@x>", "<msgid@x>", true, "<msgid@x>", true},
		{"<ab>", "<ab>", true, "", false},
	}
	for _, c := range ids {
		v1, e1 := GetMessageID(c.in, true)
		v2, e2 := GetMessageID(c.in, false)
		if v1 != c.lax || (e1 == nil) != c.laxOK || v2 != c.strict || (e2 == nil) != c.strictOK {
			t.Errorf("GetMessageID(%q): lax %q %v, strict %q %v", c.in, v1, e1, v2, e2)
		}
	}
}
