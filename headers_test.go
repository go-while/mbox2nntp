package main

import (
	"reflect"
	"testing"

	overview "github.com/go-while/nntp-overview"
)

// The copy in headers.go must behave exactly as the overview original it
// replaces, for both laxmid settings.
func TestParseHeaderKeysMatchesOverview(t *testing.T) {
	heads := [][]string{
		nil,
		{},
		{"Message-ID: <a@b>"},
		{"Subject: x", "Message-ID: <a@b>", "Date: today"},
		{"Subject: x", "message-id: <a@b>"},
		{"Subject: x", "MESSAGE-ID:<a@b>"},
		{"Subject: x", "Message-ID: <a b@c>"},
		{"Subject: x", "Message-ID: <a@b>#1/1"},
		{"Subject: x", "Message-ID: <a@b> (comment)"},
		{"Subject: x", "Message-ID: <a@b>?(x)"},
		{"Subject: x", "Message-ID: <a@b>(UMass-1)"},
		{"Subject: x", "Message-ID: <a@b>-(UMass-1)"},
		{"Subject: x", "Message-ID: a@b"},
		{"Subject: x", "Message-ID: <a@b"},
		{"Subject: x", "Message-ID: "},
		{"Subject: x", "Message-ID:", " <a@b>"},
		{"Subject: x", "Message-ID: <a@", " b>"},
		{"Subject: x", "Message-ID: <a@b>", "Message-ID: <c@d>"},
		{"Subject: a", "\tcontinued", "Message-ID: <a@b>", "Path: x"},
		{"\tleading continuation", "Message-ID: <a@b>"},
		{"Subject: x", "no colon", "Message-ID: <a@b>"},
		{"Subject: x", ":no key", "Message-ID: <a@b>"},
		{"Subject: x", "x", "Message-ID: <a@b>"},
		{"Subject: x", "", "Message-ID: <a@b>"},
		{"Subject: x", "Message-ID: <a@b>", ""},
		{"Subject: x", "Message-ID: <a@b>", "\tlast continuation"},
		{"Subject: x", "Message-ID: <msgid@b>"},
		{"Subject: x", "Message-ID: <msgid>"},
	}
	for _, lax := range []bool{true, false} {
		for _, h := range heads {
			m1, k1, id1, err1 := ParseHeaderKeys(h, lax)
			m2, k2, id2, err2 := overview.ParseHeaderKeys(h, lax)
			if !reflect.DeepEqual(m1, m2) || !reflect.DeepEqual(k1, k2) || id1 != id2 || (err1 == nil) != (err2 == nil) || (err1 != nil && err1.Error() != err2.Error()) {
				t.Errorf("lax=%t head=%q:\n copy     %q %q %q %v\n overview %q %q %q %v", lax, h, m1, k1, id1, err1, m2, k2, id2, err2)
			}
		}
		for _, id := range []string{"", "<a@b>", " <a@b> ", "<a b>", "<a@b>#1/1", "<a@b> (x)", "<a@b>?(x)", "<a@b>(UMass-2)", "a@b", "<a@b", "<msgid@x>"} {
			v1, e1 := GetMessageID(id, lax)
			v2, e2 := overview.GetMessageID(id, lax)
			if v1 != v2 || (e1 == nil) != (e2 == nil) {
				t.Errorf("GetMessageID(%q, %t): copy %q %v, overview %q %v", id, lax, v1, e1, v2, e2)
			}
		}
	}
}
