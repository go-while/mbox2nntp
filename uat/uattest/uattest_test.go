package uattest_test

import (
	"reflect"
	"testing"

	"github.com/go-while/mbox2nntp/uat"
	"github.com/go-while/mbox2nntp/uat/uattest"
)

// What Write writes, uat reads back.
func TestWrite(t *testing.T) {
	zstd := uattest.Zstd(t)
	msgs := []string{"Subject: one\nMessage-ID: <1@x>\n\nbody\n", "Subject: two\nMessage-ID: <2@x>\n\nżółw\n"}
	p, err := uat.Open(uattest.Write(t, zstd, t.TempDir(), "pl.test", "a group", msgs))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	var got []string
	if err := p.Each(zstd, "", func(i int, msg []byte) error {
		got = append(got, string(msg))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if p.Name != "pl.test" || p.DescShort != "a group" || !reflect.DeepEqual(got, msgs) {
		t.Errorf("Name %q, DescShort %q, messages %q", p.Name, p.DescShort, got)
	}
}
