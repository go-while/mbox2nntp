package main

import (
	"log"
	"sort"
	"strings"

	"github.com/go-while/mbox2nntp/internal/unpack"
	"github.com/go-while/mbox2nntp/uat"
)

// uatSuffix returns the archive suffix of a UAT input: ".usenet", or
// ".usenet.xz" (usenet.nereid.pl) or ".usenet.7z" (archive.org
// usenet-uat-pc-games), and "" for any other input.
func uatSuffix(input string) string {
	for _, s := range []string{".usenet", ".usenet.xz", ".usenet.7z"} {
		if strings.HasSuffix(input, s) {
			return s
		}
	}
	return ""
}

// procUAT feeds the articles of a UAT archive to the workers, like proc
// does for an mbox, and returns the .usenet file it read: for .xz and .7z
// the copy unpacked into unpackDir, which main removes at the end. Each
// message goes through uat.Fix, which declares its UTF-8 body and
// re-encodes non-ASCII header fields; see package uat.
func procUAT(input, suffix string) string {
	zstd, err := unpack.FindZstd() // before anything is unpacked
	if err != nil {
		log.Printf("ERROR: %v", err)
		exit(1)
	}
	path := input
	switch suffix {
	case ".usenet.xz":
		path = unpackOrExit("unxz", unpack.UnXzTo, input, ".xz")
	case ".usenet.7z":
		path = unpackOrExit("un7z", unpack.Un7zTo, input, ".7z")
	}
	p, err := uat.Open(path)
	if err != nil {
		log.Printf("ERROR: %v =exit(1)", err)
		removeUnpacked()
		exit(1)
	}
	defer p.Close() // before main renames or removes it
	log.Printf("[procUAT] '%s' group='%s' version=%d messages=%d", path, p.Name, p.Version, p.Count())

	var nomsgid, binary, empty, encoded, fixedMIME int
	raw := map[string]int{}
	err = p.Each(zstd, tmpdir, func(i int, msg []byte) error {
		head, body, info := uat.Fix(msg)
		encoded += info.Encoded
		for _, name := range info.Raw {
			raw[name]++
		}
		if info.AddedMIMEVersion {
			fixedMIME++
		}
		_, _, msgid, err := ParseHeaderKeys(head, LAXMID)
		if msgid == "" || err != nil {
			nomsgid++
			log.Printf("[procUAT] '%s' message %d: no Message-ID: %v", path, i, err)
			return nil
		}
		if DROPBINARY && hasYEnc(body) {
			binary++
			return nil
		}
		body = body_remove_finalemptylines(body)
		if len(body) == 0 {
			empty++
			return nil
		}
		if (i+1)%10000 == 0 {
			log.Printf("[procUAT] '%s' read %d of %d", path, i+1, p.Count())
		}
		globalCounter.incCounter(0, "msgnums")
		worker_channel <- &ArticleItem{msgid: msgid, head: head, body: body}
		return nil
	})
	for range p.TooBig {
		countTooBig()
	}
	stoppedEarly := err != nil
	if stoppedEarly {
		log.Printf("[procUAT] ERROR: '%s': %v, not marking done", path, err)
	}
	log.Printf("[procUAT] Done read '%s' messages=%d no-msgid=%d binary=%d empty=%d too-big=%d re-encoded-fields=%d added-mime-version=%d stopped-early=%t",
		path, p.Count(), nomsgid, binary, empty, p.TooBig, encoded, fixedMIME, stoppedEarly)
	if len(raw) > 0 {
		names := make([]string, 0, len(raw))
		for name := range raw {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			log.Printf("[procUAT]   %d fields '%s' left with non-ASCII (structured or not parseable)", raw[name], name)
		}
	}
	finishFeeding(stoppedEarly)
	return path
}

// hasYEnc reports a yEnc body, as readBody does for an mbox.
func hasYEnc(body []string) bool {
	for _, line := range body {
		if strings.HasPrefix(line, "=ybegin") {
			return true
		}
	}
	return false
}
