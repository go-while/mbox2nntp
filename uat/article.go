package uat

import (
	"mime"
	"net/mail"
	"regexp"
	"strings"
)

// Fix turns a message as the toolkit stores it into an article that can go
// on the wire. The toolkit's import (utf8ize) rewrote every message:
//
//   - header fields are decoded, so RFC 2047 encoded-words became raw UTF-8;
//   - the body is the first text/plain part (else an HTML part), decoded
//     from its transfer encoding and converted to UTF-8; other parts are gone;
//   - Content-Type and Content-Transfer-Encoding were dropped, while
//     MIME-Version mostly stayed.
//
// So Fix declares the body: Content-Type: text/plain; charset=UTF-8, with
// Content-Transfer-Encoding 8bit (7bit for a body in plain ASCII), and
// MIME-Version: 1.0 where it is missing. Header fields holding non-ASCII
// text are encoded again as RFC 2047 encoded-words:
//
//   - unstructured fields (Subject, Organization, X-..., and any field not
//     known as structured): the runs of words with non-ASCII in them;
//   - address fields (From, Reply-To, Sender, To, Cc, Approved, ...): the
//     display names; the addresses are left alone;
//   - structured fields (Message-ID, References, Newsgroups, Path, Date, ...)
//     are never changed. One with non-ASCII in it stays as it is and is
//     named in Info.Raw.
//
// Fields in plain ASCII are kept byte for byte. head has one entry per
// line, continuation lines starting with a space or tab; body has one entry
// per line. Neither has line endings.
func Fix(msg []byte) (head, body []string, info Info) {
	h, b := split(string(msg))
	fields := parseFields(h)
	hasMIME := false
	out := make([]string, 0, len(fields)+3)
	for _, f := range fields {
		switch lname := strings.ToLower(f.name); lname {
		case "content-type", "content-transfer-encoding":
			continue // describe the original body, not the stored one
		case "mime-version":
			hasMIME = true
		}
		out = append(out, f.fix(&info)...)
	}
	if !hasMIME {
		out = append(out, "MIME-Version: 1.0")
		info.AddedMIMEVersion = true
	}
	cte := "7bit"
	if !isASCII(b) {
		cte = "8bit"
	}
	out = append(out, "Content-Type: text/plain; charset=UTF-8", "Content-Transfer-Encoding: "+cte)
	return out, Lines(b), info
}

// Info says what Fix changed.
type Info struct {
	Encoded          int      // header fields that got RFC 2047 encoded-words
	Raw              []string // fields left with non-ASCII in them: structured, or not parseable
	AddedMIMEVersion bool
}

// split cuts a stored message into header block and body at the first
// empty line.
func split(msg string) (head, body string) {
	if i := strings.Index(msg, "\n\n"); i >= 0 {
		return msg[:i], msg[i+2:]
	}
	return strings.TrimSuffix(msg, "\n"), ""
}

// Lines splits text into lines without their line endings ("\n", and a
// "\r" before it). A final line ending does not start another line.
func Lines(text string) []string {
	if text == "" {
		return nil
	}
	lines := strings.Split(strings.TrimSuffix(text, "\n"), "\n")
	for i, l := range lines {
		lines[i] = strings.TrimSuffix(l, "\r")
	}
	return lines
}

type field struct {
	name  string
	lines []string // the field as stored: "Name: value", then continuation lines
}

func parseFields(head string) []field {
	var fields []field
	for _, line := range Lines(head) {
		if (line == "" || line[0] == ' ' || line[0] == '\t') && len(fields) > 0 {
			last := &fields[len(fields)-1]
			last.lines = append(last.lines, line)
			continue
		}
		name, _, _ := strings.Cut(line, ":")
		fields = append(fields, field{name: name, lines: []string{line}})
	}
	return fields
}

// The fields whose syntax encoded-words would break. Never re-encoded.
var structured = map[string]bool{
	"message-id": true, "references": true, "in-reply-to": true, "supersedes": true,
	"newsgroups": true, "followup-to": true, "path": true, "xref": true, "date": true,
	"expires": true, "control": true, "distribution": true, "lines": true,
	"nntp-posting-date": true, "nntp-posting-host": true, "injection-date": true,
	"injection-info": true, "date-received": true, "posted": true, "relay-version": true,
	"article-i.d.": true, "mime-version": true, "content-id": true, "content-type": true,
	"content-transfer-encoding": true, "content-disposition": true, "x-trace": true,
}

// The fields that hold addresses: only their display names are encoded.
var addressFields = map[string]bool{
	"from": true, "reply-to": true, "sender": true, "to": true, "cc": true, "bcc": true,
	"approved": true, "mail-copies-to": true, "x-original-from": true, "x-complaints-to": true,
}

func (f field) fix(info *Info) []string {
	value := strings.Join(f.lines, "")
	if isASCII(value) {
		return f.lines
	}
	name, rest, ok := strings.Cut(value, ":")
	lname := strings.ToLower(name)
	if !ok || !isASCII(name) || structured[lname] {
		info.Raw = append(info.Raw, name)
		return f.lines
	}
	rest = strings.TrimLeft(rest, " \t")
	var enc string
	if addressFields[lname] {
		enc, ok = encodeAddresses(rest)
	} else {
		enc, ok = encodeText(rest), true
	}
	if !ok || !isASCII(enc) {
		info.Raw = append(info.Raw, name)
		return f.lines
	}
	info.Encoded++
	return fold(name + ": " + enc)
}

// encodeText encodes the runs of words that hold non-ASCII. A run takes in
// the space between its words, because the white space between two
// adjacent encoded-words disappears when they are decoded.
func encodeText(s string) string {
	var b strings.Builder
	tokens := wordsAndSpaces(s)
	for i := 0; i < len(tokens); i++ {
		if isASCII(tokens[i]) {
			b.WriteString(tokens[i])
			continue
		}
		j := i // the run ends at the last non-ASCII word before an ASCII word
		for k := i + 2; k < len(tokens) && !isASCII(tokens[k]); k += 2 {
			j = k
		}
		b.WriteString(mime.QEncoding.Encode("UTF-8", strings.Join(tokens[i:j+1], "")))
		i = j
	}
	return b.String()
}

// wordsAndSpaces splits s into alternating runs of non-space and space,
// so that the pieces joined give s back.
func wordsAndSpaces(s string) []string {
	var tokens []string
	start := 0
	for i := 1; i <= len(s); i++ {
		if i == len(s) || isSpace(s[i]) != isSpace(s[start]) {
			tokens = append(tokens, s[start:i])
			start = i
		}
	}
	return tokens
}

var (
	nameAddr    = regexp.MustCompile(`^\s*(.*?)\s*<([^<>]*)>\s*$`)
	addrComment = regexp.MustCompile(`^\s*([^\s()<>]+)\s*\((.*)\)\s*$`)
)

// encodeAddresses encodes the display names in an address list. net/mail
// parses most fields; Address.String puts the name in an encoded-word. The
// two shapes it may refuse are handled on their own: "Name <addr>", whose
// name may hold characters a phrase must not, and the old "addr (Name)".
func encodeAddresses(s string) (string, bool) {
	if list, err := mail.ParseAddressList(s); err == nil {
		parts := make([]string, len(list))
		for i, a := range list {
			parts[i] = a.String()
		}
		return strings.Join(parts, ", "), true
	}
	if m := nameAddr.FindStringSubmatch(s); m != nil && isASCII(m[2]) {
		name := strings.TrimSpace(m[1])
		if len(name) >= 2 && name[0] == '"' && name[len(name)-1] == '"' {
			name = name[1 : len(name)-1] // an encoded-word in quotes is not decoded
		}
		return mime.QEncoding.Encode("UTF-8", name) + " <" + m[2] + ">", true
	}
	if m := addrComment.FindStringSubmatch(s); m != nil && isASCII(m[1]) {
		// B, not Q: a comment must not hold "(", ")" or '"', which Q leaves as is
		return m[1] + " (" + mime.BEncoding.Encode("UTF-8", m[2]) + ")", true
	}
	return "", false
}

// fold breaks a header line longer than 78 bytes at spaces, into
// continuation lines that start with the space. A line keeps at least one
// word: the first never ends right after "Name:", and a word longer than
// the width stays whole.
func fold(line string) []string {
	const width = 78
	var lines []string
	min := strings.Index(line, ": ") + 2 // where the value starts
	for len(line) > width {
		cut := strings.LastIndexByte(line[:width], ' ')
		if cut <= min {
			i := strings.IndexByte(line[min+1:], ' ')
			if i < 0 {
				break
			}
			cut = min + 1 + i
		}
		if strings.TrimSpace(line[cut:]) == "" {
			break
		}
		lines = append(lines, line[:cut])
		line = line[cut:]
		min = 1 // past the space a continuation line starts with
	}
	return append(lines, line)
}

func isSpace(c byte) bool { return c == ' ' || c == '\t' }

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}
