package main

// ParseHeaderKeys and GetMessageID are copied from
// github.com/go-while/nntp-overview v0.0.0-20250520152009-98ae78251105
// (overview.go), unchanged apart from the strict-mode fix in GetMessageID.
// They were the only code mbox2nntp used from that package, which pulls in
// nntp-storage, nntp-mmap, mysql and redis, and does not build on Windows. TestParseHeaderKeysAsOverview holds the copy to the
// original's recorded behaviour, strict mode excepted.

import (
	"fmt"
	"log"
	"strings"
)

func ParseHeaderKeys(head []string, laxmid bool) (headermap map[string][]string, keysorder []string, msgid string, err error) {
	/*  RFC 5322 section 2.2:
	Header fields are lines beginning with a field name, followed by a
	colon (":"), followed by a field body, and terminated by CRLF.
	*
	* A field name MUST be composed of printable US-ASCII characters (i.e.,
	characters that have values between 33 and 126, inclusive), except
	colon.
	*
	* A field body may be composed of printable US-ASCII characters
	as well as the space (SP, ASCII value 32) and horizontal tab (HTAB,
	ASCII value 9) characters (together known as the white space
	characters, WSP).  A field body MUST NOT include CR and LF except
	when used in "folding" and "unfolding", as described in section
	2.2.3.  All field bodies MUST conform to the syntax described in
	sections 3 and 4 of this specification.
	*/
	if head == nil {
		return nil, nil, "", fmt.Errorf("Error ParseHeaderKeys: head=nil")
	}

	key := ""
	//msgid = ""
	headermap = make(map[string][]string)
	keysorder = []string{}
	ignore_nextline := false
	for i, line := range head {
		if len(line) < 2 {
			log.Printf("WARN Error ParseHeaderKeys: Header attribute expected i=%d head=%d line='%s'", i, len(head), line)
			return nil, nil, "", fmt.Errorf("Error ParseHeaderKeys: Header attribute expected i=%d head=%d line='%s'", i, len(head), line)
		}
		spaced_nextline := false
		nextline := i + 1
		if nextline < len(head)-1 {
			if head[nextline] != "" {
				//str_nextline = head[nextline] // next line as string
				spaced_nextline = isspace(head[nextline][0]) // first char of next line
			}
		}

		if ignore_nextline {
			if spaced_nextline {
				ignore_nextline = true
			} else {
				ignore_nextline = false
			}
			continue
		}

		if isspace(line[0]) && key != "" {
			headermap[key] = append(headermap[key], line)
			continue
		} else if isspace(line[0]) && key == "" {
			return nil, nil, "", fmt.Errorf("Error ParseHeaderKeys: Unexpected continuation of a header somehow missed line='%s' i=%d head=%d key='%s'", line, i, len(head), key)
		}
		k := strings.Index(line, ":")
		if k < 1 {
			return nil, nil, "", fmt.Errorf("Error ParseHeaderKeys: Colon not found in expected 'key: value' syntax | line='%s'", line)
		}

		key = string(line[0:k])
		value := string(line[k+1:])

		/*
			for j, c := range value {
				if c == 32 || c == 9 {
					continue
				}
				if c < 32 || c > 126 || c == ':' {
					return nil, nil, "", fmt.Errorf("Error ParseHeaderKeys: value 'c < 32 || c > 126 || ';'' value='%s' line='%s' line_i=%d idx=j=%d c=num=%d", value, line, i, j, c)
				}
			}
		*/
		/*
			for j, c := range value {
				if c < 32 && c != 9 {
					return nil, nil, "", fmt.Errorf("Error ParseHeaderKeys: invalid value char line='%s' i=%d j=%d c=%d", line, i, j, c)
				}
			}
		*/

		headermap[key] = append(headermap[key], value)
		keysorder = append(keysorder, key)
	}
	msgid_key := ""

	for key, values := range headermap {
		if strings.ToLower(key) == "message-id" {
			if len(values) > 0 {
				if len(values) > 1 {
					msgid = strings.TrimSpace(strings.Join(values, ""))
				} else {
					msgid = values[0]
				}

				msgid_key = key
				if msgid != "" {
					msgid, err = GetMessageID(msgid, laxmid)
					if err != nil || msgid == "" {
						return nil, nil, "", fmt.Errorf("Error ParseHeaderKeys getMessageID key='%s' val='%#v'", key, values)
					}
					break
				}
			} else {
				log.Printf("Error ParseHeaderKeys getMessageID key='%s' no value", key)
			}
		}
	}

	if msgid != "" && len(headermap[msgid_key]) > 1 {
		headermap[msgid_key] = nil
		headermap[msgid_key] = append(headermap[msgid_key], msgid)
	}
	//_ = ConstructHeader(headermap, &keysorder)
	//log.Printf("ParseHeaderKeys: returns map=%d ord=%d \n v='%v'\n", len(headermap), len(keysorder), headermap)
	return
} // end func ParseHeaderKeys

func GetMessageID(amsgid string, laxmid bool) (string, error) {
	//lastInd := -1
	msgid := strings.TrimSpace(amsgid)
	containsSpace := strings.Contains(msgid, " ")
	//containsAT := strings.Contains(msgid, "@")

	if laxmid && containsSpace {
		if strings.HasPrefix(msgid, "<") && strings.HasSuffix(msgid, ">") {
			return msgid, nil
		}
	} else if !laxmid && containsSpace {
		return "", fmt.Errorf("Error GetMessageID msgid='%s' laxmid=%t containsSpace=%t", msgid, laxmid, containsSpace)
	}

	if laxmid && strings.HasSuffix(msgid, ">#1/1") {
		lastInd := strings.LastIndex(msgid, "#")
		if lastInd > 0 {
			msgid = string(msgid[:lastInd])
		}
	} else if laxmid && strings.HasPrefix(msgid, "<") && (strings.Contains(msgid, ">?(") || strings.Contains(msgid, "> (") || strings.Contains(msgid, ">(UMass-") || strings.Contains(msgid, ">-(UMass-")) && strings.HasSuffix(msgid, ")") {
		split := strings.Split(msgid, ">")
		msgid = split[0] + ">"
	}

	if laxmid {
		if strings.HasPrefix(msgid, "<") && strings.HasSuffix(msgid, ">") {
			return msgid, nil
		}
	} else if !laxmid {
		// The original tested the literal "msgid" for the "@", so without
		// laxmid it accepted nothing.
		if strings.HasPrefix(msgid, "<") && strings.Contains(msgid, "@") && strings.HasSuffix(msgid, ">") {
			return msgid, nil
		}
	}
	return "", fmt.Errorf("ERROR: getMessageID failed amsgid='%s'", amsgid)
} // end func GetMessageID
