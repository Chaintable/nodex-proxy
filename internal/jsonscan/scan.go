// Package jsonscan walks JSON text to find value boundaries without decoding
// anything. It does not validate: callers either validate the document first
// or tolerate garbage-in. Every access is bounds-checked, so malformed input
// yields ok=false rather than a panic.
package jsonscan

import "strings"

// IsSpace reports whether c is JSON insignificant whitespace.
func IsSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

// SkipSpace returns the index of the first non-whitespace byte at or after i.
func SkipSpace(s string, i int) int {
	for i < len(s) && IsSpace(s[i]) {
		i++
	}
	return i
}

// SkipString expects s[i] == '"' and returns the index just past the closing
// quote. escaped reports whether the string contains any backslash.
func SkipString(s string, i int) (end int, escaped bool, ok bool) {
	end, ok = skipString(s, i)
	if !ok {
		return 0, false, false
	}
	return end, strings.IndexByte(s[i+1:end-1], '\\') >= 0, true
}

func skipString(s string, i int) (end int, ok bool) {
	if i >= len(s) || s[i] != '"' {
		return 0, false
	}
	for j := i + 1; j < len(s); {
		k := strings.IndexByte(s[j:], '"')
		if k < 0 {
			return 0, false
		}
		q := j + k
		backslashes := 0
		for p := q - 1; p > i && s[p] == '\\'; p-- {
			backslashes++
		}
		if backslashes%2 == 0 {
			return q + 1, true
		}
		j = q + 1
	}
	return 0, false
}

// SkipValue returns the index just past the value starting at s[i].
// Scalars other than strings are taken as the run of bytes up to the next
// delimiter; their contents are not checked.
func SkipValue(s string, i int) (end int, ok bool) {
	if i >= len(s) {
		return 0, false
	}
	switch s[i] {
	case '"':
		return skipString(s, i)
	case '{', '[':
		depth := 0
		for j := i; j < len(s); {
			switch s[j] {
			case '"':
				e, ok := skipString(s, j)
				if !ok {
					return 0, false
				}
				j = e
				continue
			case '{', '[':
				depth++
			case '}', ']':
				depth--
				if depth == 0 {
					return j + 1, true
				}
			}
			j++
		}
		return 0, false
	default:
		j := i
		for j < len(s) {
			c := s[j]
			if c == ',' || c == '}' || c == ']' || c == ':' || IsSpace(c) {
				break
			}
			j++
		}
		if j == i {
			return 0, false
		}
		return j, true
	}
}

// ScanObject walks the members of the object starting at s[i] ('{'). For each
// member fn receives the key without quotes, whether the key contains a
// backslash, and the value bounds s[vs:ve]. Returning false from fn stops the
// walk early with ok=true. end is the index just past the closing brace.
func ScanObject(s string, i int, fn func(key string, keyEscaped bool, vs, ve int) bool) (end int, ok bool) {
	if i >= len(s) || s[i] != '{' {
		return 0, false
	}
	j := SkipSpace(s, i+1)
	if j < len(s) && s[j] == '}' {
		return j + 1, true
	}
	for {
		ke, escaped, ok := SkipString(s, j)
		if !ok {
			return 0, false
		}
		key := s[j+1 : ke-1]
		j = SkipSpace(s, ke)
		if j >= len(s) || s[j] != ':' {
			return 0, false
		}
		vs := SkipSpace(s, j+1)
		ve, ok := SkipValue(s, vs)
		if !ok {
			return 0, false
		}
		if !fn(key, escaped, vs, ve) {
			return ve, true
		}
		j = SkipSpace(s, ve)
		if j >= len(s) {
			return 0, false
		}
		switch s[j] {
		case ',':
			j = SkipSpace(s, j+1)
		case '}':
			return j + 1, true
		default:
			return 0, false
		}
	}
}

// ScanArray walks the elements of the array starting at s[i] ('['). fn
// receives the element index and bounds s[vs:ve]; returning false stops the
// walk early with ok=true. end is the index just past the closing bracket, or
// past the last visited element when stopped early.
func ScanArray(s string, i int, fn func(idx, vs, ve int) bool) (end int, ok bool) {
	if i >= len(s) || s[i] != '[' {
		return 0, false
	}
	j := SkipSpace(s, i+1)
	if j < len(s) && s[j] == ']' {
		return j + 1, true
	}
	for idx := 0; ; idx++ {
		ve, ok := SkipValue(s, j)
		if !ok {
			return 0, false
		}
		if !fn(idx, j, ve) {
			return ve, true
		}
		j = SkipSpace(s, ve)
		if j >= len(s) {
			return 0, false
		}
		switch s[j] {
		case ',':
			j = SkipSpace(s, j+1)
		case ']':
			return j + 1, true
		default:
			return 0, false
		}
	}
}

// IsPlainASCII reports whether s holds only printable ASCII, i.e. it decodes
// to itself as JSON string content when it also has no backslash.
func IsPlainASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if c := s[i]; c < 0x20 || c >= 0x7f {
			return false
		}
	}
	return true
}
