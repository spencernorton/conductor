package epg

import (
	"context"
	"errors"
	"net/url"
	"regexp"
	"sort"
	"strings"

	"github.com/spencercnorton/conductor/internal/store"
)

var (
	urllikePattern     = regexp.MustCompile(`https?://[^\s"'<>]+`)
	malformedUserRegex = regexp.MustCompile(`(https?://)[^/\s@]+@`)
)

var sensitiveQueryKeys = map[string]struct{}{
	"access_token": {},
	"api_key":      {},
	"apikey":       {},
	"auth":         {},
	"cred":         {},
	"credentials":  {},
	"key":          {},
	"pass":         {},
	"passwd":       {},
	"password":     {},
	"pin":          {},
	"pwd":          {},
	"secret":       {},
	"token":        {},
	"user":         {},
	"username":     {},
}

var xtreamPathPrefixes = map[string]struct{}{
	"live":   {},
	"movie":  {},
	"series": {},
	"xmltv":  {},
}

// RedactURL strips embedded credentials from an EPG source URL before it is
// written to logs, persisted status columns, or outbound alert payloads.
func RedactURL(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	u, err := url.Parse(trimmed)
	if err != nil || u.Scheme == "" || u.Host == "" {
		safe := malformedUserRegex.ReplaceAllString(trimmed, "${1}REDACTED@")
		if qIdx := strings.IndexByte(safe, '?'); qIdx >= 0 {
			safe = safe[:qIdx] + "?REDACTED"
		}
		return safe
	}

	hasUser := u.User != nil
	u.User = nil
	u.Path = redactPathSegments(u.Path)
	u.RawPath = ""

	if u.RawQuery != "" {
		q, qErr := url.ParseQuery(u.RawQuery)
		if qErr != nil {
			u.RawQuery = "REDACTED"
		} else {
			u.RawQuery = encodeRedactedQuery(q)
		}
	}
	if u.Fragment != "" {
		u.Fragment = ""
	}

	out := u.String()
	if hasUser {
		prefix := u.Scheme + "://"
		if strings.HasPrefix(out, prefix) {
			out = prefix + "REDACTED@" + out[len(prefix):]
		}
	}
	return out
}

func redactPathSegments(p string) string {
	if p == "" || p == "/" {
		return p
	}
	parts := strings.Split(p, "/")
	// Leading "/" yields parts[0] == "".
	if len(parts) >= 4 {
		first := strings.ToLower(parts[1])
		if _, ok := xtreamPathPrefixes[first]; ok && parts[2] != "" && parts[3] != "" {
			parts[2] = "REDACTED"
			parts[3] = "REDACTED"
			return strings.Join(parts, "/")
		}
	}
	return p
}

func encodeRedactedQuery(q url.Values) string {
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		_, sensitive := sensitiveQueryKeys[strings.ToLower(strings.TrimSpace(k))]
		ek := url.QueryEscape(k)
		if sensitive {
			parts = append(parts, ek+"=REDACTED")
			continue
		}
		vals := q[k]
		if len(vals) == 0 {
			parts = append(parts, ek)
			continue
		}
		for _, v := range vals {
			parts = append(parts, ek+"="+url.QueryEscape(v))
		}
	}
	return strings.Join(parts, "&")
}

// ScrubText replaces occurrences of known raw URLs and any embedded HTTP(S)
// URLs in s with their redacted equivalents.
func ScrubText(s string, rawURLs ...string) string {
	if s == "" {
		return ""
	}
	out := s
	for _, raw := range rawURLs {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		redacted := RedactURL(raw)
		out = strings.ReplaceAll(out, raw, redacted)
		xmlRaw := strings.ReplaceAll(raw, "&", "&amp;")
		if xmlRaw != raw {
			out = strings.ReplaceAll(out, xmlRaw, redacted)
		}
	}
	out = urllikePattern.ReplaceAllStringFunc(out, func(match string) string {
		trimmed := strings.TrimRight(match, ".,;:)]}")
		suffix := match[len(trimmed):]
		return RedactURL(trimmed) + suffix
	})
	return out
}

type scrubbedError struct {
	msg   string
	cause error
}

func (e *scrubbedError) Error() string { return e.msg }
func (e *scrubbedError) Unwrap() error { return e.cause }

// ScrubError returns an error whose string representation has all URL
// credentials redacted while preserving sentinel matching for context and
// store concurrency errors.
func ScrubError(err error, rawURLs ...string) error {
	if err == nil {
		return nil
	}
	var uerr *url.Error
	if errors.As(err, &uerr) && uerr != nil {
		allURLs := append(append([]string(nil), rawURLs...), uerr.URL)
		cleanInner := ScrubError(uerr.Err, allURLs...)
		cleanUErr := &url.Error{
			Op:  uerr.Op,
			URL: RedactURL(uerr.URL),
			Err: cleanInner,
		}
		if err == uerr {
			return cleanUErr
		}
		return &scrubbedError{
			msg:   ScrubText(err.Error(), allURLs...),
			cause: cleanUErr,
		}
	}

	msg := err.Error()
	scrubbed := ScrubText(msg, rawURLs...)
	if scrubbed == msg {
		return err
	}
	var preserved error
	switch {
	case errors.Is(err, store.ErrStaleEPGSnapshot):
		preserved = store.ErrStaleEPGSnapshot
	case errors.Is(err, store.ErrStaleEPGPollAttempt):
		preserved = store.ErrStaleEPGPollAttempt
	case errors.Is(err, store.ErrNotFound):
		preserved = store.ErrNotFound
	case errors.Is(err, context.Canceled):
		preserved = context.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		preserved = context.DeadlineExceeded
	}
	return &scrubbedError{msg: scrubbed, cause: preserved}
}
