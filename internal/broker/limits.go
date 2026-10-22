package broker

import (
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// BucketConfig allows Capacity actions per Per, refilled continuously.
type BucketConfig struct {
	Capacity float64
	Per      time.Duration
}

// Bucket is a token bucket evaluated lazily at the time of each request (no timers).
type Bucket struct {
	cfg    BucketConfig
	tokens float64
	last   time.Time
}

func NewBucket(cfg BucketConfig, now time.Time) *Bucket {
	return &Bucket{cfg: cfg, tokens: cfg.Capacity, last: now}
}

// Take consumes one token if available.
func (b *Bucket) Take(now time.Time) bool {
	if elapsed := now.Sub(b.last); elapsed > 0 {
		b.tokens += elapsed.Seconds() * b.cfg.Capacity / b.cfg.Per.Seconds()
		if b.tokens > b.cfg.Capacity {
			b.tokens = b.cfg.Capacity
		}
		b.last = now
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// Filter blocks messages containing configured terms, after normalizing common evasions:
// case, look-alike digits/symbols (l33t), stretched letters ("baaad"), punctuation inside a word
// ("b.a.d") and single letters separated by spaces ("b a d"). Matching is per word, so a blocked
// term never fires on an innocent word that merely contains it; terms of 5+ letters also match
// inside longer words. Known limit: a one-letter word next to spaced letters joins the run
// ("a b a d" normalizes to "abad"), so such evasions can slip through.
type Filter struct {
	terms []filterTerm
}

type filterTerm struct {
	name  string
	exact *regexp.Regexp
	inner *regexp.Regexp // nil for short terms
}

func NewFilter(terms []string) *Filter {
	f := &Filter{}
	for _, t := range terms {
		n := lettersOnly(t)
		if n == "" {
			continue
		}
		var pat strings.Builder
		for _, r := range n {
			pat.WriteString(regexp.QuoteMeta(string(r)) + "+")
		}
		ft := filterTerm{name: n, exact: regexp.MustCompile("^" + pat.String() + "$")}
		if utf8.RuneCountInString(n) >= 5 {
			ft.inner = regexp.MustCompile(pat.String())
		}
		f.terms = append(f.terms, ft)
	}
	return f
}

var leet = map[rune]rune{'0': 'o', '1': 'i', '3': 'e', '4': 'a', '5': 's', '7': 't', '@': 'a', '$': 's', '!': 'i'}

// lettersOnly lowercases, maps look-alikes and drops everything that is not a letter.
func lettersOnly(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		if m, ok := leet[r]; ok {
			r = m
		}
		if unicode.IsLetter(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// words splits text into normalized words, joining runs of single letters ("b a d" -> "bad").
func words(text string) []string {
	var out []string
	var run strings.Builder
	flush := func() {
		if run.Len() > 0 {
			out = append(out, run.String())
			run.Reset()
		}
	}
	for _, raw := range strings.Fields(text) {
		w := lettersOnly(raw)
		if w == "" {
			continue
		}
		if utf8.RuneCountInString(w) == 1 {
			run.WriteString(w)
			continue
		}
		flush()
		out = append(out, w)
	}
	flush()
	return out
}

// Match reports the first blocked term found in text.
func (f *Filter) Match(text string) (string, bool) {
	for _, w := range words(text) {
		for _, t := range f.terms {
			if t.exact.MatchString(w) || (t.inner != nil && t.inner.MatchString(w)) {
				return t.name, true
			}
		}
	}
	return "", false
}
