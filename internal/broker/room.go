package broker

import (
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/aaryanpallath/chorus/internal/protocol"
)

// MaxMessageRunes is the longest chat message accepted.
const MaxMessageRunes = 500

// Limits for the per-user token bucket. Moderators get a higher allowance, as on most chat platforms.
var (
	viewerLimit = BucketConfig{Capacity: 20, Per: 30 * time.Second}
	modLimit    = BucketConfig{Capacity: 100, Per: 30 * time.Second}
)

// Room is the authoritative state of one channel. A channel lives on exactly one broker, which
// makes every rule decision (slow mode, bans, rate limits) and assigns the sequence number, so
// all edges agree without coordinating with each other. Room is not safe for concurrent use; the
// broker serializes access per channel.
type Room struct {
	Name        string
	seq         uint64
	slow        time.Duration
	lastMessage map[string]time.Time
	timeouts    map[string]time.Time // user -> muted until
	bans        map[string]bool
	buckets     map[string]*Bucket
	filter      *Filter
}

func NewRoom(name string, filter *Filter) *Room {
	return &Room{
		Name: name, lastMessage: map[string]time.Time{}, timeouts: map[string]time.Time{},
		bans: map[string]bool{}, buckets: map[string]*Bucket{}, filter: filter,
	}
}

// Outcome of handling one action: zero or more events to broadcast, or a rejection for the author.
type Outcome struct {
	Events []protocol.Event
	Reject string
}

func (r *Room) next() uint64 { r.seq++; return r.seq }

// Handle applies a chat message or moderator command from a user at time now.
func (r *Room) Handle(user string, role protocol.Role, text string, clientTS int64, now time.Time) Outcome {
	if user == "" {
		return Outcome{Reject: "anonymous viewers cannot chat; log in to send messages"}
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return Outcome{Reject: "empty message"}
	}
	if !utf8.ValidString(text) {
		return Outcome{Reject: "message is not valid UTF-8"}
	}
	if utf8.RuneCountInString(text) > MaxMessageRunes {
		return Outcome{Reject: "message exceeds " + strconv.Itoa(MaxMessageRunes) + " characters"}
	}
	// Bans and timeouts apply to everyone except the broadcaster, including moderators issuing commands.
	if role != protocol.RoleBroadcaster {
		if r.bans[user] {
			return Outcome{Reject: "you are banned from this channel"}
		}
		if until, ok := r.timeouts[user]; ok {
			if now.Before(until) {
				return Outcome{Reject: "you are timed out for " + until.Sub(now).Round(time.Second).String()}
			}
			delete(r.timeouts, user)
		}
	}
	b := r.buckets[user]
	if b == nil {
		cfg := viewerLimit
		if role.Privileged() {
			cfg = modLimit
		}
		b = NewBucket(cfg, now)
		r.buckets[user] = b
	}
	if strings.HasPrefix(text, "/") {
		if !b.Take(now) { // commands count against the same allowance as messages
			return Outcome{Reject: "you are sending messages too quickly"}
		}
		return r.command(user, role, text, now)
	}
	if !role.Privileged() && r.slow > 0 {
		if last, ok := r.lastMessage[user]; ok && now.Sub(last) < r.slow {
			return Outcome{Reject: "slow mode: wait " + (r.slow - now.Sub(last)).Round(time.Second).String()}
		}
	}
	if !b.Take(now) {
		return Outcome{Reject: "you are sending messages too quickly"}
	}
	if !role.Privileged() && r.filter != nil {
		if term, hit := r.filter.Match(text); hit {
			return Outcome{Reject: "message held by moderation filter (" + term + ")"}
		}
	}
	r.lastMessage[user] = now
	return Outcome{Events: []protocol.Event{{
		Kind: protocol.KindMessage, Channel: r.Name, Seq: r.next(), User: user, Text: text, ClientTS: clientTS,
	}}}
}

func (r *Room) notice(text string) protocol.Event {
	return protocol.Event{Kind: protocol.KindNotice, Channel: r.Name, Seq: r.next(), Text: text}
}

func (r *Room) command(user string, role protocol.Role, text string, now time.Time) Outcome {
	f := strings.Fields(text)
	cmd := strings.ToLower(f[0])
	if !role.Privileged() {
		return Outcome{Reject: "only moderators can use " + cmd}
	}
	arg := func(i int) string {
		if i < len(f) {
			return strings.TrimPrefix(strings.ToLower(f[i]), "@")
		}
		return ""
	}
	// The broadcaster cannot be timed out or banned in their own channel, and nobody can target themselves.
	// (Moderators can still act on other moderators: the broker learns roles only from each action's
	// sender, so it cannot rank targets.)
	if cmd == "/timeout" || cmd == "/ban" {
		if t := arg(1); t == r.Name {
			return Outcome{Reject: "the broadcaster cannot be " + map[string]string{"/ban": "banned", "/timeout": "timed out"}[cmd]}
		} else if t == user {
			return Outcome{Reject: "you cannot " + strings.TrimPrefix(cmd, "/") + " yourself"}
		}
	}
	switch cmd {
	case "/slow":
		secs := 30
		if a := arg(1); a != "" {
			n, err := strconv.Atoi(a)
			if err != nil || n < 1 || n > 1800 {
				return Outcome{Reject: "usage: /slow [seconds 1-1800]"}
			}
			secs = n
		}
		r.slow = time.Duration(secs) * time.Second
		return Outcome{Events: []protocol.Event{r.notice("slow mode on: " + strconv.Itoa(secs) + "s between messages")}}
	case "/slowoff":
		r.slow = 0
		return Outcome{Events: []protocol.Event{r.notice("slow mode off")}}
	case "/timeout":
		target := arg(1)
		if target == "" {
			return Outcome{Reject: "usage: /timeout <user> [seconds]"}
		}
		secs := 600
		if a := arg(2); a != "" {
			n, err := strconv.Atoi(a)
			if err != nil || n < 1 || n > 1209600 {
				return Outcome{Reject: "timeout must be 1-1209600 seconds"}
			}
			secs = n
		}
		r.timeouts[target] = now.Add(time.Duration(secs) * time.Second)
		return Outcome{Events: []protocol.Event{
			{Kind: protocol.KindPurge, Channel: r.Name, Seq: r.next(), Target: target},
			r.notice(target + " was timed out for " + strconv.Itoa(secs) + "s by " + user),
		}}
	case "/ban":
		target := arg(1)
		if target == "" {
			return Outcome{Reject: "usage: /ban <user>"}
		}
		r.bans[target] = true
		return Outcome{Events: []protocol.Event{
			{Kind: protocol.KindPurge, Channel: r.Name, Seq: r.next(), Target: target},
			r.notice(target + " was banned by " + user),
		}}
	case "/unban":
		target := arg(1)
		delete(r.bans, target)
		delete(r.timeouts, target)
		return Outcome{Events: []protocol.Event{r.notice(target + " was unbanned by " + user)}}
	case "/clear":
		return Outcome{Events: []protocol.Event{{Kind: protocol.KindClear, Channel: r.Name, Seq: r.next()},
			r.notice("chat was cleared by " + user)}}
	}
	return Outcome{Reject: "unknown command " + cmd}
}
