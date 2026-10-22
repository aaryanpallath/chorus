package broker

import (
	"strings"
	"testing"
	"time"

	"github.com/aaryanpallath/chorus/internal/protocol"
)

var t0 = time.Date(2027, 6, 1, 12, 0, 0, 0, time.UTC)

func newRoom() *Room { return NewRoom("demo", NewFilter([]string{"spoiler", "bad"})) }

func TestMessagesGetIncreasingSequence(t *testing.T) {
	r := newRoom()
	for i := 1; i <= 3; i++ {
		out := r.Handle("alice", protocol.RoleViewer, "hi", 0, t0.Add(time.Duration(i)*time.Second))
		if out.Reject != "" || out.Events[0].Seq != uint64(i) {
			t.Fatalf("msg %d: %+v", i, out)
		}
	}
}

func TestValidation(t *testing.T) {
	r := newRoom()
	for _, tc := range []struct{ user, text, want string }{
		{"", "hi", "anonymous"},
		{"alice", "   ", "empty"},
		{"alice", strings.Repeat("x", MaxMessageRunes+1), "exceeds"},
		{"alice", "\xff\xfe", "UTF-8"},
		{"alice", "/ban bob", "only moderators"},
	} {
		if out := r.Handle(tc.user, protocol.RoleViewer, tc.text, 0, t0); !strings.Contains(out.Reject, tc.want) {
			t.Errorf("%q: got %q want %q", tc.text, out.Reject, tc.want)
		}
	}
}

func TestSlowModeAppliesToViewersNotMods(t *testing.T) {
	r := newRoom()
	if out := r.Handle("mod1", protocol.RoleModerator, "/slow 10", 0, t0); len(out.Events) != 1 {
		t.Fatal(out)
	}
	if out := r.Handle("alice", protocol.RoleViewer, "one", 0, t0); out.Reject != "" {
		t.Fatal(out.Reject)
	}
	if out := r.Handle("alice", protocol.RoleViewer, "two", 0, t0.Add(5*time.Second)); !strings.Contains(out.Reject, "slow mode") {
		t.Fatalf("expected slow mode rejection, got %+v", out)
	}
	if out := r.Handle("alice", protocol.RoleViewer, "three", 0, t0.Add(10*time.Second)); out.Reject != "" {
		t.Fatal(out.Reject)
	}
	for i := 0; i < 3; i++ {
		if out := r.Handle("mod1", protocol.RoleModerator, "mod msg", 0, t0.Add(time.Second)); out.Reject != "" {
			t.Fatal("mods are exempt from slow mode:", out.Reject)
		}
	}
	r.Handle("mod1", protocol.RoleModerator, "/slowoff", 0, t0)
	if out := r.Handle("alice", protocol.RoleViewer, "four", 0, t0.Add(11*time.Second)); out.Reject != "" {
		t.Fatal(out.Reject)
	}
}

func TestTimeoutExpiresAndBanPersists(t *testing.T) {
	r := newRoom()
	out := r.Handle("mod1", protocol.RoleModerator, "/timeout @Alice 60", 0, t0)
	if len(out.Events) != 2 || out.Events[0].Kind != protocol.KindPurge || out.Events[0].Target != "alice" {
		t.Fatalf("timeout should purge then notify: %+v", out.Events)
	}
	if r.Handle("alice", protocol.RoleViewer, "hi", 0, t0.Add(59*time.Second)).Reject == "" {
		t.Fatal("timed-out user could chat")
	}
	if out := r.Handle("alice", protocol.RoleViewer, "hi", 0, t0.Add(61*time.Second)); out.Reject != "" {
		t.Fatal("timeout did not expire:", out.Reject)
	}
	r.Handle("mod1", protocol.RoleModerator, "/ban alice", 0, t0)
	if r.Handle("alice", protocol.RoleViewer, "hi", 0, t0.Add(time.Hour)).Reject == "" {
		t.Fatal("banned user could chat")
	}
	r.Handle("mod1", protocol.RoleModerator, "/unban alice", 0, t0)
	if out := r.Handle("alice", protocol.RoleViewer, "hi", 0, t0.Add(2*time.Hour)); out.Reject != "" {
		t.Fatal(out.Reject)
	}
}

func TestRateLimit(t *testing.T) {
	r := newRoom()
	ok := 0
	for i := 0; i < 25; i++ { // 25 messages in one second; viewers get 20 per 30s
		if r.Handle("alice", protocol.RoleViewer, "spam", 0, t0.Add(time.Duration(i)*40*time.Millisecond)).Reject == "" {
			ok++
		}
	}
	if ok != 20 {
		t.Fatalf("allowed %d, want 20", ok)
	}
	// 1.5s later, one token (20/30s) has refilled
	if r.Handle("alice", protocol.RoleViewer, "again", 0, t0.Add(2500*time.Millisecond)).Reject != "" {
		t.Fatal("bucket did not refill")
	}
}

func TestFilter(t *testing.T) {
	f := NewFilter([]string{"spoiler", "bad"})
	for _, tc := range []struct {
		text string
		hit  bool
	}{
		{"no SPOILERS please", true}, // 5+ letter term matches inside a longer word
		{"5p01l3r alert", true},      // leetspeak
		{"s.p.o.i.l.e.r", true},      // punctuation inside a word
		{"s p o i l e r", true},      // spaced single letters
		{"that was baaaad", true},    // stretched letters
		{"badge earned", false},      // short term does not fire inside other words
		{"b a d idea", true},         // ...but spaced letters still join up
		{"a b a d idea", false},      // known limit: a one-letter word joins the run ("abad")
		{"great play", false},
	} {
		if _, hit := f.Match(tc.text); hit != tc.hit {
			t.Errorf("%q: hit=%v want %v", tc.text, hit, tc.hit)
		}
	}
}

func TestModsBypassFilter(t *testing.T) {
	r := newRoom()
	if !strings.Contains(r.Handle("alice", protocol.RoleViewer, "spoiler: he wins", 0, t0).Reject, "filter") {
		t.Fatal("viewer message should be filtered")
	}
	if out := r.Handle("mod1", protocol.RoleModerator, "spoiler warning, be careful", 0, t0); out.Reject != "" {
		t.Fatal(out.Reject)
	}
}

func TestModerationHierarchy(t *testing.T) {
	r := newRoom() // channel "demo"
	if out := r.Handle("mod1", protocol.RoleModerator, "/ban demo", 0, t0); !strings.Contains(out.Reject, "broadcaster") {
		t.Fatalf("mod banned the broadcaster: %+v", out)
	}
	if out := r.Handle("mod1", protocol.RoleModerator, "/timeout @DEMO 60", 0, t0); !strings.Contains(out.Reject, "broadcaster") {
		t.Fatalf("mod timed out the broadcaster: %+v", out)
	}
	if out := r.Handle("mod1", protocol.RoleModerator, "/ban mod1", 0, t0); !strings.Contains(out.Reject, "yourself") {
		t.Fatalf("self-ban allowed: %+v", out)
	}
	// a banned moderator can no longer run commands; the broadcaster can unban them
	r.Handle("demo", protocol.RoleBroadcaster, "/ban mod1", 0, t0)
	if out := r.Handle("mod1", protocol.RoleModerator, "/clear", 0, t0); !strings.Contains(out.Reject, "banned") {
		t.Fatalf("banned moderator ran a command: %+v", out)
	}
	if out := r.Handle("demo", protocol.RoleBroadcaster, "/unban mod1", 0, t0); out.Reject != "" {
		t.Fatal(out.Reject)
	}
}

func TestCommandsAreRateLimited(t *testing.T) {
	r := newRoom()
	ok := 0
	for i := 0; i < 120; i++ { // moderators get 100 actions per 30s
		if r.Handle("mod1", protocol.RoleModerator, "/slow 5", 0, t0).Reject == "" {
			ok++
		}
	}
	if ok != 100 {
		t.Fatalf("allowed %d commands, want 100", ok)
	}
}
