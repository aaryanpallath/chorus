package edge_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/aaryanpallath/chorus/internal/broker"
	"github.com/aaryanpallath/chorus/internal/edge"
	"github.com/aaryanpallath/chorus/internal/protocol"
)

var secret = []byte("test-secret")

type cluster struct {
	edges   []*httptest.Server
	brokers []net.Listener
}

func start(t *testing.T, nBrokers, nEdges, queue int) *cluster {
	t.Helper()
	c := &cluster{}
	var addrs []string
	for i := 0; i < nBrokers; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		go broker.New(broker.NewFilter([]string{"spoiler"}), nil).Serve(ln)
		c.brokers = append(c.brokers, ln)
		addrs = append(addrs, ln.Addr().String())
	}
	for i := 0; i < nEdges; i++ {
		e := edge.New(edge.Config{Brokers: addrs, Secret: secret, QueueSize: queue})
		srv := httptest.NewServer(e.Handler(nil))
		t.Cleanup(func() { srv.Close(); e.Close() })
		c.edges = append(c.edges, srv)
	}
	t.Cleanup(func() {
		for _, ln := range c.brokers {
			ln.Close()
		}
	})
	return c
}

func token(user string, mods ...string) string {
	return edge.Sign(secret, edge.Claims{User: user, ModOf: mods, Expires: time.Now().Add(time.Hour).Unix()})
}

func dial(t *testing.T, srv *httptest.Server, channel, tok string) *websocket.Conn {
	t.Helper()
	u := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws?channel=" + channel
	if tok != "" {
		u += "&token=" + tok
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, u, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.CloseNow() })
	return c
}

func say(t *testing.T, c *websocket.Conn, text string) {
	t.Helper()
	b, _ := json.Marshal(map[string]any{"text": text, "ts": time.Now().UnixNano()})
	if err := c.Write(context.Background(), websocket.MessageText, b); err != nil {
		t.Fatal(err)
	}
}

var pending sync.Map // *websocket.Conn -> []protocol.Event not yet consumed (frames may batch events)

func next(t *testing.T, c *websocket.Conn) protocol.Event {
	t.Helper()
	if q, ok := pending.Load(c); ok && len(q.([]protocol.Event)) > 0 {
		evs := q.([]protocol.Event)
		pending.Store(c, evs[1:])
		return evs[0]
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, b, err := c.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var evs []protocol.Event
	for _, line := range bytes.Split(b, []byte("\n")) {
		var ev protocol.Event
		if err := json.Unmarshal(line, &ev); err != nil {
			t.Fatal(err)
		}
		evs = append(evs, ev)
	}
	pending.Store(c, evs[1:])
	return evs[0]
}

// countEvents returns how many events a raw frame carries.
func countEvents(b []byte) int64 { return int64(bytes.Count(b, []byte("\n")) + 1) }

// waitSubscribed sends a probe message until it comes back, i.e. the edge's subscription is live.
func waitSubscribed(t *testing.T, sender *websocket.Conn, readers ...*websocket.Conn) {
	t.Helper()
	say(t, sender, "probe")
	for _, r := range readers {
		if ev := next(t, r); ev.Text != "probe" {
			t.Fatalf("expected probe, got %+v", ev)
		}
	}
}

func TestViewersOnDifferentEdgesSeeSameOrder(t *testing.T) {
	c := start(t, 2, 2, 256)
	a := dial(t, c.edges[0], "race", token("alice"))
	b := dial(t, c.edges[1], "race", token("bob"))
	time.Sleep(100 * time.Millisecond)
	waitSubscribed(t, a, a, b)
	for i := 0; i < 15; i++ { // interleave senders on both edges (viewers may send 20 per 30s)
		say(t, a, fmt.Sprintf("a%d", i))
		say(t, b, fmt.Sprintf("b%d", i))
	}
	var seenA, seenB []string
	var lastSeq uint64
	for i := 0; i < 30; i++ {
		ea, eb := next(t, a), next(t, b)
		if ea.Seq <= lastSeq {
			t.Fatalf("sequence not increasing: %d after %d", ea.Seq, lastSeq)
		}
		lastSeq = ea.Seq
		seenA, seenB = append(seenA, ea.Text), append(seenB, eb.Text)
	}
	if strings.Join(seenA, ",") != strings.Join(seenB, ",") {
		t.Fatalf("edges disagree on order:\n%v\n%v", seenA, seenB)
	}
}

func TestAnonymousViewersAreReadOnly(t *testing.T) {
	c := start(t, 1, 1, 256)
	anon := dial(t, c.edges[0], "demo", "")
	say(t, anon, "hello")
	if ev := next(t, anon); ev.Kind != protocol.KindReject {
		t.Fatalf("got %+v", ev)
	}
}

func TestBadTokenRejected(t *testing.T) {
	c := start(t, 1, 1, 256)
	u := "ws" + strings.TrimPrefix(c.edges[0].URL, "http") + "/ws?channel=demo&token=forged.sig"
	if _, _, err := websocket.Dial(context.Background(), u, nil); err == nil {
		t.Fatal("forged token accepted")
	}
}

func TestBanAcrossEdges(t *testing.T) {
	c := start(t, 1, 2, 256)
	mod := dial(t, c.edges[0], "stream", token("mia", "stream"))
	troll := dial(t, c.edges[1], "stream", token("troll"))
	time.Sleep(100 * time.Millisecond)
	waitSubscribed(t, mod, mod, troll)
	say(t, mod, "/ban troll")
	for _, conn := range []*websocket.Conn{mod, troll} {
		if ev := next(t, conn); ev.Kind != protocol.KindPurge || ev.Target != "troll" {
			t.Fatalf("expected purge, got %+v", ev)
		}
		next(t, conn) // notice
	}
	say(t, troll, "still here")
	if ev := next(t, troll); ev.Kind != protocol.KindReject || !strings.Contains(ev.Text, "banned") {
		t.Fatalf("banned user's message was not rejected: %+v", ev)
	}
}

func drain(c *websocket.Conn) {
	for {
		if _, _, err := c.Read(context.Background()); err != nil {
			return
		}
	}
}

func metrics(t *testing.T, srv *httptest.Server) edge.Metrics {
	t.Helper()
	resp, err := srv.Client().Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var m edge.Metrics
	_ = json.NewDecoder(resp.Body).Decode(&m)
	return m
}

func TestSlowConsumerIsDisconnectedOthersUnaffected(t *testing.T) {
	c := start(t, 1, 1, 128) // must absorb one sender burst (90 msgs) yet still overflow for a non-reader
	fast := dial(t, c.edges[0], "flood", "")
	slow := dial(t, c.edges[0], "flood", "") // never reads
	first := dial(t, c.edges[0], "flood", token("mod0", "flood"))
	time.Sleep(100 * time.Millisecond)
	waitSubscribed(t, first, first, fast)
	go drain(first)

	// The fast viewer reads everything concurrently.
	var received atomic.Int64
	go func() {
		for {
			_, b, err := fast.Read(context.Background())
			if err != nil {
				return
			}
			received.Add(countEvents(b))
		}
	}()
	// Flood with large messages from many moderators (100 msgs/30s each) until the never-reading
	// viewer has filled its TCP buffers and its edge queue. Kernel socket buffers absorb the first
	// few hundred KB, which is why this needs real volume.
	payload := strings.Repeat("x", 480)
	sent := int64(0)
	for m := 0; m < 60 && metrics(t, c.edges[0]).SlowDisconnects == 0; m++ {
		mod := dial(t, c.edges[0], "flood", token(fmt.Sprintf("mod%d", m+1), "flood"))
		go drain(mod) // senders are viewers too; keep them reading so only `slow` falls behind
		for i := 0; i < 90; i++ {
			say(t, mod, payload)
			sent++
		}
		time.Sleep(20 * time.Millisecond)
	}
	if metrics(t, c.edges[0]).SlowDisconnects != 1 {
		t.Fatalf("slow viewer was never disconnected: %+v (sent %d)", metrics(t, c.edges[0]), sent)
	}
	deadline := time.Now().Add(10 * time.Second)
	for received.Load() < sent && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := received.Load(); got != sent {
		t.Fatalf("fast viewer received %d of %d messages", got, sent)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for { // the slow viewer's connection was closed: draining it ends in an error
		if _, _, err := slow.Read(ctx); err != nil {
			if ctx.Err() != nil {
				t.Fatal("slow viewer connection still open")
			}
			break
		}
	}
}

func TestEdgeResubscribesAfterBrokerRestart(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	b1 := broker.New(nil, nil)
	go b1.Serve(ln)
	e := edge.New(edge.Config{Brokers: []string{addr}, Secret: secret})
	srv := httptest.NewServer(e.Handler(nil))
	defer func() { srv.Close(); e.Close() }()
	a := dial(t, srv, "phoenix", token("alice"))
	time.Sleep(100 * time.Millisecond)
	waitSubscribed(t, a, a)

	b1.Close() // broker crashes: edge connection dropped
	ln2, err := net.Listen("tcp", addr)
	if err != nil {
		t.Skip("could not rebind broker port:", err)
	}
	b2 := broker.New(nil, nil)
	go b2.Serve(ln2)
	defer b2.Close()

	// The viewer stays connected to the edge; once the edge reconnects and resubscribes, chat flows again.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if b2.Stats().Channels == 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if b2.Stats().Channels != 1 {
		t.Fatal("edge did not resubscribe to the restarted broker")
	}
	say(t, a, "back online")
	if ev := next(t, a); ev.Text != "back online" || ev.Seq != 1 {
		t.Fatalf("got %+v (a restarted broker starts a fresh sequence)", ev)
	}
}
