// Package edge terminates viewer WebSocket connections. An edge keeps one connection per broker,
// subscribes to a channel only while it has local viewers in it, and fans each event out to those
// viewers. Every viewer has a bounded send queue: a viewer that cannot keep up is disconnected
// instead of slowing down delivery to everyone else.
//
// Wire format to clients: each WebSocket text frame carries one or more JSON events separated by
// newlines (events never contain raw newlines, since JSON escapes them).
package edge

import (
	"context"
	"encoding/json"
	"errors"
	"hash/fnv"
	"log/slog"
	"net"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/aaryanpallath/chorus/internal/protocol"
)

type Config struct {
	Brokers   []string // broker addresses; each channel is owned by one of them
	Secret    []byte   // HMAC key for viewer tokens
	QueueSize int      // per-viewer send queue (frames); full queue => disconnect
	Log       *slog.Logger
}

type Metrics struct {
	Viewers         int64  `json:"viewers"`
	Channels        int    `json:"channels"`
	Delivered       uint64 `json:"delivered"`
	SlowDisconnects uint64 `json:"slow_disconnects"`
	ActionsSent     uint64 `json:"actions_sent"`
	Rejects         uint64 `json:"rejects"`
	SeqGaps         uint64 `json:"seq_gaps"`
}

type Edge struct {
	cfg     Config
	brokers []*brokerLink
	ctx     context.Context
	cancel  context.CancelFunc

	mu       sync.RWMutex
	channels map[string]*localChannel
	conns    map[uint64]*viewer

	nextID                                          atomic.Uint64
	viewers                                         atomic.Int64
	delivered, slowDrops, actions, rejects, seqGaps atomic.Uint64
}

type localChannel struct {
	viewers map[*viewer]struct{}
	lastSeq uint64
}

type viewer struct {
	id      uint64
	channel string
	user    string // "" for anonymous read-only viewers
	role    protocol.Role
	send    chan []byte
	closed  atomic.Bool
	cancel  context.CancelFunc
}

// New starts broker links in the background; Close stops them.
func New(cfg Config) *Edge {
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 256
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	e := &Edge{cfg: cfg, ctx: ctx, cancel: cancel, channels: map[string]*localChannel{}, conns: map[uint64]*viewer{}}
	for _, addr := range cfg.Brokers {
		bl := &brokerLink{edge: e, addr: addr, out: make(chan *protocol.Frame, 65536)}
		e.brokers = append(e.brokers, bl)
		go bl.run(ctx)
	}
	return e
}

func (e *Edge) Close() { e.cancel() }

// brokerFor picks the channel's broker by rendezvous (highest random weight) hashing, so adding a
// broker moves only about 1/N of the channels.
func (e *Edge) brokerFor(channel string) *brokerLink {
	var best *brokerLink
	var bestScore uint64
	for _, b := range e.brokers {
		h := fnv.New64a()
		h.Write([]byte(b.addr))
		h.Write([]byte{0})
		h.Write([]byte(channel))
		if s := h.Sum64(); best == nil || s > bestScore {
			best, bestScore = b, s
		}
	}
	return best
}

func (e *Edge) Metrics() Metrics {
	e.mu.RLock()
	n := len(e.channels)
	e.mu.RUnlock()
	return Metrics{Viewers: e.viewers.Load(), Channels: n, Delivered: e.delivered.Load(),
		SlowDisconnects: e.slowDrops.Load(), ActionsSent: e.actions.Load(), Rejects: e.rejects.Load(),
		SeqGaps: e.seqGaps.Load()}
}

// maxBatch caps how many queued events are coalesced into one WebSocket frame.
const maxBatch = 128

var channelName = regexp.MustCompile(`^[a-z0-9_]{1,25}$`)

// Handler serves /ws (viewer WebSocket), /metrics and /healthz.
func (e *Edge) Handler(static http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", e.serveWS)
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(e.Metrics())
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("ok")) })
	if static != nil {
		mux.Handle("GET /", static)
	}
	return mux
}

func (e *Edge) serveWS(w http.ResponseWriter, r *http.Request) {
	ch := strings.ToLower(r.URL.Query().Get("channel"))
	if !channelName.MatchString(ch) {
		http.Error(w, "channel must be 1-25 characters of a-z, 0-9, _", http.StatusBadRequest)
		return
	}
	var claims Claims
	if tok := r.URL.Query().Get("token"); tok != "" {
		c, err := Verify(e.cfg.Secret, tok, time.Now())
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		claims = c
	}
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: []string{"localhost:*", "127.0.0.1:*"}})
	if err != nil {
		return
	}
	c.SetReadLimit(4096)
	ctx, cancel := context.WithCancel(e.ctx)
	v := &viewer{id: e.nextID.Add(1), channel: ch, user: claims.User, role: claims.RoleIn(ch),
		send: make(chan []byte, e.cfg.QueueSize), cancel: cancel}
	e.join(v)
	defer e.leave(v)
	go e.writeLoop(ctx, c, v)
	e.readLoop(ctx, c, v)
	cancel()
}

func (e *Edge) join(v *viewer) {
	e.mu.Lock()
	lc := e.channels[v.channel]
	first := lc == nil
	if first {
		lc = &localChannel{viewers: map[*viewer]struct{}{}}
		e.channels[v.channel] = lc
	}
	lc.viewers[v] = struct{}{}
	e.conns[v.id] = v
	e.mu.Unlock()
	e.viewers.Add(1)
	if first {
		e.brokerFor(v.channel).send(&protocol.Frame{Op: protocol.OpSub, Channel: v.channel})
	}
}

func (e *Edge) leave(v *viewer) {
	e.mu.Lock()
	delete(e.conns, v.id)
	last := false
	if lc := e.channels[v.channel]; lc != nil {
		delete(lc.viewers, v)
		if len(lc.viewers) == 0 {
			delete(e.channels, v.channel)
			last = true
		}
	}
	e.mu.Unlock()
	e.viewers.Add(-1)
	if last {
		e.brokerFor(v.channel).send(&protocol.Frame{Op: protocol.OpUnsub, Channel: v.channel})
	}
}

// inbound is what a client sends: a chat line (or /command) and an optional client timestamp.
type inbound struct {
	Text string `json:"text"`
	TS   int64  `json:"ts"`
}

func (e *Edge) readLoop(ctx context.Context, c *websocket.Conn, v *viewer) {
	defer c.CloseNow()
	for {
		_, data, err := c.Read(ctx)
		if err != nil {
			return
		}
		var in inbound
		if json.Unmarshal(data, &in) != nil {
			continue
		}
		if v.user == "" {
			e.deliverTo(v, rejectEvent(v.channel, "log in to chat", in.TS))
			continue
		}
		e.actions.Add(1)
		e.brokerFor(v.channel).send(&protocol.Frame{Op: protocol.OpAction, Channel: v.channel, ConnID: v.id,
			User: v.user, Role: v.role, Text: in.Text, ClientTS: in.TS})
	}
}

func (e *Edge) writeLoop(ctx context.Context, c *websocket.Conn, v *viewer) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-v.send:
			// Coalesce whatever else is already queued into the same frame (newline-separated events),
			// so a burst costs one WebSocket write per viewer instead of one per message.
			frame := msg
			if n := len(v.send); n > 0 {
				buf := make([]byte, 0, len(msg)*(n+1)+n)
				buf = append(buf, msg...)
				for i := 0; i < n && i < maxBatch; i++ {
					buf = append(buf, '\n')
					buf = append(buf, <-v.send...)
				}
				frame = buf
			}
			wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := c.Write(wctx, websocket.MessageText, frame)
			cancel()
			if err != nil {
				v.cancel()
				return
			}
		}
	}
}

// deliverTo queues an event for one viewer; a full queue marks the viewer as a slow consumer.
func (e *Edge) deliverTo(v *viewer, msg []byte) {
	if v.closed.Load() {
		return
	}
	select {
	case v.send <- msg:
		e.delivered.Add(1)
	default:
		if v.closed.CompareAndSwap(false, true) {
			e.slowDrops.Add(1)
			v.cancel()
		}
	}
}

// fanout delivers a broker event to every local viewer of the channel.
func (e *Edge) fanout(f *protocol.Frame) {
	var seq struct {
		Seq uint64 `json:"seq"`
	}
	_ = json.Unmarshal(f.Event, &seq)
	e.mu.Lock()
	lc := e.channels[f.Channel]
	if lc == nil {
		e.mu.Unlock()
		return
	}
	if lc.lastSeq != 0 && seq.Seq > lc.lastSeq+1 {
		e.seqGaps.Add(seq.Seq - lc.lastSeq - 1)
	}
	if seq.Seq > lc.lastSeq {
		lc.lastSeq = seq.Seq
	}
	targets := make([]*viewer, 0, len(lc.viewers))
	for v := range lc.viewers {
		targets = append(targets, v)
	}
	e.mu.Unlock()
	msg := []byte(f.Event) // encoded once by the broker, shared by every viewer
	for _, v := range targets {
		e.deliverTo(v, msg)
	}
}

func rejectEvent(ch, reason string, ts int64) []byte {
	b, _ := json.Marshal(&protocol.Event{Kind: protocol.KindReject, Channel: ch, Text: reason, ClientTS: ts})
	return b
}

func (e *Edge) reject(f *protocol.Frame) {
	e.rejects.Add(1)
	e.mu.RLock()
	v := e.conns[f.ConnID]
	e.mu.RUnlock()
	if v != nil {
		e.deliverTo(v, rejectEvent(f.Channel, f.Reason, f.ClientTS))
	}
}

// subscribedTo lists channels with local viewers owned by the given broker (for resubscribe).
func (e *Edge) subscribedTo(b *brokerLink) []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	var out []string
	for ch := range e.channels {
		if e.brokerFor(ch) == b {
			out = append(out, ch)
		}
	}
	return out
}

func (e *Edge) resetSeq(b *brokerLink) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for ch, lc := range e.channels {
		if e.brokerFor(ch) == b {
			lc.lastSeq = 0
		}
	}
}

// brokerLink is a persistent connection to one broker with automatic reconnect.
type brokerLink struct {
	edge *Edge
	addr string
	out  chan *protocol.Frame
}

func (b *brokerLink) send(f *protocol.Frame) {
	select {
	case b.out <- f:
	default:
		b.edge.cfg.Log.Warn("broker queue full; dropping frame", "broker", b.addr, "op", f.Op)
	}
}

func (b *brokerLink) run(ctx context.Context) {
	backoff := 100 * time.Millisecond
	for ctx.Err() == nil {
		var d net.Dialer
		conn, err := d.DialContext(ctx, "tcp", b.addr)
		if err != nil {
			b.edge.cfg.Log.Warn("broker dial failed", "broker", b.addr, "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, 5*time.Second)
			continue
		}
		backoff = 100 * time.Millisecond
		b.serve(ctx, conn)
	}
}

func (b *brokerLink) serve(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { <-connCtx.Done(); _ = conn.Close() }()

	w := protocol.NewWriter(conn)
	// (Re)subscribe every channel this broker owns that has local viewers. A restarted broker begins
	// each channel's sequence again at 1, so forget the old position to keep gap detection correct.
	b.edge.resetSeq(b)
	for _, ch := range b.edge.subscribedTo(b) {
		if w.Write(&protocol.Frame{Op: protocol.OpSub, Channel: ch}) != nil {
			return
		}
	}
	if w.Flush() != nil {
		return
	}
	go func() {
		defer cancel()
		for {
			select {
			case <-connCtx.Done():
				return
			case f := <-b.out:
				if w.Write(f) != nil {
					return
				}
				if len(b.out) == 0 && w.Flush() != nil {
					return
				}
			}
		}
	}()
	r := protocol.NewReader(conn)
	for {
		f, err := r.Read()
		if err != nil {
			if !errors.Is(err, context.Canceled) && connCtx.Err() == nil {
				b.edge.cfg.Log.Warn("broker connection lost", "broker", b.addr, "err", err)
			}
			return
		}
		switch f.Op {
		case protocol.OpEvent:
			b.edge.fanout(f)
		case protocol.OpReject:
			b.edge.reject(f)
		}
	}
}
