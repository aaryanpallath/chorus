// Package broker owns channel state and fans accepted events out to the edges that have viewers in
// each channel. One broker can serve many channels; several brokers split channels between them
// (edges pick the broker for a channel with rendezvous hashing), so no broker sees every channel.
package broker

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aaryanpallath/chorus/internal/protocol"
)

// EdgeQueue is how many frames may be waiting for one edge before the broker starts dropping for it.
const EdgeQueue = 16384

type Stats struct {
	Edges, Channels                     int
	Actions, Events, Rejects, EdgeDrops uint64
}

type Broker struct {
	filter *Filter
	log    *slog.Logger
	now    func() time.Time

	mu       sync.Mutex
	ln       net.Listener
	channels map[string]*channel
	edges    map[*edgeConn]struct{}

	actions, events, rejects, drops atomic.Uint64
}

type channel struct {
	mu   sync.Mutex
	room *Room
	subs map[*edgeConn]struct{}
}

type edgeConn struct {
	conn net.Conn
	out  chan []byte
	subs map[string]struct{} // touched only by the reader goroutine
}

func New(filter *Filter, log *slog.Logger) *Broker {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Broker{filter: filter, log: log, now: time.Now, channels: map[string]*channel{},
		edges: map[*edgeConn]struct{}{}}
}

// Serve accepts edge connections until the listener is closed.
func (b *Broker) Serve(ln net.Listener) error {
	b.mu.Lock()
	b.ln = ln
	b.mu.Unlock()
	for {
		c, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		if tc, ok := c.(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
		}
		go b.handle(c)
	}
}

// Close stops accepting and drops every edge connection (edges reconnect and resubscribe).
func (b *Broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ln != nil {
		_ = b.ln.Close()
	}
	for e := range b.edges {
		_ = e.conn.Close()
	}
}

func (b *Broker) Stats() Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return Stats{Edges: len(b.edges), Channels: len(b.channels), Actions: b.actions.Load(),
		Events: b.events.Load(), Rejects: b.rejects.Load(), EdgeDrops: b.drops.Load()}
}

func (b *Broker) channel(name string) *channel {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := b.channels[name]
	if ch == nil {
		ch = &channel{room: NewRoom(name, b.filter), subs: map[*edgeConn]struct{}{}}
		b.channels[name] = ch
	}
	return ch
}

func (b *Broker) handle(c net.Conn) {
	e := &edgeConn{conn: c, out: make(chan []byte, EdgeQueue), subs: map[string]struct{}{}}
	b.mu.Lock()
	b.edges[e] = struct{}{}
	b.mu.Unlock()
	b.log.Info("edge connected", "addr", c.RemoteAddr())

	done := make(chan struct{})
	go b.writer(e, done)
	defer func() {
		for name := range e.subs {
			ch := b.channel(name)
			ch.mu.Lock()
			delete(ch.subs, e)
			ch.mu.Unlock()
		}
		b.mu.Lock()
		delete(b.edges, e)
		b.mu.Unlock()
		close(e.out)
		<-done
		_ = c.Close()
		b.log.Info("edge disconnected", "addr", c.RemoteAddr())
	}()

	r := protocol.NewReader(c)
	for {
		f, err := r.Read()
		if err != nil {
			return
		}
		switch f.Op {
		case protocol.OpSub:
			if _, ok := e.subs[f.Channel]; ok {
				continue
			}
			e.subs[f.Channel] = struct{}{}
			ch := b.channel(f.Channel)
			ch.mu.Lock()
			ch.subs[e] = struct{}{}
			ch.mu.Unlock()
		case protocol.OpUnsub:
			if _, ok := e.subs[f.Channel]; !ok {
				continue
			}
			delete(e.subs, f.Channel)
			ch := b.channel(f.Channel)
			ch.mu.Lock()
			delete(ch.subs, e)
			ch.mu.Unlock()
		case protocol.OpAction:
			b.act(e, f)
		}
	}
}

// act runs an action against the channel's room and broadcasts the result. Holding the channel
// lock while enqueueing keeps every edge's copy of the channel in sequence order.
func (b *Broker) act(origin *edgeConn, f *protocol.Frame) {
	b.actions.Add(1)
	ch := b.channel(f.Channel)
	ch.mu.Lock()
	defer ch.mu.Unlock()
	out := ch.room.Handle(f.User, f.Role, f.Text, f.ClientTS, b.now())
	if out.Reject != "" {
		b.rejects.Add(1)
		rej, _ := json.Marshal(&protocol.Frame{Op: protocol.OpReject, Channel: f.Channel, ConnID: f.ConnID,
			Reason: out.Reject, ClientTS: f.ClientTS})
		b.enqueue(origin, rej)
		return
	}
	for i := range out.Events {
		ev, _ := json.Marshal(&out.Events[i])
		frame, _ := json.Marshal(&protocol.Frame{Op: protocol.OpEvent, Channel: f.Channel, Event: ev})
		b.events.Add(1)
		for e := range ch.subs {
			b.enqueue(e, frame)
		}
	}
}

func (b *Broker) enqueue(e *edgeConn, frame []byte) {
	select {
	case e.out <- frame:
	default:
		// The edge is not keeping up. Dropping here protects every other edge; the drop is
		// counted and the edge can detect the sequence gap.
		b.drops.Add(1)
	}
}

func (b *Broker) writer(e *edgeConn, done chan<- struct{}) {
	defer close(done)
	w := protocol.NewWriter(e.conn)
	for frame := range e.out {
		if err := w.WriteRaw(frame); err != nil {
			_ = e.conn.Close()
			for range e.out { // drain until the reader side closes the queue
			}
			return
		}
		if len(e.out) == 0 { // batch writes while the queue has more
			if err := w.Flush(); err != nil {
				_ = e.conn.Close()
				for range e.out {
				}
				return
			}
		}
	}
}
