// Package protocol defines the messages exchanged between edge servers and brokers, and the
// events delivered to chat clients.
//
// Edge <-> broker traffic is newline-delimited JSON over TCP. Each event is encoded once by the
// broker and the same bytes are written to every subscribed edge, and then by the edge to every
// local viewer, so fan-out cost is dominated by socket writes rather than serialization.
package protocol

import (
	"bufio"
	"encoding/json"
	"io"
)

// Op codes for edge <-> broker frames.
const (
	OpSub    = "sub"   // edge -> broker: start receiving events for a channel
	OpUnsub  = "unsub" // edge -> broker: stop receiving events for a channel
	OpAction = "act"   // edge -> broker: a viewer's chat message or moderator command
	OpEvent  = "ev"    // broker -> edge: an accepted, sequenced channel event
	OpReject = "rej"   // broker -> edge: an action was refused (addressed to one connection)
)

// Role of a user within a channel.
type Role string

const (
	RoleViewer      Role = "viewer"
	RoleModerator   Role = "mod"
	RoleBroadcaster Role = "broadcaster"
)

// Privileged reports whether the role may run moderator commands.
func (r Role) Privileged() bool { return r == RoleModerator || r == RoleBroadcaster }

// Frame is the envelope for edge <-> broker traffic.
type Frame struct {
	Op      string `json:"op"`
	Channel string `json:"ch,omitempty"`
	ConnID  uint64 `json:"conn,omitempty"`
	User    string `json:"user,omitempty"`
	Role    Role   `json:"role,omitempty"`
	Text    string `json:"text,omitempty"`
	// ClientTS is the sender's timestamp (Unix ns), carried through so receivers can measure latency.
	ClientTS int64           `json:"ts,omitempty"`
	Reason   string          `json:"reason,omitempty"`
	Event    json.RawMessage `json:"ev,omitempty"`
}

// Event kinds delivered to clients.
const (
	KindMessage = "msg"    // a chat message
	KindNotice  = "notice" // a channel notice (mode change, moderation action)
	KindPurge   = "purge"  // remove a user's visible messages (timeout/ban)
	KindClear   = "clear"  // clear the whole chat
	KindReject  = "reject" // sent only to the author of a refused action
)

// Event is what chat clients receive. Seq is assigned by the channel's broker and is strictly
// increasing per channel, so every viewer on every edge sees the same order.
type Event struct {
	Kind     string `json:"kind"`
	Channel  string `json:"ch"`
	Seq      uint64 `json:"seq,omitempty"`
	User     string `json:"user,omitempty"`
	Text     string `json:"text,omitempty"`
	ClientTS int64  `json:"ts,omitempty"`
	Target   string `json:"target,omitempty"`
}

// Writer encodes frames as newline-delimited JSON.
type Writer struct{ w *bufio.Writer }

func NewWriter(w io.Writer) *Writer { return &Writer{w: bufio.NewWriterSize(w, 64<<10)} }

func (w *Writer) Write(f *Frame) error {
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}
	return w.WriteRaw(b)
}

// WriteRaw writes an already-encoded frame (without trailing newline).
func (w *Writer) WriteRaw(b []byte) error {
	if _, err := w.w.Write(b); err != nil {
		return err
	}
	return w.w.WriteByte('\n')
}

func (w *Writer) Flush() error { return w.w.Flush() }

// Reader decodes newline-delimited JSON frames.
type Reader struct{ s *bufio.Scanner }

func NewReader(r io.Reader) *Reader {
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 64<<10), 1<<20)
	return &Reader{s: s}
}

// Read returns the next frame, or io.EOF when the stream ends.
func (r *Reader) Read() (*Frame, error) {
	if !r.s.Scan() {
		if err := r.s.Err(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
	var f Frame
	if err := json.Unmarshal(r.s.Bytes(), &f); err != nil {
		return nil, err
	}
	return &f, nil
}
