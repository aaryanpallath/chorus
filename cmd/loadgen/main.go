// Command loadgen starts brokers and edges in-process, connects simulated viewers and chatters over
// real WebSockets, and measures delivery latency, completeness, and slow-consumer isolation.
//
// Two phases run back to back on the same cluster:
//
//	steady  - every viewer reads normally
//	stalled - a fraction of viewers stop reading; measures whether healthy viewers are affected
//	hype    - stalled viewers stay stalled while hundreds of extra chatters flood the largest channel
//
// Results are written as JSON; the README's numbers are generated from that file.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/coder/websocket"

	"github.com/aaryanpallath/chorus/internal/broker"
	"github.com/aaryanpallath/chorus/internal/edge"
	"github.com/aaryanpallath/chorus/internal/protocol"
)

type config struct {
	Brokers, Edges, Channels, Viewers, ChattersPerChannel int
	ChatRate                                              float64 // messages/s per chatter
	Zipf                                                  float64
	Phase                                                 time.Duration
	StallFraction                                         float64
	HypeChatters                                          int
	Queue                                                 int
	Seed                                                  uint64
}

type latencyStats struct {
	Samples int     `json:"samples"`
	P50ms   float64 `json:"p50_ms"`
	P90ms   float64 `json:"p90_ms"`
	P99ms   float64 `json:"p99_ms"`
	P999ms  float64 `json:"p999_ms"`
	MaxMs   float64 `json:"max_ms"`
}

type phaseResult struct {
	Name                string       `json:"name"`
	DurationS           float64      `json:"duration_s"`
	MessagesAccepted    int64        `json:"messages_accepted"`
	MessagesRejected    int64        `json:"messages_rejected"`
	ExpectedDeliveries  int64        `json:"expected_deliveries"`
	Deliveries          int64        `json:"deliveries"`
	DeliveryRatio       float64      `json:"delivery_ratio"`
	DeliveriesPerSecond float64      `json:"deliveries_per_second"`
	Latency             latencyStats `json:"latency_healthy_viewers"`
	StalledViewers      int          `json:"stalled_viewers"`
	SlowDisconnects     uint64       `json:"slow_disconnects"`
}

type result struct {
	GeneratedAt string        `json:"generated_at"`
	Machine     machine       `json:"machine"`
	Config      config        `json:"config"`
	Hottest     int           `json:"viewers_in_largest_channel"`
	Phases      []phaseResult `json:"phases"`
	SeqGaps     uint64        `json:"sequence_gaps_seen_by_edges"`
	BrokerDrops uint64        `json:"broker_to_edge_drops"`
	HeapMB      float64       `json:"heap_inuse_mb_end"`
	Goroutines  int           `json:"goroutines_end"`
	Note        string        `json:"note"`
}

type machine struct {
	OS, Arch string
	CPUs     int
	Go       string
}

// client is one WebSocket viewer (chatters are viewers that also send).
type client struct {
	user     string       // set for chatters
	accepted atomic.Int64 // own messages that came back sequenced, sent within the window
	rejected atomic.Int64 // reject events for messages sent within the window
	channel  int
	conn     *websocket.Conn
	stalled  atomic.Bool
	mu       sync.Mutex
	samples  []int64 // latency (ns) of messages received while healthy, within the measuring window
	received atomic.Int64
}

type window struct {
	mu         sync.RWMutex
	start, end int64 // ClientTS range counted in the current phase
}

func (w *window) contains(ts int64) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return ts >= w.start && ts < w.end
}

func main() {
	var c config
	flag.IntVar(&c.Brokers, "brokers", 2, "brokers")
	flag.IntVar(&c.Edges, "edges", 3, "edges")
	flag.IntVar(&c.Channels, "channels", 40, "channels")
	flag.IntVar(&c.Viewers, "viewers", 6000, "viewers (across all channels, Zipf-distributed)")
	flag.IntVar(&c.ChattersPerChannel, "chatters", 6, "chatting users per channel (also counted as viewers)")
	flag.Float64Var(&c.ChatRate, "rate", 0.5, "messages per second per chatter (viewer limit is 20 per 30s)")
	flag.Float64Var(&c.Zipf, "zipf", 1.1, "Zipf exponent for viewers per channel")
	flag.DurationVar(&c.Phase, "phase", 20*time.Second, "length of each measured phase")
	flag.Float64Var(&c.StallFraction, "stall", 0.05, "fraction of viewers that stop reading in the stalled phase")
	flag.IntVar(&c.HypeChatters, "hype", 300, "extra chatters who flood the largest channel in the hype phase")
	flag.IntVar(&c.Queue, "queue", 256, "per-viewer send queue at the edge")
	flag.Uint64Var(&c.Seed, "seed", 7, "random seed")
	out := flag.String("out", "results/loadtest.json", "output file")
	flag.Parse()

	raiseFDLimit()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	rng := rand.New(rand.NewPCG(c.Seed, c.Seed))

	// --- cluster
	secret := []byte("loadgen-secret")
	var brokerAddrs []string
	var brokers []*broker.Broker
	for i := 0; i < c.Brokers; i++ {
		ln := must(net.Listen("tcp", "127.0.0.1:0"))
		b := broker.New(nil, nil)
		brokers = append(brokers, b)
		go b.Serve(ln)
		brokerAddrs = append(brokerAddrs, ln.Addr().String())
	}
	var edges []*edge.Edge
	var edgeURLs []string
	for i := 0; i < c.Edges; i++ {
		e := edge.New(edge.Config{Brokers: brokerAddrs, Secret: secret, QueueSize: c.Queue})
		ln := must(net.Listen("tcp", "127.0.0.1:0"))
		go http.Serve(ln, e.Handler(nil))
		edges = append(edges, e)
		edgeURLs = append(edgeURLs, "ws://"+ln.Addr().String())
	}

	// --- viewers per channel ~ Zipf
	weights := make([]float64, c.Channels)
	total := 0.0
	for i := range weights {
		weights[i] = 1 / math.Pow(float64(i+1), c.Zipf)
		total += weights[i]
	}
	perChannel := make([]int, c.Channels)
	for i := range perChannel {
		perChannel[i] = max(c.ChattersPerChannel+1, int(math.Round(float64(c.Viewers)*weights[i]/total)))
	}
	fmt.Printf("viewers per channel (largest first): %v ... total %d\n", perChannel[:min(8, len(perChannel))], sum(perChannel))

	win := &window{}
	var clients []*client
	var chatters []*chatter
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dialSem := make(chan struct{}, 64)
	var wg sync.WaitGroup
	var mu sync.Mutex
	for ch, n := range perChannel {
		for i := 0; i < n; i++ {
			isChatter := i < c.ChattersPerChannel
			wg.Add(1)
			dialSem <- struct{}{}
			go func(ch, i int, edgeURL string) {
				defer wg.Done()
				defer func() { <-dialSem }()
				u := fmt.Sprintf("%s/ws?channel=ch%03d", edgeURL, ch)
				user := ""
				if isChatter {
					user = fmt.Sprintf("u%03d_%d", ch, i)
					u += "&token=" + edge.Sign(secret, edge.Claims{User: user, Expires: time.Now().Add(time.Hour).Unix()})
				}
				conn := dialRetry(ctx, u)
				conn.SetReadLimit(1 << 16)
				cl := &client{channel: ch, conn: conn, user: user}
				go cl.read(ctx, win)
				mu.Lock()
				clients = append(clients, cl)
				if isChatter {
					chatters = append(chatters, &chatter{client: cl, user: user})
				}
				mu.Unlock()
			}(ch, i, edgeURLs[rng.IntN(len(edgeURLs))])
		}
	}
	wg.Wait()
	fmt.Printf("connected %d clients (%d chatters) across %d edges\n", len(clients), len(chatters), c.Edges)
	time.Sleep(2 * time.Second) // subscriptions settle

	res := result{GeneratedAt: time.Now().UTC().Format(time.RFC3339), Config: c, Hottest: perChannel[0],
		Machine: machine{OS: runtime.GOOS, Arch: runtime.GOARCH, CPUs: runtime.NumCPU(), Go: runtime.Version()},
		Note: "brokers, edges and all clients run in one process on one machine over loopback; latency includes " +
			"client-side scheduling and is a conservative upper bound for the server path"}

	// chatters send for the whole run; each phase measures a window of their messages
	for _, ch := range chatters {
		go ch.chat(ctx, c.ChatRate, rng.Uint64())
	}
	time.Sleep(3 * time.Second) // warm-up

	edgeSlow := func() (n uint64) {
		for _, e := range edges {
			n += e.Metrics().SlowDisconnects
		}
		return
	}
	run := func(idx int, name string) phaseResult {
		for _, cl := range clients {
			cl.mu.Lock()
			cl.samples = cl.samples[:0]
			cl.mu.Unlock()
			cl.received.Store(0)
			cl.accepted.Store(0)
			cl.rejected.Store(0)
		}
		slowBefore := edgeSlow()
		start := time.Now()
		win.mu.Lock()
		win.start, win.end = start.UnixNano(), start.Add(c.Phase).UnixNano()
		win.mu.Unlock()
		if name == "hype" { // a burst of new chatters piles into the largest channel
			var hwg sync.WaitGroup
			for i := 0; i < c.HypeChatters; i++ {
				hwg.Add(1)
				go func(i int) {
					defer hwg.Done()
					user := fmt.Sprintf("hype_%d", i)
					u := fmt.Sprintf("%s/ws?channel=ch000&token=%s", edgeURLs[i%len(edgeURLs)],
						edge.Sign(secret, edge.Claims{User: user, Expires: time.Now().Add(time.Hour).Unix()}))
					conn := dialRetry(ctx, u)
					conn.SetReadLimit(1 << 16)
					cl := &client{channel: 0, conn: conn, user: user}
					go cl.read(ctx, win)
					ch := &chatter{client: cl, user: user}
					mu.Lock()
					clients = append(clients, cl)
					chatters = append(chatters, ch)
					mu.Unlock()
					go ch.chat(ctx, 0.6, uint64(i)+99)
				}(i)
			}
			hwg.Wait()
		}
		stalled := 0
		for _, cl := range clients {
			if cl.stalled.Load() {
				stalled++
			}
		}
		if name == "stalled" {
			stalled = 0
			for _, cl := range clients {
				if rng.Float64() < c.StallFraction && !cl.isChatter(chatters) {
					cl.stalled.Store(true)
					stalled++
				}
			}
		}
		time.Sleep(c.Phase + 3*time.Second) // let in-flight messages land
		pr := phaseResult{Name: name, DurationS: c.Phase.Seconds(), StalledViewers: stalled,
			SlowDisconnects: edgeSlow() - slowBefore}
		// expected deliveries: every message accepted in a channel reaches each healthy viewer in it
		perCh := make([]int64, c.Channels)
		for _, ch := range chatters {
			perCh[ch.channel] += ch.accepted.Load()
			pr.MessagesAccepted += ch.accepted.Load()
			pr.MessagesRejected += ch.rejected.Load()
		}
		var lat []int64
		for _, cl := range clients {
			if cl.stalled.Load() {
				continue
			}
			pr.ExpectedDeliveries += perCh[cl.channel]
			pr.Deliveries += cl.received.Load()
			cl.mu.Lock()
			lat = append(lat, cl.samples...)
			cl.mu.Unlock()
		}
		pr.DeliveryRatio = round(float64(pr.Deliveries)/float64(max(pr.ExpectedDeliveries, 1)), 5)
		pr.DeliveriesPerSecond = round(float64(pr.Deliveries)/c.Phase.Seconds(), 0)
		pr.Latency = summarize(lat)
		fmt.Printf("%-8s accepted=%d deliveries=%d/%d (%.4f) %.0f/s  p50=%.1fms p99=%.1fms p99.9=%.1fms  slow_disconnects=%d\n",
			name, pr.MessagesAccepted, pr.Deliveries, pr.ExpectedDeliveries, pr.DeliveryRatio, pr.DeliveriesPerSecond,
			pr.Latency.P50ms, pr.Latency.P99ms, pr.Latency.P999ms, pr.SlowDisconnects)
		return pr
	}
	res.Phases = append(res.Phases, run(0, "steady"), run(1, "stalled"), run(2, "hype"))
	for _, e := range edges {
		res.SeqGaps += e.Metrics().SeqGaps
	}
	for _, b := range brokers {
		res.BrokerDrops += b.Stats().EdgeDrops
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	res.HeapMB = round(float64(ms.HeapInuse)/1e6, 1)
	res.Goroutines = runtime.NumGoroutine()
	cancel()

	if err := os.MkdirAll(dir(*out), 0o755); err != nil {
		panic(err)
	}
	b, _ := json.MarshalIndent(res, "", "  ")
	if err := os.WriteFile(*out, append(b, '\n'), 0o644); err != nil {
		panic(err)
	}
	fmt.Println("wrote", *out)
}

func (cl *client) isChatter(chatters []*chatter) bool {
	return slices.ContainsFunc(chatters, func(c *chatter) bool { return c.client == cl })
}

func (cl *client) read(ctx context.Context, win *window) {
	for {
		if cl.stalled.Load() { // a stalled viewer stops reading but keeps the socket open
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}
		_, b, err := cl.conn.Read(ctx)
		if err != nil {
			return
		}
		now := time.Now().UnixNano()
		for _, line := range bytes.Split(b, []byte("\n")) {
			cl.handle(line, now, win)
		}
	}
}

func (cl *client) handle(line []byte, now int64, win *window) {
	{
		var ev protocol.Event
		if json.Unmarshal(line, &ev) != nil || !win.contains(ev.ClientTS) {
			return
		}
		if ev.Kind == protocol.KindReject {
			cl.rejected.Add(1)
			return
		}
		if ev.Kind != protocol.KindMessage {
			return
		}
		if cl.user != "" && ev.User == cl.user {
			cl.accepted.Add(1)
		}
		cl.received.Add(1)
		cl.mu.Lock()
		cl.samples = append(cl.samples, now-ev.ClientTS)
		cl.mu.Unlock()
	}
}

type chatter struct {
	*client
	user string
}

// chat sends at a Poisson rate until ctx ends. Acceptance is measured by the chatter's reader:
// an accepted message comes back sequenced from the broker, a refused one comes back as a reject.
func (c *chatter) chat(ctx context.Context, rate float64, seed uint64) {
	rng := rand.New(rand.NewPCG(seed, 1))
	for {
		wait := time.Duration(rng.ExpFloat64() / rate * float64(time.Second))
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		msg := fmt.Sprintf(`{"text":"%s says %s","ts":%d}`, c.user, strings.Repeat("pog ", 1+rng.IntN(8)),
			time.Now().UnixNano())
		if c.conn.Write(ctx, websocket.MessageText, []byte(msg)) != nil {
			return
		}
	}
}

func summarize(ns []int64) latencyStats {
	if len(ns) == 0 {
		return latencyStats{}
	}
	slices.Sort(ns)
	q := func(p float64) float64 {
		return round(float64(ns[min(len(ns)-1, int(p*float64(len(ns))))])/1e6, 2)
	}
	return latencyStats{Samples: len(ns), P50ms: q(0.50), P90ms: q(0.90), P99ms: q(0.99), P999ms: q(0.999),
		MaxMs: round(float64(ns[len(ns)-1])/1e6, 2)}
}

func dialRetry(ctx context.Context, u string) *websocket.Conn {
	for attempt := 0; ; attempt++ {
		dctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		c, _, err := websocket.Dial(dctx, u, nil)
		cancel()
		if err == nil {
			return c
		}
		if attempt > 20 {
			panic(err)
		}
		time.Sleep(time.Duration(attempt+1) * 50 * time.Millisecond)
	}
}

func raiseFDLimit() {
	var r syscall.Rlimit
	if syscall.Getrlimit(syscall.RLIMIT_NOFILE, &r) == nil {
		r.Cur = min(r.Max, 200000)
		_ = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &r)
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func sum(xs []int) (s int) {
	for _, x := range xs {
		s += x
	}
	return
}

func round(x float64, d int) float64 { p := math.Pow(10, float64(d)); return math.Round(x*p) / p }

func dir(p string) string {
	if i := strings.LastIndex(p, "/"); i >= 0 {
		return p[:i]
	}
	return "."
}
