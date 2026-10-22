// Command broker owns channel state for the channels routed to it and relays accepted events to edges.
package main

import (
	"flag"
	"log/slog"
	"net"
	"os"
	"strings"
	"time"

	"github.com/aaryanpallath/chorus/internal/broker"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7001", "listen address for edge connections")
	blocked := flag.String("blocked", "", "comma-separated terms for the moderation filter")
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Error("listen", "err", err)
		os.Exit(1)
	}
	var terms []string
	if *blocked != "" {
		terms = strings.Split(*blocked, ",")
	}
	b := broker.New(broker.NewFilter(terms), log)
	go func() {
		for range time.Tick(30 * time.Second) {
			s := b.Stats()
			log.Info("stats", "edges", s.Edges, "channels", s.Channels, "actions", s.Actions, "events", s.Events,
				"rejects", s.Rejects, "edge_drops", s.EdgeDrops)
		}
	}()
	log.Info("broker listening", "addr", *addr)
	if err := b.Serve(ln); err != nil {
		log.Error("serve", "err", err)
		os.Exit(1)
	}
}
