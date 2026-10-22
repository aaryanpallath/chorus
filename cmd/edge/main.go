// Command edge serves viewer WebSockets and the demo chat page.
package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/aaryanpallath/chorus/internal/edge"
	"github.com/aaryanpallath/chorus/web"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "HTTP/WebSocket listen address")
	brokers := flag.String("brokers", "127.0.0.1:7001", "comma-separated broker addresses")
	queue := flag.Int("queue", 256, "per-viewer send queue; a viewer whose queue fills is disconnected")
	flag.Parse()
	secret := os.Getenv("CHORUS_SECRET")
	if secret == "" {
		slog.Error("set CHORUS_SECRET (shared with whatever issues viewer tokens)")
		os.Exit(1)
	}
	e := edge.New(edge.Config{Brokers: strings.Split(*brokers, ","), Secret: []byte(secret), QueueSize: *queue})
	slog.Info("edge listening", "addr", *addr)
	if err := http.ListenAndServe(*addr, e.Handler(http.FileServerFS(web.Files))); err != nil {
		slog.Error("serve", "err", err)
		os.Exit(1)
	}
}
