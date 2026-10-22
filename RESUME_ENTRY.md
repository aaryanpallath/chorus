# chorus — Live Chat Fan-out Service in Go
Tech: Go, WebSockets, TCP, distributed systems, concurrency, load testing, GitHub Actions
- Built a live-stream chat backend in Go: WebSocket edge servers fan messages out to viewers, and per-channel brokers apply rate limits, slow mode, timeouts/bans and moderation filtering while giving every viewer the same message order.
- Load-tested 5,999 concurrent WebSocket viewers: 100% delivery at 350K messages/s with 46 ms p99 latency during a simulated chat surge in a 1,631-viewer channel.
- Found that one-frame-per-message writes collapsed under the surge (2.0 s p99, 824 healthy viewers dropped); batching queued messages per frame removed every drop and cut p99 latency 44×.
