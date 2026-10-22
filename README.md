# chorus — live chat fan-out in Go

A live-stream chat backend: viewers connect over WebSockets to stateless **edge** servers; each
channel is owned by one **broker**, which applies the chat rules, puts messages in order, and sends
them to only the edges that have viewers in that channel. A load generator drives thousands of real
WebSocket clients and measures delivery latency, completeness, and what happens during a "hype
moment" when one huge channel's chat explodes.

## Results

One run of `go run ./cmd/loadgen` (defaults): 2 brokers, 3 edges, 40 channels, **5,999 viewers**
(Zipf-distributed, so the largest channel has **1,631 viewers**), 240 regular chatters each at 0.5
messages/s, three 20-second phases. Apple M4 (10 cores), Go 1.27.3. Brokers, edges and every client
share one process over loopback, so latency includes the clients' own scheduling.

| phase | messages accepted | deliveries to healthy viewers | delivered | p50 | p99 | p99.9 |
|---|---:|---:|---:|---:|---:|---:|
| steady | 2,357 | 353,346 (17,667/s) | 100% | 3.47 ms | 20.69 ms | 25.95 ms |
| 295 viewers stop reading | 2,434 | 337,208 (16,860/s) | 100% | 3.80 ms | 23.11 ms | 29.41 ms |
| hype: +300 chatters flood the 1,631-viewer channel | 5,948 | 7,005,768 (350,288/s) | 100% | 16.75 ms | 46.38 ms | 62.39 ms |

No sequence gaps were seen by any edge and no broker-to-edge frames were dropped
([`results/loadtest.json`](results/loadtest.json)).

### What batching changed
The first version sent one WebSocket frame per chat message per viewer. In the hype phase that meant
hundreds of thousands of tiny writes a second, and it fell over
([`results/loadtest_baseline_unbatched.json`](results/loadtest_baseline_unbatched.json)). The edge
now coalesces whatever is already queued for a viewer (up to 128 events) into one frame. Same
machine, same seed:

| hype phase | one frame per message | batched frames |
|---|---:|---:|
| deliveries per second | 198,697 | **350,288** |
| delivered to viewers who were reading | 80.7% | **100%** |
| p99 / p99.9 latency | 2,049.98 ms / 5,477.90 ms | **46.38 ms / 62.39 ms** |
| reading viewers disconnected as "too slow" | 824 | **0** |

Before batching, edges could not write fast enough, so viewers' queues filled even though they were
reading, and the slow-consumer protection disconnected them. That protection is meant for clients
that have stopped reading, so the fix was to make the write path cheaper rather than to loosen it.

### What the "stop reading" phase does and doesn't show
295 viewers (5%) stopped reading for the last two phases, and the healthy viewers' latency did not
change. But none of the stalled viewers were disconnected in the 40 seconds: at these chat rates the
kernel's socket buffers absorb everything sent to them. The disconnect path is instead covered by
`TestSlowConsumerIsDisconnectedOthersUnaffected`, which floods a channel until a non-reading viewer
overflows and checks that a reading viewer on the same edge still receives every message.

## Architecture

```
 viewers ──WebSocket──▶ edge 1 ─┐            ┌─▶ broker A  (owns channels hashed to A)
 viewers ──WebSocket──▶ edge 2 ─┼── TCP ─────┤
 viewers ──WebSocket──▶ edge 3 ─┘            └─▶ broker B  (owns channels hashed to B)
```

* **Edge** (`internal/edge`): authenticates viewers (HMAC-signed tokens; no token = read-only),
  subscribes to a channel's broker only while it has local viewers there, and fans each event out to
  them. Each viewer has a bounded queue; a full queue means the viewer is disconnected, so one stuck
  client never holds up anyone else.
* **Broker** (`internal/broker`): the single authority for its channels: rate limits, slow mode,
  timeouts and bans, a moderation filter, and a per-channel sequence number. Because one broker
  decides everything for a channel, viewers on different edges see identical order with no edge-to-edge
  coordination.
* **Routing**: edges choose a channel's broker by rendezvous hashing, so adding a broker moves only
  about 1/N of the channels.
* **Encode once**: the broker serializes each event once; the same bytes go to every edge and on to
  every viewer.
* **Recovery**: edges reconnect to a restarted broker with backoff and resubscribe their channels.

### Chat rules
* 500-character messages, valid UTF-8; anonymous viewers can read but not send.
* Token bucket per user: 20 messages per 30 s for viewers, 100 per 30 s for moderators.
* Moderator commands: `/slow [s]`, `/slowoff`, `/timeout user [s]`, `/ban user`, `/unban user`,
  `/clear`. Timeouts and bans also remove the user's messages from viewers' screens.
* Moderation filter that catches l33t-speak (`5p01l3r`), stretched letters (`baaaad`), dots between
  letters and spaced-out letters, without blocking innocent words that contain a short term (`badge`).

## Setup and usage

Requires Go 1.27+.

```bash
go test -race ./...                      # unit + integration tests (real brokers, edges and WebSockets)
go run ./cmd/loadgen                     # the load test above (~90 s; raises the open-file limit itself)

# run it by hand
export CHORUS_SECRET=dev-secret
go run ./cmd/broker -addr 127.0.0.1:7001 -blocked spoiler &
go run ./cmd/edge -addr 127.0.0.1:8080 -brokers 127.0.0.1:7001 &
go run ./cmd/token -user alice -mod demo   # paste into the page to chat as a moderator of #demo
open http://127.0.0.1:8080                  # demo chat client
```

Edges expose `/metrics` (viewers, deliveries, slow-consumer disconnects, sequence gaps) and `/healthz`.

## Design decisions

* **Per-channel authority instead of shared state.** Rules like slow mode and bans must be consistent
  for everyone in a channel. Putting each channel on exactly one broker gives that consistency and a
  total order without a database or consensus. The cost is that a broker restart loses in-memory channel
  state (bans, slow mode); see Limitations.
* **Disconnect slow consumers rather than buffer without bound.** Unbounded per-viewer buffers turn one
  stalled client into memory growth for the whole edge.
* **Size queues for bursts, not averages.** While testing, a 16-slot queue was smaller than a single
  moderator's 90-message burst, so even a viewer that was reading got disconnected. Queues now default
  to 256 events.
* **Own broker protocol, no Redis.** Newline-delimited JSON over TCP keeps the system dependency-free and
  testable in-process; the trade-off is no persistence or replay.

## Limitations
* Broker state lives in memory: a restarted broker forgets bans, timeouts and slow mode, and starts
  sequence numbers over (edges detect this and reset their gap tracking). Persisting moderation state
  is the next step.
* A single broker is a single point of failure for its channels; there is no standby.
* The load test runs clients and servers on one machine, so it measures this machine's limits, not a
  deployed cluster's.
* Tokens are HMAC-signed by a shared secret. There is no login service; `cmd/token` issues tokens for testing.
* The filter can be evaded by a one-letter word next to spaced letters ("a b a d" reads as "abad").

## Testing
`go test -race ./...` covers the chat rules (sequencing, validation, slow mode, timeouts and bans,
rate limits, filter cases) and, with real brokers, edges and WebSocket clients:
* identical order for viewers on different edges;
* read-only anonymous viewers;
* forged tokens rejected;
* a ban issued on one edge enforced on another;
* slow-consumer isolation;
* resubscription after a broker restart.

CI also runs a short load test and fails if any healthy viewer misses a message.

## Data & licensing
No external data; all traffic is generated by `cmd/loadgen`. Code is MIT-licensed. The WebSocket
library is [coder/websocket](https://github.com/coder/websocket) (ISC).

## AI assistance disclosure
Built with AI assistance (Claude Code). The load tests are how the design was checked. They found the
one-frame-per-message bottleneck, and the integration tests caught a per-viewer queue that was too
small for a single message burst. All numbers above come from the committed result files.
