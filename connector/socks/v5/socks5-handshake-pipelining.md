# SOCKS5 Pipelining for the gost SOCKS5 connector (`connector/socks/v5`)

> **Status:** design, intended for upstream (`go-gost/x`). Backward compatible —
> default behavior is unchanged; the feature is opt-in per connector via metadata.
>
> Two-step rollout. **Step 1** prototypes and measures the optimization in iProxy's
> `relay-netprobe` against the real fleet of phone SOCKS5 servers
> (`iprx-core-main/apps/relay-netprobe/docs/socks5-handshake-pipelining.md`).
> **Step 2 (this doc)** applies it to the gost SOCKS5 **connector** — the client side
> used for SOCKS5 hops in a chain. Enabling the more aggressive mode in production is
> gated on Step 1's fleet validation.

## Motivation

When gost uses a SOCKS5 node as a forward hop, the connector is a SOCKS5 *client*.
Today it pays up to **2 RTT** (no-auth) or **3 RTT** (user/pass) of proxy round trips
before the tunneled application can send its first byte:

```
greeting ─►            ◄─ method reply        (RTT 1)
[user/pass] ─►         ◄─ auth reply          (RTT 2, user/pass only)
CONNECT ─►             ◄─ connect reply        (RTT 3)
<application data> ─►
```

If the client commits to **exactly one** auth method, the method reply is
predictable, so these exchanges can be coalesced. This doc proposes a **graded**
pipelining option (not a single boolean), so an operator can choose how aggressive
to be against a given upstream:

| `pipelining` | what is coalesced | RTT to first app byte at target | risk |
|---|---|---|---|
| `off` (default) | nothing — current behavior | 2–3 RTT + connect | none |
| `handshake` (alias `auth`) | greeting + [auth] + CONNECT in one write; replies read in order | 1 RTT + connect | needs single committed method |
| `data` (alias `earlydata`) | the above; the CONNECT reply is **not** awaited, so the tunneled app's first bytes go out right behind the request — a separate write, but with no reply round trip in between (SOCKS5 "0‑RTT"); all replies validated lazily on first Read | ~0 extra (app data follows the request without waiting for the reply) | also needs the server to buffer/relay early data and tolerate an optimistic CONNECT |

`data` is a strict superset of `handshake`, which is a strict superset of `off` — a
monotonic aggressiveness level. The latency win scales with the client→proxy RTT and
**compounds across every SOCKS5 hop** in a chain.

The single-method safety constraint is the same for all enabled modes (RFC 1928: the
server picks one of the offered methods; with one offered, the reply is
deterministic). It comes straight from the reference benchmark
`dengaleev/glitch-gate`, `go/socks5-pipelining-bench`.

## The architectural constraint

A SOCKS5 hop is established by two **separate** connector calls, driven by
`chain/route.go`:

- `Transport.Handshake(ctx, cc)` (`route.go:245`, `:291`) → `socks5Connector.Handshake`
  (`connector.go:75`): greeting + method reply + optional auth / TLS wrap.
  **No CONNECT address is known here.**
- `Transport.Connect(ctx, cn, "tcp", addr)` (`route.go:279`; single-node fast path
  `chainRoute.Dial`, `route.go:152`) → `socks5Connector.Connect` (`connector.go:95`):
  the destination is known only now; writes `NewRequest(CmdConnect, addr)`
  (`connector.go:140`) and reads the reply.

Because the address arrives only at `Connect`, today the greeting must be flushed in
`Handshake`. The fix is to make `Handshake` defer and let `Connect` emit the
coalesced flight.

The tunneled conn returned by the connector is then relayed with
`xnet.Pipe(ctx, conn, cc)` (e.g. `handler/socks/v5/connect.go`), which copies both
directions **concurrently** — so the connected conn is read and written by two
different goroutines. This matters for the `data` mode design below.

## Design: a deferred handshake wrapper

Add a `pipelineClientConn` (a `net.Conn`) returned by `Handshake` when pipelining is
on. It captures the raw conn, the committed method, the credentials, the mode, and the
connect timeout. **Do not reuse `gosocks5.ClientConn`** — its `Read`/`Write` call
`Handleshake()` first (`gosocks5 conn.go`), which would eagerly run the standard
2‑RTT handshake and defeat pipelining.

```go
// connector/socks/v5/pipeline_conn.go
type pipelineClientConn struct {
	net.Conn
	mode     pipelineMode
	method   uint8          // gosocks5.MethodNoAuth | MethodUserPass
	user     *url.Userinfo
	timeout  time.Duration  // connect timeout, applied around the lazy reply read
	logger   logger.Logger

	writeOnce sync.Once      // prefix injection (the greeting/auth) on first Write
	readOnce  sync.Once      // lazy reply validation on first Read (data mode)
	writeErr  error
	readErr   error
}
```

Two independent one-shot guards keep it concurrency-safe: the prefix is injected on
the first `Write`, and (in `data` mode) the replies are validated on the first `Read`
— these run on the two `xnet.Pipe` goroutines without sharing mutable state.

### `Write`

The **first** `Write` is the connector's `req.Write(conn)` inside `Connect`, so the
caller's `b` is the **CONNECT request bytes** (not application data). On that first call
(`writeOnce`): build **one** buffer = greeting `[Ver5, 1, method]` + (if
`method == MethodUserPass`) `gosocks5.NewUserPassRequest(user, pass)` encoded + `b`; do
a single `c.Conn.Write` — so the greeting + [auth] + CONNECT leave in one segment. Then:

- **`handshake` mode:** synchronously `readFull` the 2-byte method reply and verify
  `[Ver5, method]` (else `gosocks5.ErrBadMethod`); if user/pass,
  `gosocks5.ReadUserPassResponse` and check `Succeeded` (else
  `gosocks5.ErrAuthFailure`). The CONNECT reply is **not** read here.
- **`data` mode:** read nothing.

**Return `n = len(b)`** (the caller's bytes), never the coalesced total — to honor the
`io.Writer` contract (`gosocks5.Request.Write` ignores `n`, but `io.CopyBuffer` in the
relay checks `nw == nr`). Subsequent writes pass straight through.

Where the early data comes from: it is **not** part of the first-Write buffer. In
`data` mode the application's first bytes (a TLS ClientHello / the next hop's greeting /
an HTTP request) arrive on a *later* pass-through `Write` from the relay's client→target
goroutine. The saving comes from `data` mode not reading the CONNECT reply before those
later writes, so they go out right behind the request instead of one RTT later.

> The greeting + CONNECT is flushed eagerly by this first `Write` **at `Connect` time**,
> not lazily when application data first appears — and that is a guarantee, not a
> convention: `Connect` always calls `req.Write` *before* the handler starts
> `xnet.Pipe`. So `writeOnce` has always fired before any relay `Read`/pass-through
> `Write`. This is what prevents a deadlock for **server-speaks-first** targets (where
> the client sends no early data: there would otherwise be no greeting ⇒ no server
> bytes ⇒ hang).

### `Read`

- **`handshake` / `off`:** pass through. In `handshake` mode the method/auth replies
  were already consumed in `Write`; the connector's `gosocks5.ReadReply` then reads the
  CONNECT reply normally.
- **`data` mode:** on the first call (`readOnce`), set a deadline of `timeout` with
  **`SetReadDeadline` only** — never `SetDeadline`, which would also bound the
  concurrent early-data `Write` running on the other relay goroutine and corrupt it on a
  slow link. Then drain+validate the method reply, [auth reply], **and** the CONNECT
  reply (`gosocks5.ReadReply`, check `Succeeded`), clear the read deadline, and continue
  reading the application response into `b`. A reply failure is returned as `readErr`
  and surfaces to the relay as a read error.

Per the `Write` guarantee above, `Read` can never run before `writeOnce` has fired for
this connector (`req.Write` precedes `xnet.Pipe`). Still, keep it defensive: if `Read`
is reached with no write yet, return an explicit error rather than blocking on a socket
to which nothing was sent.

## Connector changes (minimal, localized)

### `metadata.go`

```go
type pipelineMode int

const (
	pipelineOff pipelineMode = iota
	pipelineHandshake
	pipelineData
)

func parsePipelineMode(s string) pipelineMode {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "handshake", "auth":
		return pipelineHandshake
	case "data", "earlydata":
		return pipelineData
	default: // "", "off", unknown
		return pipelineOff
	}
}
```

Add `pipeline pipelineMode` to `metadata` and parse it in `parseMetadata`
idiomatically (the file already uses `mdutil`):

```go
c.md.pipeline = parsePipelineMode(mdutil.GetString(md, "pipelining", "pipeline"))
```

Canonical values are `off | handshake | data` (plus the `auth`/`earlydata` aliases);
bool-style `true`/`false` are intentionally **not** accepted, to avoid the ambiguity of
whether `true` means "handshake" or "the most aggressive mode".

(The existing connect timeout is `md.connectTimeout`, parsed from the metadata key
`"timeout"` — there is no `connectTimeout` key; the wrapper reuses `md.connectTimeout`.)

No config-parsing-layer change is needed: a connector's `metadata` is a free-form
`map[string]any` wrapped by `metadata.NewMetadata` and consumed only by this package's
`parseMetadata`, so adding a key here is self-contained.

### `connector.go` — `Init`

When `c.md.pipeline != pipelineOff`, the offered method set must collapse to **one**
and TLS is incoherent (TLS needs the method reply before its own handshake). Fail
loudly on misconfiguration rather than silently downgrading:

```go
if c.md.pipeline != pipelineOff {
	if !c.md.noTLS {
		return fmt.Errorf("socks5: pipelining requires notls=true")
	}
	c.method = gosocks5.MethodNoAuth
	if c.options.Auth != nil {
		c.method = gosocks5.MethodUserPass
	}
	return nil // no gosocks5 selector needed on the pipelined path
}
// else: build the existing multi-method clientSelector, byte-for-byte unchanged
```

This **replaces** the method set (today `Init` unconditionally prepends
`MethodNoAuth`, `:48-49`).

### `connector.go` — `Handshake`

```go
if c.md.pipeline != pipelineOff {
	// no I/O here: no deadline to set (avoid dead code implying otherwise)
	return newPipelineClientConn(conn, c.md.pipeline, c.method,
		c.options.Auth, c.md.connectTimeout, log), nil
}
// else: existing gosocks5.ClientConn(...).Handleshake() path, unchanged
```

### `connector.go` — `Connect`

The TCP path needs one branch; everything else is unchanged:

```go
req := gosocks5.NewRequest(gosocks5.CmdConnect, &addr)
if err := req.Write(conn); err != nil { ... }          // wrapper coalesces greeting+[auth]+CONNECT

if pc, ok := conn.(*pipelineClientConn); ok && pc.mode == pipelineData {
	return pc, nil                                      // early-data: validate reply lazily on first Read
}

reply, err := gosocks5.ReadReply(conn)                 // off + handshake modes (handshake reply was consumed in Write)
... // check reply.Rep == Succeeded, as today
return conn, nil
```

In `off` mode `conn` is a `*gosocks5.Conn`, so the type assertion is false and the
legacy path runs verbatim. In `handshake` mode the assertion matches but
`pc.mode != pipelineData`, so it also falls through to `gosocks5.ReadReply` (which reads
the CONNECT reply through the wrapper's pass-through `Read`, the method/auth replies
having been consumed in `Write`).

### UDP

`connectUDP` / `relayUDP` (`CmdUDPTun` / `CmdUdp`) read the reply and use `reply.Addr`
(the bind address) before relaying (`connector.go:181`, `:203`/`:214-216`), so the
reply **cannot** be deferred — **`data` mode does not apply to UDP**.

The wrapper's lazy `readOnce` logic is **CONNECT-specific** (it validates method/auth
*and* the CONNECT reply), so the UDP paths must not go through it. They reach
`Connect`'s `network` switch (`connector.go:115-117`) *before* the TCP `data` branch, so
spell out the interaction: the wrapper exposes a `drainHandshake()` helper that consumes
the method + [auth] replies exactly once (this is the same helper `handshake`-mode
`Write` calls internally). For a `data` wrapper, `connectUDP`/`relayUDP` call
`pc.drainHandshake()` after `req.Write(conn)` and before `gosocks5.ReadReply(conn)` — i.e.
UDP forces synchronous, `handshake`-style reply consumption regardless of the configured
mode. (A `handshake` wrapper already drained in `Write`, so the helper is a no-op there.)
Document this asymmetry.

### `selector.go`

No change. The single-method path bypasses `gosocks5`'s selector entirely (the wrapper
does auth inline); `OnSelected` stays for the legacy path.

## Behavioral changes to document (upstream reviewers will ask)

- **Error attribution & failover marking (REQUIRED change, not just a note).**
  Auth/method failures move from the `Handshake` chain step (`route.go:245`/`:291`) into
  `Connect`'s first write (`handshake`) or into the first relay `Read` (`data`). For
  multi-hop, `route.go:280-289` already closes/marks the node on a `Connect` error.
  **But** a `Connect` failure on the **last/only** node runs through `chainRoute.Dial`
  (`route.go:152-157`), which only `conn.Close()`s — it does **not** run the `r.connect`
  marker / `MetricChainErrorsCounter` path that a `Handshake` failure used to trigger.
  Single-node chains are the primary iProxy relay case, so for them a pipelined
  auth/method failure would silently skip `marker.Mark()` and the chain-error counter,
  **breaking failover**. In `data` mode it is worse: the failure is a relay `Read` error
  raised entirely outside `route.go`, with no synchronous SOCKS error at all. Therefore
  this rollout **must** mark the node (and increment the chain-error counter) in the
  `Dial` last-node error branch — treat it as part of the change, not optional polish.
- **Metrics (REQUIRED).** `MetricNodeConnectDurationObserver` is recorded **only**
  around node[0]'s `Dial + Handshake` (`route.go:236` … `:262-264`); **no observer wraps
  any `Connect`**. With a no-op `Handshake` the metric degrades to *Dial-only* for
  node[0] and the handshake leg vanishes for later hops — it is **not** relocated.
  Because the rollout is metric-gated, add a dedicated pipelined-handshake/connect
  duration metric inside `pipelineClientConn` (timing the coalesced write + reply reads),
  and/or wrap the `route.go` `Connect` call with an observer. Land this with the feature,
  not after — otherwise the canary has no signal to watch.
- **`data` mode early-data exposure.** Application early data (e.g. a TLS ClientHello
  with SNI) is written to the proxy before CONNECT is confirmed; if CONNECT then fails,
  those bytes were already sent to the (trusted) proxy and are discarded. Acceptable for
  trusted upstreams; note it.
- **Deadlines.** `handshake` mode runs all I/O under `Connect`'s deadline
  (`connector.go:105-108`). `data` mode returns from `Connect` before reading the reply,
  so the wrapper applies `md.connectTimeout` around its own lazy reply read.

## Interop

gost's own SOCKS5 server and `gosocks5.ServerConn` tolerate a coalesced
`greeting + [auth] + request` (+ early data) in one segment: every server read is
length-bounded — `ReadMethods` reads exactly `2 + nmethods`, the user/pass read is
length-bounded, and `ReadRequest` reads exactly the request; trailing bytes (CONNECT,
then early data) stay buffered for the next read and are relayed after the server
connects to the target (`handler/socks/v5/{handler,connect}.go`). That guarantees the
**gost↔gost** case. Real third-party / phone SOCKS5 servers vary, which is exactly
what **Step 1** validates fleet-wide, per mode, before `data` (or even `handshake`) is
enabled against them in production.

## Configuration

```yaml
chains:
- name: chain-0
  hops:
  - name: hop-0
    nodes:
    - name: node-0
      addr: upstream.example:1080
      connector:
        type: socks5
        auth: { username: u, password: p }   # optional
        metadata:
          pipelining: handshake               # off | handshake | data
          notls: true                          # required when pipelining != off (Init errors otherwise)
```

## Verification & rollout

- **Build/vet:** `cd x && go build ./... && go vet ./...` (the connector package has no
  tests today; the module overall does, so an added test fits the existing footprint).
- **Correctness harness:** `go/socks5-pipelining-bench` exercises the identical
  coalesced byte framing against a SOCKS5 server; use it as the local check, and add a
  unit test with an in-process fake server asserting the single-segment write per mode
  and the lazy-reply ordering for `data`.
- **Staged:** default `off` ⇒ no behavior change. Enable `handshake` on one canary hop,
  watch the pipelined-handshake metric + SOCKS connect error rate, widen; only then try
  `data`, gated on Step 1 showing the fleet tolerates early data. **Rollback = remove
  the `pipelining` key** (reverts to the legacy multi-method handshake, no code revert).

---

**Grounding (file:line):**
`connector/socks/v5/connector.go` — `Init` methods `:48-68`, `Handshake` `:75-93`
(`gosocks5.ClientConn` `:86`), `Connect` `:95-161` (`req.Write` `:142`, `ReadReply`
`:147`), `connectUDP` `:163` (`CmdUDPTun` `:174`), `relayUDP` `:195`, deadlines
`:81-84`/`:105-108`;
`connector/socks/v5/metadata.go` — `parseMetadata`, `notls` `:26`, `timeout` `:25`;
`connector/socks/v5/selector.go` — `clientSelector`/`OnSelected` `:33-77`;
`chain/route.go` — `Handshake` `:245`/`:291` vs `Connect` `:152`/`:279`, error/close
`:280-289`, observer `:236`/`:262-264`, `chainRoute.Dial` `:135-159`;
`handler/socks/v5/connect.go` — `xnet.Pipe(ctx, conn, cc)` concurrent relay;
`gosocks5@v0.5.0/conn.go` — `clientHandshake`, eager `Handleshake()` in `Read`/`Write`;
`gosocks5@v0.5.0/socks5.go` — `io.Writer`/`io.Reader` wire primitives.

**New surface:** `pipelineMode` + parse + `pipeline` field in `metadata.go`; a new
`method uint8` field on `socks5Connector`; single-method/TLS gate + wrapper return in
`connector.go` `Init`/`Handshake`; one `data`-mode branch in `Connect` and a
`drainHandshake()` call in `connectUDP`/`relayUDP`; a node-marking fix in
`chain/route.go`'s last-node `Dial` branch plus a pipelined-handshake duration metric;
new `pipeline_conn.go`. The legacy path is byte-for-byte unchanged when `pipelining` is
`off` (the default).
