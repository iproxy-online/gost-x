# SOCKS5 Handshake Pipelining — Step 2: gost `connector/socks/v5`

> Two-step rollout. **Step 1** prototypes and measures the optimization in
> `relay-netprobe` against the real fleet of phone SOCKS5 servers
> (`iprx-core-main/apps/relay-netprobe/docs/socks5-handshake-pipelining.md`).
> **Step 2 (this doc)** applies it to the production gost SOCKS5 **connector** (the
> client side used for SOCKS5 hops in a chain). **Enabling Step 2 in production is
> gated on Step 1's fleet results** confirming real phone servers tolerate a single
> coalesced write.

## What pipelining buys

A SOCKS5 client hop costs a full client→proxy RTT before the CONNECT request can even
be written, because the greeting/method-selection round trip completes first:
2 RTT (no-auth) or 3 (user/pass) per hop. Committing to a **single** auth method
makes the method reply predictable, so `greeting + [auth] + CONNECT` can go in one
write and the replies be read in order — collapsing the per-hop handshake to **1
RTT** and saving 1 RTT (no-auth) / 2 (user/pass). In a multi-hop chain the saving
**compounds across every SOCKS5 hop**. See the reference benchmark
`dengaleev/glitch-gate`, `go/socks5-pipelining-bench`.

## The architectural problem

A SOCKS5 client hop is established by two **separate** connector-interface calls,
driven by `chain/route.go`:

- `Transport.Handshake(ctx, cc)` (`route.go:245`, `:291`) →
  `socks5Connector.Handshake` (`connector/socks/v5/connector.go:75`): writes the
  method greeting, reads the method reply, and (via `clientSelector.OnSelected`,
  `selector.go:33`) optionally does user/pass auth or a TLS wrap. **No CONNECT
  address is available here.**
- `Transport.Connect(ctx, cn, "tcp", addr)` (`route.go:279`; single-node fast path
  `route.go:152`) → `socks5Connector.Connect` (`connector.go:95`): only *now* is the
  destination known; it writes `NewRequest(CmdConnect, addr)` (`connector.go:140`)
  and reads the reply.

Because the address arrives only at `Connect` time, today the greeting must be
flushed in `Handshake`, paying the RTT up front. The per-hop flow per `route.go` is
`Dial → Handshake`, then for each further node `preNode.Connect → node.Handshake`.

## Solution: deferred (lazy) handshake

Keep the two-call interface intact, but make `Handshake` **write nothing** when
pipelining is enabled. It returns a new `pipelineClientConn` wrapper (a `net.Conn`)
capturing the raw conn, the committed method, and the auth credentials. The wrapper's
**first `Write`** — which is exactly the `req.Write(conn)` issued inside `Connect`
(`connector.go:142`, and the UDP paths `:176`/`:198`) — prepends the greeting +
(if user/pass) the auth request, performs **one** `Write`, then synchronously
**reads and validates** the 2-byte method reply and (if user/pass) the 2-byte
`UserPassResponse`, and only then lets the caller's CONNECT bytes flow. The
subsequent `gosocks5.ReadReply(conn)` (`connector.go:147`) reads the CONNECT reply
cleanly off the same stream.

This preserves the connector interface and the chain driver unchanged, and the saving
compounds per SOCKS5 hop.

> **Do not reuse `gosocks5.ClientConn` for this.** Its `Read`/`Write` call
> `Handleshake()` first (`gosocks5 conn.go`), which eagerly runs the standard 2-RTT
> handshake and defeats pipelining. `pipelineClientConn` is a purpose-built wrapper.

### `pipelineClientConn` (new file `connector/socks/v5/pipeline_conn.go`)

```go
type pipelineClientConn struct {
    net.Conn               // raw transport conn
    method  uint8          // committed: gosocks5.MethodNoAuth or MethodUserPass
    user    *url.Userinfo
    logger  logger.Logger
    once    sync.Once
    done    bool           // handshake completed
    hsErr   error
}
```

- **`Write(b []byte) (int, error)`** — on the first call (guarded by `once`), build
  one buffer: greeting `[Ver5, 1, method]` (matching `gosocks5 conn.go`
  `clientHandshake`) + if `method == MethodUserPass`,
  `gosocks5.NewUserPassRequest(...).Write(buf)` + the caller's `b`. Single
  `c.Conn.Write`. Then `readFull` 2 bytes for the method reply and verify
  `reply[0]==Ver5 && reply[1]==method` (else `gosocks5.ErrBadMethod`). If user/pass,
  `gosocks5.ReadUserPassResponse` and check `Status==Succeeded` (else
  `gosocks5.ErrAuthFailure`). Record any failure in `hsErr` and return it.
  **Return `n = len(b)`** (the caller's byte count) — *not* the coalesced total — to
  honour the `io.Writer` contract; map a short write of the coalesced buffer to an
  error. Subsequent `Write`s pass straight through to `c.Conn`.
- **`Read(b []byte) (int, error)`** — if the one-time handshake has not run yet,
  **return an explicit error** (`"socks5 pipeline: read before request write"`),
  *not* a silent block on a socket to which nothing was sent. `pipelineClientConn` is
  only valid in the `Handshake → Connect`(write-first) sequence, which is exactly how
  the connector and `chain/route.go` use it; the explicit error makes that contract
  defensive rather than convention-only.
- All other `net.Conn` methods delegate to the embedded conn.

## Gating rule

Pipelining is **safe only** when exactly one non-TLS method is committed. Enable only
when **both** hold:

1. New metadata flag `pipelining` (alias `pipeline`) is `true`.
2. The method set collapses to a single non-TLS method:
   - `Auth == nil` → `[gosocks5.MethodNoAuth]`
   - `Auth != nil` → `[gosocks5.MethodUserPass]`

TLS and pipelining are incoherent (TLS needs the method reply before its handshake).
**Enforce it:** `Init` returns an error when `md.pipelining && !md.noTLS`, rather
than silently downgrading a TLS-expecting hop to a plaintext single-method handshake.
(Equivalently: document that `pipelining` unconditionally implies no-TLS — pick one
and make the code match it. Enforcing is preferred, since it fails loudly on
misconfig.)

Default is **off** — when the flag is absent/false, `Init` builds the existing
multi-method selector **byte-for-byte unchanged** and `Handshake`/`Connect` follow
today's `gosocks5.ClientConn` path exactly.

## Change surface

- **`metadata.go`** — add `pipelining bool`, parsed via
  `mdutil.GetBool(md, "pipelining", "pipeline")` (default `false`). (The existing
  connect-timeout field `md.connectTimeout` is parsed from the metadata key
  `"timeout"`, `metadata.go:25` — there is no `connectTimeout` config key; the
  pipelined path reuses the same `md.connectTimeout`.)
- **`connector.go` `Init`** (`:42`) — if `c.md.pipelining`: error out when
  `!c.md.noTLS`; otherwise set `selector.methods` to the single committed method per
  `Auth` (this **replaces** the slice — today `Init` unconditionally prepends
  `MethodNoAuth` at `:48-49`), skip the `!noTLS` TLS-method block, and stash the
  committed method on the connector for the wrapper. Otherwise unchanged.
- **`connector.go` `Handshake`** (`:75`) — if pipelining, return
  `&pipelineClientConn{Conn: conn, method: committed, user: c.options.Auth, …}`
  **without** setting a deadline (the current `SetDeadline` block at `:81-84` would
  wrap a no-op — drop it on this branch to avoid dead code that implies I/O happens
  here). Otherwise unchanged.
- **`connector.go` `Connect` / `connectUDP` / `relayUDP`** — **no change.** They
  already call `req.Write(conn)` then `gosocks5.ReadReply(conn)` on whatever conn
  `Handshake` returned, which is now the lazy wrapper. `Connect` already sets the I/O
  deadline (`:105-108`), which now correctly covers the coalesced write + the reply
  reads.
- **`selector.go`** — no structural change; the single-method slice is supplied by
  `Init`. `OnSelected` is unused on the pipelined path (the wrapper does auth inline)
  and stays intact for the legacy path.
- **`pipeline_conn.go`** (new) — the `pipelineClientConn` type above.

## UDP, deadlines, error semantics

- **UDP / `CmdUDPTun`** — `connectUDP` writes `NewRequest(CmdUDPTun, nil)`
  (`:174`) and `relayUDP` writes `NewRequest(CmdUdp, nil)` (`:196`); both go through
  the same first-`Write` trigger (`CmdUDPTun` has a nil addr, so the first write is
  small but still a valid trigger). `relay=="udp"` then dials `reply.Addr`,
  unaffected by client-side pipelining. Works identically.
- **Deadlines** — with deferral, the real I/O deadline is the one `Connect` sets
  (`:105-108`); the wrapper never relies on a `Handshake` deadline (which is why the
  pipelined `Handshake` branch sets none).
- **Error semantics** — today auth/method failures surface during the `Handshake`
  chain step (`route.go:245`/`:291`); with deferral they surface on the first `Write`
  inside `Connect`, attributed to the `Connect` step / target node. For the multi-hop
  case `route.go:280-289` closes conns and marks the node on a `Connect` error, so
  failover handling is preserved. **Caveat to verify and document:** a `Connect`
  failure on the **last** node runs through `chainRoute.Dial` (`route.go:152-157`),
  which closes the conn but does **not** run the `r.connect` marker /
  `MetricChainErrorsCounter` path that a `Handshake` failure used to trigger. So for a
  single-node (or last-node) chain, a pipelined auth/method failure now bypasses
  `marker.Mark()` / the chain-error counter. If selector marking on auth failure
  matters for failover, either restore it in the `Dial` error branch or accept and
  document the change — do not claim "failure handling is unchanged."

## Metrics / observability (read carefully — there is a regression)

`MetricNodeConnectDurationObserver` is recorded **only** around node[0]'s
`Dial + Handshake` (`route.go:236` start … `route.go:262-264` observe, inside
`r.connect`). **No observer wraps any `Connect` call** (neither `route.go:152` nor
`:279`). Consequences of moving the handshake bytes into `Connect`:

- For node[0] (and the only node in the common single-hop case), the observed
  duration **degrades to Dial latency only** — the handshake leg it used to include
  is now a no-op.
- For later hops, the handshake leg simply **disappears** from metrics; it is **not**
  relocated into any `Connect` observer.

So the very dashboard a rollout would "watch" no longer reflects what changed. Address
it deliberately:

- Add a dedicated **pipelined-handshake duration** metric inside
  `pipelineClientConn.Write` (time the coalesced write + reply reads), so the canary
  has a real signal; **and/or**
- Wrap the `Connect` call in `route.go` with an observer feeding
  `MetricNodeConnectDurationObserver` to keep handshake timing in the existing metric.

Keep `log.Trace(req)`/`Trace(reply)` in `Connect`; add a one-time debug line in the
wrapper's first `Write` recording the coalesced size and committed method.

## Interop

gost's own SOCKS5 server (`handler/socks/v5`) and `gosocks5.ServerConn` tolerate a
coalesced `greeting + [auth] + request` in one TCP segment: every server read is
length-bounded — `ReadMethods` reads exactly `2 + nmethods`, `serverHandshake` then
reads a length-bounded `UserPassRequest`, and `ReadRequest` reads exactly the request
— none over-read, so trailing CONNECT bytes stay buffered for the next read
(`handler/socks/v5/handler.go:158-159`). **This is the gost↔gost case.** Production
hops terminate at real phone SOCKS5 servers, whose tolerance is what **Step 1**
validates fleet-wide before any hop flips this flag on.

## Config, verification, rollout

**Operator config** — per chain hop:

```yaml
chains:
- name: chain-0
  hops:
  - name: hop-0
    nodes:
    - name: node-0
      addr: phone.example:1080
      connector:
        type: socks5
        auth: { username: u, password: p }   # optional
        metadata:
          pipelining: true
          notls: true                          # required (Init errors otherwise)
```

**Verification:** `cd x && go build ./... && go vet ./...` (no tests in the module).
Use `go/socks5-pipelining-bench` as a local correctness/latency check — it exercises
the identical coalesced byte framing against a fake server.

**Staged rollout / rollback:** default OFF ⇒ no behavior change. Enable on one canary
hop, watch the new pipelined-handshake metric and error rates, then widen. **Rollback
is flipping `pipelining: false`** (or removing the key) on the hop — `Init` rebuilds
the legacy multi-method selector and the regular `gosocks5.ClientConn` path runs
unchanged, with zero code revert required.

---

**Grounding (file:line):**
`connector/socks/v5/connector.go` — `Init` methods built `:48-68`, `Handshake`
`:75-93` (`gosocks5.ClientConn` `:86`), `Connect` `:95-161` (`req.Write` `:142`,
`ReadReply` `:147`), `connectUDP` `:163` (`CmdUDPTun` `:174`), `relayUDP` `:195`,
deadlines `:81-84`/`:105-108`;
`connector/socks/v5/metadata.go` — `parseMetadata`, `notls` `:26`, `timeout` `:25`;
`connector/socks/v5/selector.go` — `clientSelector`/`OnSelected` `:33-77`;
`chain/route.go` — `Handshake` `:245`/`:291` vs `Connect` `:152`/`:279`, error/close
`:280-289`, observer `:236`/`:262-264`;
`gosocks5@v0.5.0/conn.go` — `clientHandshake`, eager `Handleshake()` in `Read`/`Write`
(reason a fresh wrapper is required);
`gosocks5@v0.5.0/socks5.go` — `io.Writer`/`io.Reader` wire primitives (`NewRequest`,
`ReadReply`, `NewUserPassRequest`, `ReadUserPassResponse`, `ReadMethods`).

**New code surface:** `pipelining` in `metadata.go`; single-method gate + lazy-wrapper
return in `connector.go` `Init`/`Handshake`; new `pipeline_conn.go`. `Connect` /
`connectUDP` / `relayUDP` and the entire legacy path stay byte-for-byte unchanged when
the flag is off.
