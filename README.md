# TunnelBench

Find the fastest and steadiest tunnel between an **Iran server** and a server abroad (**Kharej**).

Iran's filtering behaves differently per datacenter and per route, so the method that works on one pair of servers can be dead or throttled on the next. TunnelBench runs every tunnel method between *your* two servers, in both directions, and ranks them by what users actually feel: download and upload speed, how steady that speed is, and whether the connection survives over time.

```
#   STATUS    MODE     METHOD    RTT ms   DOWN     UP  FLOOR    CV STALLS RESETS  SCORE
1 ● OK        reverse  tcp          84   92.1   41.0   71.3  0.08      0      0   88.4
2 ● OK        direct   kcp-fec      91   85.4   44.2   60.2  0.12      0      0   80.9
3 ● UNSTABLE  reverse  wss          88   70.3   35.8    4.1  0.61      9      2   43.0
4 ● DOWN      reverse  quic          -      -      -      -     -      -      -      -
```
*(illustrative output)*

## How it works

The same binary runs on both servers. The Iran side (`run`) is the controller; the Kharej side (`join`) is an agent that connects back to it. For each method TunnelBench tests:

| Mode | Who dials | Matches |
| --- | --- | --- |
| `reverse` | Kharej → Iran | Reverse tunnels (BackPack, Backhaul, Rathole…) |
| `direct` | Iran → Kharej | Direct tunnels and plain client→server setups |

Each method goes through three stages:

1. **Probe** — 10 connections with echo pings (latency, success rate) and a 2 MB burst, which catches filters that kill a connection after a few packets.
2. **Benchmark** — download and upload, single stream and 4 parallel streams, measured every second at the receiving side.
3. **Soak** — the 3 best methods keep downloading for 5 minutes, reconnecting after every reset. This exposes duration-based kills and throttling that short tests miss.

**DOWN** is Kharej → Iran (what users download in Instagram and Telegram); **UP** is Iran → Kharej. **FLOOR** is the 10th-percentile second, i.e. the speed on a bad second. **CV** is how much the speed varies (lower is steadier). The score weights download 30%, upload 15%, floor 20%, stability 20% and latency 15%, each relative to the best method in the same run.

## Methods

| Method | Family | Notes |
| --- | --- | --- |
| `tcp` | TCP | Baseline |
| `tcp-mux` | TCP | smux streams over one TCP connection |
| `tls` | TLS | Configurable SNI (`-sni`) |
| `ws`, `wss` | WebSocket | Binary frames; `wss` adds TLS |
| `ws-mux`, `wss-mux` | WebSocket | smux over WebSocket |
| `kcp` | UDP | KCP fast mode, AES |
| `kcp-fec` | UDP | KCP with Reed-Solomon FEC 10:3 |
| `quic` | QUIC | TLS 1.3, HTTP/3 ALPN |

Planned: raw UDP, Noise stealth, gRPC/HTTP2, fragmented TLS, raw-packet (fake TCP, KCP over raw TCP), ICMP, then real engines (BackPack, Backhaul, GOST, Rathole, kernel GRE/SIT, WireGuard, Hysteria2, paqet…) and an Xray end-to-end test. Comparing a protocol probe with an engine on the same protocol shows whether the filter blocks the protocol or fingerprints one engine.

## Quick start

On **both** servers (Ubuntu, amd64 or arm64):

```bash
bash <(curl -fsSL https://raw.githubusercontent.com/Art4Man/tunnelbench/main/install.sh)
```

If the Iran server cannot reach GitHub, download `tunnelbench_linux_amd64.tar.gz`, `SHA256SUMS` and `install.sh` from the [releases page](https://github.com/Art4Man/tunnelbench/releases) on another machine, `scp` them to the server and run `sudo bash install.sh` in that folder.

**1. Iran server** — start the run; it prints a link:

```bash
sudo tunnelbench run
```

**2. Kharej server** — run `join` and paste the link when it asks (this keeps the link out of shell history and `ps`):

```bash
sudo tunnelbench join
Paste the link from the Iran server: tb://<token>@<iran-ip>:20000?fp=<fingerprint>&sni=www.speedtest.net
```

The test starts as soon as the agent joins. Results are printed as a table and saved to `tunnelbench-runs/<time>/result.json`; `tunnelbench show <file>` prints a saved run again.

Open the control port and the ports after it (TCP **and** UDP) on both firewalls; `run` prints the exact range. With `ufw`:

```bash
sudo ufw allow 20000:20040/tcp && sudo ufw allow 20000:20040/udp
```

### Useful flags (`tunnelbench run -h`)

| Flag | Default | Meaning |
| --- | --- | --- |
| `-quick` | off | 8 s transfers, 5 probes, no soak (about 10 minutes) |
| `-methods` | all | e.g. `-methods kcp-fec,wss,tcp` |
| `-modes` | `reverse,direct` | test one direction only |
| `-bench` | `20s` | single-stream transfer length |
| `-streams` | `4` | parallel streams |
| `-soak` / `-soak-top` | `5m` / `3` | soak length and how many methods get one |
| `-sni` | `www.speedtest.net` | SNI for TLS-family methods |
| `-listen` | `0.0.0.0:20000` | control address; test ports follow it |
| `-public-ip` | auto | Iran address the agent should dial |

A full default run takes roughly 30–45 minutes; `-quick` takes about 10.

## Security of the pairing link

The link (`tb://<token>@<iran-ip>:<port>?fp=<fingerprint>&sni=…`) is a one-time credential for one run.

| Protection | What it does |
| --- | --- |
| Single-use token | A new 128-bit token for every `run`, kept only in memory; when the run ends the control port closes and the link is dead |
| Certificate pinning | `fp` is the SHA-256 of the Iran server's certificate; the agent refuses any other server, so the control channel cannot be intercepted |
| Session-bound mutual authentication | Both sides prove they know the token with an HMAC over the TLS session's exported key (TLS 1.3), so a proof cannot be replayed or relayed |
| One agent per run | The first agent to join owns the run; another agent with a valid link is refused and the Iran side prints a warning |
| Agent limits | The agent only dials the Iran host from the link, only listens on the ports after the control port, and caps tests at 1 h, 100 probes and 16 streams |
| Agent gives up | Without contact from the Iran server for 10 minutes, the agent exits |
| Authenticated test ports | Every test connection must present an HMAC of the token; KCP traffic is encrypted with a key derived from it |

TunnelBench never runs commands or reads files on either server. Treat the link like a one-time password anyway: do not post it in chats or screenshots.

## Development

```bash
make build     # build for this machine
make test      # unit + loopback tests for every method
make local     # run the whole benchmark over 127.0.0.1
make linux     # static Linux binaries in dist/ to scp to servers
```

Code layout:

```
cmd/tunnelbench/      CLI: run, join, local, show, methods
internal/transport/   one file per method, all behind one interface
internal/measure/     test protocol: echo pings, framed transfers, per-second sampling
internal/plan/        one test stage, run by whichever side dials
internal/control/     command channel; the agent reconnects and requests are de-duplicated
internal/runner/      orchestration, status rules and scoring
internal/report/      table and result.json
```

Adding a method = one new file in `internal/transport/` that implements `Transport` and calls `Register` in `init()`; the measurement and scoring pick it up automatically.

Releases are built by GitHub Actions with GoReleaser when a `v*` tag is pushed.

## License

MIT
