# Bifrost (with zai-web) — Quickstart

This is a fork of [Bifrost](https://github.com/maximhq/bifrost) with a
custom `zai-web` provider that proxies to chat.z.ai. Everything else is
identical to upstream Bifrost.

> 📖 Full upstream docs: [`README.md`](./README.md)
> 📖 zai-web client docs: [`docs/quickstart/gateway/zai-web.mdx`](./docs/quickstart/gateway/zai-web.mdx)
> 📖 zai-web internals: [`core/providers/zaiweb/README.md`](./core/providers/zaiweb/README.md)

---

## TL;DR — running it

```bash
./start.sh
```

That's it. Open `http://localhost:8080` in your browser.

The script handles all of:

- Checking that Go 1.26+ is installed
- Setting up the Go workspace (`go.work` pointing to local modules)
- Building the `bifrost-http` binary (only if missing or sources changed)
- Starting the gateway

## Prerequisites

- **Go 1.26+** — check with `go version`. Install from <https://go.dev/dl/> if needed.
- **Node.js + npm** — for the UI build (`brew install node` on Mac).
- **macOS / Linux** — Windows users can use WSL2.
- **Xcode Command Line Tools** (Mac only) — for CGO. `xcode-select --install`.

## Common operations

| What | Command |
|---|---|
| Run normally | `./start.sh` |
| Force rebuild | `./start.sh --rebuild` |
| Use a different port | `./start.sh --port 9090` |
| Hot-reload dev mode | `./start.sh --dev` |
| Show help | `./start.sh --help` |
| Stop | `Ctrl+C` |

## First-time configuration (zai-web)

After `./start.sh` is running, configure the zai-web provider:

1. Get your Z.ai session token:
   - Open <https://chat.z.ai> in Chrome → log in.
   - DevTools (F12) → Application → Cookies → `https://chat.z.ai`.
   - Copy the value of the `token` cookie (a long JWT).

2. In Bifrost UI at `http://localhost:8080/workspace/providers`:
   - Click **Add Provider** → select **Z.ai Web**.
   - Paste the token in **API Key** (no `Bearer ` prefix).
   - Set **Model** = `GLM-5.1` (or `GLM-5`).
   - Save.

3. Test:

   ```bash
   curl http://localhost:8080/v1/chat/completions \
     -H 'Content-Type: application/json' \
     -d '{
       "model": "zai-web/GLM-5.1",
       "messages": [{"role":"user","content":"hi"}],
       "stream": true
     }'
   ```

## Reasoning / thinking mode

Pass `reasoning_effort` to enable GLM's chain-of-thought:

```bash
curl http://localhost:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "zai-web/GLM-5.1",
    "messages": [{"role":"user","content":"What is 17 * 23?"}],
    "stream": true,
    "reasoning_effort": "medium"
  }'
```

Reasoning chunks come through as `delta.reasoning`; the final answer as
`delta.content`.

See [`docs/quickstart/gateway/zai-web.mdx`](./docs/quickstart/gateway/zai-web.mdx)
for full client SDK examples (Python, Node, Cline, etc.).

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `Go 1.x.y is too old` | Go < 1.26 | Install Go 1.26+ from <https://go.dev/dl/> |
| `go.work missing — running setup-workspace` | First run | Normal — script sets it up automatically |
| `cannot use zaiweb.NewZaiWebProvider...` | Stale build artifacts | `./start.sh --rebuild` |
| `401 Unauthorized` from Z.ai | Token expired / revoked | Re-login to chat.z.ai, paste new token in UI |
| `426 client outdated` | `X-FE-Version` missing | Already handled by adapter; should not happen |
| Binary downloads from npm instead of local | You ran `npx -y @maximhq/bifrost` | That fetches upstream pre-built. Use `./start.sh` instead |

## What's customized vs. upstream Bifrost

The fork adds a single new provider, `zai-web`:

| File | Change |
|---|---|
| `core/providers/zaiweb/zaiweb.go` | New — provider implementation (~600 LOC) |
| `core/providers/zaiweb/README.md` | New — developer overview |
| `core/schemas/bifrost.go` | Added `ZaiWeb ModelProvider = "zai-web"` constant |
| `core/bifrost.go` | Added `case schemas.ZaiWeb` in provider switch + import |
| `ui/lib/constants/logs.ts` | Added `"zai-web"` to `KnownProvidersNames` + label |
| `ui/lib/constants/config.ts` | Added zai-web to `isKeyRequiredByProvider` + `ModelPlaceholders` |
| `ui/lib/constants/icons.tsx` | Added zai-web SVG icon |
| `docs/quickstart/gateway/zai-web.mdx` | New — client docs |
| `docs/docs.json` | Registered zai-web in nav sidebar |
| `start.sh` | New — one-command startup wrapper |
| `QUICKSTART.md` | New — this file |

Everything else is identical to upstream Bifrost. To pull future upstream
updates, the fork should rebase cleanly except for these specific files.

## Credits

- Upstream gateway: [maximhq/bifrost](https://github.com/maximhq/bifrost) (Apache 2.0)
- zai-web adapter: implemented as a learning exercise; for production use Z.ai's
  official API at <https://api.z.ai>.
