# zai-web provider

OpenAI-compatible adapter for **chat.z.ai's reverse-engineered web API**.
Lets you call GLM-5.1 / GLM-5 from any OpenAI client through Bifrost.

> ⚠️  **Learning / experimentation only.** Uses session tokens harvested
> from a logged-in browser, not official API keys. See the user-facing
> docs at [`docs/quickstart/gateway/zai-web.mdx`](../../../docs/quickstart/gateway/zai-web.mdx)
> for the full caveats.

## Quick start

```bash
# 1. Get token from chat.z.ai (DevTools → Cookies → "token")
export ZAI_TOKEN='eyJ...'

# 2. Configure provider in Bifrost
curl http://localhost:8080/api/providers \
  -H 'Content-Type: application/json' \
  -d '{
    "provider": "zai-web",
    "keys": [{"name":"k1","value":"env.ZAI_TOKEN","models":["GLM-5.1"],"weight":1.0}]
  }'

# 3. Use it
curl http://localhost:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "zai-web/GLM-5.1",
    "messages": [{"role":"user","content":"hi"}],
    "stream": true
  }'
```

## How the adapter works

The web endpoint accepts a minimal OpenAI-style body but returns SSE in a
custom envelope. The adapter handles the translation in both directions.

### Request transformation

```text
BifrostChatRequest                       Z.ai web body
{                                        {
  Model: "GLM-5.1",                        "model": "GLM-5.1",
  Input: [...messages],                    "messages": [...],
  Params.Reasoning.Enabled: true   ────►   "stream": true,
}                                          "features": {
                                             "enable_thinking": true
                                           }
                                         }
```

Headers:

- `Authorization: Bearer <ZAI_TOKEN>` — adapter prefixes "Bearer " automatically;
  paste the raw JWT into `Key.Value`.
- `X-FE-Version: prod-fe-1.1.22` — required by the server (without it, you
  get a `426 client outdated` error inside the SSE stream).

### Response transformation

Z.ai web sends SSE chunks like:

```text
data: {"type":"chat:completion","data":{"delta_content":"Hi","phase":"answer"}}
data: {"type":"chat:completion","data":{"phase":"other","usage":{...}}}
data: {"type":"chat:completion","data":{"phase":"done","done":true}}
```

The adapter maps each `phase` to OpenAI streaming format:

| Z.ai phase | OpenAI delta | Notes |
|---|---|---|
| `thinking` | `choices[0].delta.reasoning` | Chain-of-thought reasoning |
| `answer` | `choices[0].delta.content` | The actual reply |
| `other` (with `usage`) | chunk with `usage` populated, `choices: null` | Final token counts |
| `other` (with `edit_index` + `edit_content`) | additional `delta.content` chunk | Z.ai's retroactive edit primitive — emitted as a content append since OpenAI has no rewind primitive |
| `done` | `finish_reason: "stop"` + `data: [DONE]` | End marker |

Plus an `id` (`chatcmpl-<uuid>`) is synthesized per request since Z.ai web
doesn't return one.

## Thinking mode activation

The adapter accepts three input forms (priority order):

1. `Params.Reasoning.Enabled == true` — set via OpenAI `reasoning: {enabled: true}`.
2. `Params.Reasoning.Effort != "" && != "none"` — set via OpenAI `reasoning_effort: "medium"`.
3. `Params.ExtraParams["enable_thinking"] == true` — escape hatch via top-level
   `enable_thinking: true` in the request body (Bifrost compat layer routes
   unknown top-level fields here).

All three set `features.enable_thinking: true` in the upstream Z.ai request.
GLM has no granular effort levels, so any non-`none` value enables thinking.

## Files

| File | Purpose |
|---|---|
| `zaiweb.go` | Single-file provider — types, request/response transforms, all 50+ Provider interface methods (most return `UnsupportedOperationError`). |
| `README.md` | This file — developer overview. |

## Wiring (already done; reference for future provider authors)

To add a new provider to Bifrost, three edits are needed:

1. **Constant** — `core/schemas/bifrost.go`:
   ```go
   ZaiWeb ModelProvider = "zai-web"
   ```

2. **Registration** — `core/bifrost.go` `createBaseProvider()` switch:
   ```go
   case schemas.ZaiWeb:
       return zaiweb.NewZaiWebProvider(config, bifrost.logger)
   ```

3. **UI registration** — three files:
   - `ui/lib/constants/logs.ts` — `KnownProvidersNames` array + `ProviderLabels`.
   - `ui/lib/constants/config.ts` — `isKeyRequiredByProvider` Record + `ModelPlaceholders`.
   - `ui/lib/constants/icons.tsx` — `ProviderIcons` map (use SVG or a lucide icon).

The TS types in `ui/lib/types/config.ts` (notably `ModelProviderName`) auto-derive
from `KnownProvidersNames`, so no edits are needed there.

## License & legal

Same license as Bifrost upstream (Apache 2.0). The provider implementation
is original work; no Z.ai code is included.

The adapter does not bypass any technical access controls — it uses
documented browser HTTP APIs with credentials the user already possesses.
Whether that's permissible under Z.ai's ToS is a separate question; see the
warning in the user-facing docs.
