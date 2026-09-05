# JanitorAI Multi-Provider Proxy v9

`JanitorAI → v9 → OpenCode Zen OR Bitdeer AI`

v9 keeps the proxy thin: one persistent aiohttp connection pool, direct OpenAI-compatible Chat Completions calls, native SSE passthrough, and a buffered SSE fallback.

## Routing

Global `provider_mode` can be `auto`, `opencode`, or `bitdeer`.

- `auto`: use `provider_order`, defaulting to OpenCode first.
- `opencode`: try every configured OpenCode key, then fall back to Bitdeer if those keys are rejected with a retryable status or the upstream is unavailable.
- `bitdeer`: same behavior in the opposite direction.

A single request can override the global choice with `X-Proxy-Provider: opencode` or `X-Proxy-Provider: bitdeer`.

Within each provider, v9 tries `KEY_1` through `KEY_9`. Retryable upstream statuses are `401, 402, 403, 408, 429, 500, 502, 503, 504`. Validation/model errors such as 400 are returned instead of silently masking them with another provider.

## Provider endpoints

OpenCode Zen uses `https://opencode.ai/zen/v1/chat/completions` with a Bearer API key. DeepSeek V4 Flash is currently listed by OpenCode as `deepseek-v4-flash` on this OpenAI-compatible endpoint.

Bitdeer uses `https://api-inference.bitdeer.ai/v1/chat/completions` with a Bearer API key. `deepseek-ai/DeepSeek-V4-Flash` is the Bitdeer-side model mapping used by this proxy.

## Environment variables

```text
OPENCODE_KEY_1 ... OPENCODE_KEY_9
BITDEER_KEY_1 ... BITDEER_KEY_9
PORT
```

For hosted deployment, environment variables are preferred over storing keys in `config.json`.

## JanitorAI

API URL:

```text
https://YOUR-HOST/v1/chat/completions
```

API key: the generated proxy key shown by the private panel.

Model:

```text
deepseek-v4-flash
```

## Railway / Render

Build: `pip install -r requirements.txt`

Start: `python server.py`

Health check: `/health`

Set `OPENCODE_KEY_1` and/or `BITDEER_KEY_1` in the host's environment variables.

## Research notes

The open-source `WyvernCW/FreeProxy` project demonstrates the exact useful OpenCode pattern: direct Zen `/chat/completions`, Bearer keys, multi-key failover and SSE streaming. The `12errh/zen-proxy` project additionally emphasizes persistent connections, OpenCode User-Agent handling, BYOK and `Retry-After`; v9 borrows those transport ideas without depending on either project.
