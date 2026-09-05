from __future__ import annotations
import asyncio, json, os, time
from aiohttp import web, ClientSession, ClientTimeout, TCPConnector

RETRY = {401, 402, 403, 408, 429, 500, 502, 503, 504}
MODELS = {
    "deepseek-v4-flash": ("deepseek-v4-flash", "deepseek-ai/DeepSeek-V4-Flash"),
    "deepseek-v4-pro": ("deepseek-v4-pro", "deepseek-ai/DeepSeek-V4-Pro"),
}
OPENCODE = "https://opencode.ai/zen/v1"
BITDEER = "https://api-inference.bitdeer.ai/v1"
PROXY_KEY = os.getenv("PROXY_API_KEY", "").strip()
if not PROXY_KEY:
    raise RuntimeError("PROXY_API_KEY is required")
SESSION = None


def keys(provider):
    prefix = "OPENCODE_KEY_" if provider == "opencode" else "BITDEER_KEY_"
    return [v.strip() for i in range(1, 10) if (v := os.getenv(prefix + str(i), "").strip())]


def providers():
    mode = os.getenv("PROVIDER_MODE", "auto").lower()
    order = ["opencode", "bitdeer"] if mode not in ("opencode", "bitdeer") else [mode, "bitdeer" if mode == "opencode" else "opencode"]
    return [p for p in order if keys(p)]


def model_for(model, provider):
    low = str(model or "deepseek-v4-flash").lower()
    if low.endswith("/deepseek-v4-flash"): low = "deepseek-v4-flash"
    if low.endswith("/deepseek-v4-pro"): low = "deepseek-v4-pro"
    if low in MODELS: return MODELS[low][0 if provider == "opencode" else 1], low
    return model or "deepseek-v4-flash", model or "deepseek-v4-flash"


def auth(req):
    value = req.headers.get("Authorization", "")
    token = value[7:].strip() if value.lower().startswith("bearer ") else value.strip()
    token = token or req.headers.get("X-API-Key", "").strip()
    return bool(token) and token == PROXY_KEY


def upstream_headers(provider, key, stream):
    h = {"Authorization": f"Bearer {key}", "Content-Type": "application/json", "Accept": "text/event-stream" if stream else "application/json"}
    if provider == "opencode":
        h["User-Agent"] = os.getenv("OPENCODE_USER_AGENT", "OpenCode/1.2.31")
    return h


async def get_upstream(provider, payload):
    base = OPENCODE if provider == "opencode" else BITDEER
    url = base + "/chat/completions"
    last = (502, "upstream unavailable")
    for slot, key in enumerate(keys(provider), 1):
        try:
            r = await SESSION.post(url, headers=upstream_headers(provider, key, True), json=payload)
            if 200 <= r.status < 300:
                print(f"UPSTREAM OK provider={provider} slot={slot} status={r.status}", flush=True)
                return r, slot, last
            text = (await r.text())[:800]
            last = (r.status, text or r.reason)
            print(f"UPSTREAM FAIL provider={provider} slot={slot} status={r.status} body={text[:300]!r}", flush=True)
            r.release()
            if r.status not in RETRY:
                return None, slot, last
        except Exception as e:
            last = (502, str(e)[:500])
            print(f"UPSTREAM EXCEPTION provider={provider} slot={slot} error={last[1]!r}", flush=True)
    return None, 0, last


async def get_buffered(provider, payload):
    base = OPENCODE if provider == "opencode" else BITDEER
    url = base + "/chat/completions"
    last = (502, "upstream unavailable")
    for slot, key in enumerate(keys(provider), 1):
        try:
            r = await SESSION.post(url, headers=upstream_headers(provider, key, False), json=payload)
            text = await r.text(); status = r.status
            if 200 <= status < 300:
                print(f"UPSTREAM OK provider={provider} slot={slot} status={status}", flush=True)
                try: return json.loads(text), status, slot
                except Exception: return None, 502, slot
            last = (status, text[:800])
            print(f"UPSTREAM FAIL provider={provider} slot={slot} status={status} body={text[:300]!r}", flush=True)
            r.release()
            if status not in RETRY: return None, status, slot
        except Exception as e:
            last = (502, str(e)[:500])
            print(f"UPSTREAM EXCEPTION provider={provider} slot={slot} error={last[1]!r}", flush=True)
    return None, last[0], 0


def error(status, message):
    return web.json_response({"error": {"message": message, "type": "proxy_error"}}, status=status)


async def chat(req):
    if not auth(req): return error(401, "Invalid proxy API key")
    try: body = await req.json()
    except Exception: return error(400, "Invalid JSON")
    if not isinstance(body, dict): return error(400, "JSON object required")
    override = (req.headers.get("X-Proxy-Provider") or req.query.get("provider") or "").lower()
    order = [override] + [p for p in providers() if p != override] if override in ("opencode", "bitdeer") else providers()
    requested = str(body.get("model") or "deepseek-v4-flash")
    stream = bool(body.get("stream"))
    print(f"REQUEST model={requested} stream={stream} order={order}", flush=True)
    for provider in order:
        if not keys(provider): continue
        upstream_model, client_model = model_for(requested, provider)
        payload = dict(body); payload["model"] = upstream_model; payload["stream"] = stream
        print(f"TRY provider={provider} model={upstream_model} keys={len(keys(provider))}", flush=True)
        if stream:
            r, slot, problem = await get_upstream(provider, payload)
            if r is None: continue
            resp = web.StreamResponse(status=200, headers={"Content-Type":"text/event-stream","Cache-Control":"no-cache","Connection":"keep-alive","X-Proxy-Provider":provider,"X-Proxy-Key-Slot":str(slot)})
            await resp.prepare(req)
            try:
                async for chunk in r.content.iter_any(): await resp.write(chunk)
            except (ConnectionResetError, asyncio.CancelledError): pass
            finally:
                r.release()
                try: await resp.write_eof()
                except Exception: pass
            return resp
        data, status, slot = await get_buffered(provider, payload)
        if data is not None:
            data["model"] = client_model
            return web.json_response(data, headers={"X-Proxy-Provider":provider,"X-Proxy-Key-Slot":str(slot)})
        if status not in RETRY: return error(status, "Upstream request failed")
    return error(502, "All configured providers/keys failed")


async def health(req):
    return web.json_response({"ok":True,"service":"janitorai-multiproxy-v9","providers":{"opencode":bool(keys("opencode")),"bitdeer":bool(keys("bitdeer"))}})

async def models(req):
    if not auth(req): return error(401,"Invalid proxy API key")
    return web.json_response({"object":"list","data":[{"id":m,"object":"model","owned_by":"janitor-proxy"} for m in MODELS]})

async def init(app):
    global SESSION
    SESSION = ClientSession(timeout=ClientTimeout(total=None, connect=12, sock_read=300), connector=TCPConnector(limit=64, limit_per_host=32, ttl_dns_cache=300, force_close=False))
    yield
    await SESSION.close()

app = web.Application()
app.router.add_get("/health", health)
app.router.add_get("/v1/models", models)
app.router.add_post("/v1/chat/completions", chat)
app.cleanup_ctx.append(init)

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    print(f"JanitorAI Multi-Provider Proxy v9 FIX listening on 0.0.0.0:{port}")
    print(f"Providers: OpenCode={len(keys('opencode'))} keys, Bitdeer={len(keys('bitdeer'))} keys")
    web.run_app(app, host="0.0.0.0", port=port, access_log=True)