from __future__ import annotations

import asyncio, json, os, secrets, time, uuid
from pathlib import Path
from collections import deque
from aiohttp import web, ClientSession, ClientTimeout, TCPConnector

ROOT = Path(__file__).resolve().parent
CFG_FILE = ROOT / "config.json"
RETRY = {401, 402, 403, 408, 429, 500, 502, 503, 504}
MODELS = {
    "deepseek-v4-flash": {"opencode": "deepseek-v4-flash", "bitdeer": "deepseek-ai/DeepSeek-V4-Flash"},
    "deepseek-v4-pro": {"opencode": "deepseek-v4-pro", "bitdeer": "deepseek-ai/DeepSeek-V4-Pro"},
}
DEFAULT = {
    "provider_mode": "auto", "provider_order": ["opencode", "bitdeer"],
    "opencode_base": "https://opencode.ai/zen/v1", "opencode_model": "deepseek-v4-flash",
    "opencode_user_agent": "OpenCode/1.2.31",
    "bitdeer_base": "https://api-inference.bitdeer.ai/v1", "bitdeer_model": "deepseek-ai/DeepSeek-V4-Flash",
    "janitor_model": "deepseek-v4-flash", "proxy_api_key": "", "proxy_port": 8080,
}

def load():
    c = dict(DEFAULT)
    try: c.update(json.loads(CFG_FILE.read_text()))
    except Exception: pass
    env_key = os.getenv("PROXY_API_KEY", "").strip()
    if env_key:
        c["proxy_api_key"] = env_key
    elif not c.get("proxy_api_key"):
        c["proxy_api_key"] = "jk_" + secrets.token_urlsafe(27)
    save(c); return c

def save(c):
    tmp = CFG_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(c, indent=2) + "\n")
    os.replace(tmp, CFG_FILE)
    try: os.chmod(CFG_FILE, 0o600)
    except OSError: pass

cfg = load()
events = deque(maxlen=80)
stats = {"requests_total":0,"requests_stream":0,"requests_buffered":0,"native_stream":0,"buffered_fallback":0,"upstream_errors":0,"provider_switches":0,"active_requests":0,"last_provider":"-","last_status":0,"last_duration_ms":0,"last_ttfb_ms":0}
session: ClientSession | None = None

def keys(p):
    prefix = "OPENCODE_KEY_" if p == "opencode" else "BITDEER_KEY_"
    out=[]
    for i in range(1,10):
        v=os.getenv(prefix+str(i),"").strip() or str(cfg.get(p+"_key_"+str(i),"")).strip()
        if v and v not in out: out.append(v)
    return out

def configured(p): return bool(keys(p))
def order(override=None):
    if override in ("opencode","bitdeer"): first=override
    else:
        mode=cfg.get("provider_mode","auto")
        first=mode if mode in ("opencode","bitdeer") else (cfg.get("provider_order") or ["opencode"])[0]
    rest=[p for p in (cfg.get("provider_order") or ["opencode","bitdeer"]) if p != first]
    seq=[first]+rest
    return [p for p in seq if configured(p)] or seq

def model_for(name,p):
    name=name.strip() or cfg["janitor_model"]
    low=name.lower()
    if low in MODELS: return MODELS[low][p], low
    if low.endswith("/deepseek-v4-flash"): return MODELS["deepseek-v4-flash"][p], "deepseek-v4-flash"
    if low.endswith("/deepseek-v4-pro"): return MODELS["deepseek-v4-pro"][p], "deepseek-v4-pro"
    return name, name

def event(kind, **x): events.appendleft({"time":time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime()),"event":kind,**x})

def authorized(req):
    a=req.headers.get("Authorization","")
    token=a[7:].strip() if a.lower().startswith("bearer ") else a.strip()
    token=token or req.headers.get("X-API-Key","").strip()
    expected=os.getenv("PROXY_API_KEY","").strip() or cfg.get("proxy_api_key","")
    return bool(token and expected and secrets.compare_digest(token,expected))

def headers(p,key):
    h={"Authorization":"Bearer "+key,"Content-Type":"application/json","Accept":"text/event-stream"}
    if p=="opencode": h["User-Agent"]=cfg.get("opencode_user_agent","OpenCode/1.2.31")
    return h

def err(status,msg): return web.json_response({"error":{"message":msg,"type":"proxy_error"}},status=status)

async def upstream_stream(p,payload):
    assert session
    base=cfg["opencode_base"] if p=="opencode" else cfg["bitdeer_base"]
    url=base.rstrip("/")+"/chat/completions"; last=(502,"upstream unavailable")
    for n,key in enumerate(keys(p),1):
        try:
            r=await session.post(url,headers=headers(p,key),json=payload)
            if 200 <= r.status < 300: return r,n
            body=(await r.text())[:500]; status=r.status; last=(status,body or r.reason)
            stats["upstream_errors"]+=1; event("upstream_error",provider=p,key_slot=n,status=status)
            await r.release()
            if status not in RETRY: return None,n,last
        except Exception as e:
            last=(502,str(e)[:300]); stats["upstream_errors"]+=1; event("upstream_exception",provider=p,key_slot=n,message=str(e)[:200])
    return None,0,last

async def buffered(p,payload):
    assert session
    base=cfg["opencode_base"] if p=="opencode" else cfg["bitdeer_base"]
    url=base.rstrip("/")+"/chat/completions"; last=(502,"upstream unavailable")
    for n,key in enumerate(keys(p),1):
        try:
            r=await session.post(url,headers={**headers(p,key),"Accept":"application/json"},json=payload)
            text=await r.text(); last=(r.status,text[:500])
            if 200<=r.status<300:
                try: return json.loads(text),r.status
                except Exception: return None,r.status
            stats["upstream_errors"]+=1; event("upstream_error",provider=p,key_slot=n,status=r.status)
            if r.status not in RETRY: return None,r.status
        except Exception as e: last=(502,str(e)[:300])
    return None,last[0]

def chunk(obj,model):
    return ("data: "+json.dumps({"id":obj.get("id","chatcmpl-"+uuid.uuid4().hex),"object":"chat.completion.chunk","created":obj.get("created",int(time.time())),"model":model,"choices":[{"index":0,"delta":{"content":obj.get("choices",[{}])[0].get("message",{}).get("content","")},"finish_reason":"stop"}]},separators=(",",":"))+"\n\ndata: [DONE]\n\n").encode()

async def chat(req):
    if not authorized(req): return err(401,"Invalid proxy API key")
    try: body=await req.json()
    except Exception: return err(400,"Invalid JSON")
    if not isinstance(body,dict): return err(400,"JSON object required")
    override=req.headers.get("X-Proxy-Provider","").lower() or req.query.get("provider","").lower()
    if override not in ("opencode","bitdeer"): override=None
    seq=order(override); requested=str(body.get("model") or cfg["janitor_model"]); stream=bool(body.get("stream"))
    started=time.monotonic(); stats["requests_total"]+=1; stats["active_requests"]+=1
    try:
        for idx,p in enumerate(seq):
            upstream_model,client_model=model_for(requested,p); payload=dict(body); payload["model"]=upstream_model
            if stream:
                payload["stream"]=True; r,slot,problem=await upstream_stream(p,payload)
                if r is not None:
                    if idx: stats["provider_switches"]+=1
                    stats["requests_stream"]+=1; stats["native_stream"]+=1; stats["last_provider"]=p
                    resp=web.StreamResponse(status=200,headers={"Content-Type":"text/event-stream","Cache-Control":"no-cache","X-Proxy-Provider":p,"X-Proxy-Key-Slot":str(slot),"X-Accel-Buffering":"no"})
                    await resp.prepare(req); ttfb=None
                    try:
                        async for data in r.content.iter_any():
                            if ttfb is None: ttfb=(time.monotonic()-started)*1000; stats["last_ttfb_ms"]=round(ttfb)
                            await resp.write(data)
                    except (ConnectionResetError,asyncio.CancelledError): pass
                    finally: r.release()
                    stats["last_status"]=200; return resp
                event("provider_failed",provider=p)
            else:
                full,status=await buffered(p,payload)
                if full is not None:
                    if idx: stats["provider_switches"]+=1
                    stats["requests_buffered"]+=1; stats["last_provider"]=p; stats["last_status"]=status; full["model"]=client_model
                    return web.json_response(full,headers={"X-Proxy-Provider":p})
                if status not in RETRY: return err(status,"Upstream request failed")
        return err(502,"All configured providers/keys failed")
    finally:
        stats["active_requests"]-=1; stats["last_duration_ms"]=round((time.monotonic()-started)*1000)

async def models(req):
    if not authorized(req): return err(401,"Invalid proxy API key")
    return web.json_response({"object":"list","data":[{"id":m,"object":"model","owned_by":"janitor-proxy"} for m in MODELS]})

async def health(req): return web.json_response({"ok":True,"service":"janitorai-multiproxy-v9","providers":{"opencode":configured("opencode"),"bitdeer":configured("bitdeer")}})

async def status(req):
    if not authorized(req): return err(401,"Invalid proxy API key")
    return web.json_response({"provider_mode":cfg["provider_mode"],"provider_order":cfg["provider_order"],"janitor_model":cfg["janitor_model"],"proxy_api_key":cfg["proxy_api_key"],"janitor_url":public_url(req),"providers":{"opencode":{"configured":configured("opencode"),"keys":len(keys("opencode"))},"bitdeer":{"configured":configured("bitdeer"),"keys":len(keys("bitdeer"))}},"stats":stats})

def public_url(req): return str(cfg.get("public_url") or f"{req.scheme}://{req.host}")+"/v1/chat/completions"

async def settings(req):
    if not authorized(req): return err(401,"Invalid proxy API key")
    try: data=await req.json()
    except Exception: return err(400,"Invalid JSON")
    for k in ("provider_mode","opencode_model","opencode_user_agent","bitdeer_model","janitor_model"):
        if k in data: cfg[k]=str(data[k])
    if isinstance(data.get("provider_order"),list): cfg["provider_order"]=[p for p in data["provider_order"] if p in ("opencode","bitdeer")]
    for p in ("opencode","bitdeer"):
        for i in range(1,10):
            k=f"{p}_key_{i}"
            if k in data and str(data[k]).strip(): cfg[k]=str(data[k]).strip()
    save(cfg); return web.json_response({"ok":True})

async def test(req):
    if not authorized(req): return err(401,"Invalid proxy API key")
    data=await req.json(); ps=[data.get("provider")] if data.get("provider") in ("opencode","bitdeer") else order(); out=[]
    for p in ps:
        if not configured(p): out.append({"provider":p,"ok":False,"message":"No key"}); continue
        t=time.monotonic(); payload={"model":model_for("deepseek-v4-flash",p)[0],"messages":[{"role":"user","content":"Reply exactly: PROVIDER_TEST_OK"}],"stream":False,"max_tokens":20}
        full,status=await buffered(p,payload); out.append({"provider":p,"ok":bool(full),"status":status,"latency_ms":round((time.monotonic()-t)*1000)})
    return web.json_response({"ok":any(x["ok"] for x in out),"results":out})

async def init(app):
    global session
    session=ClientSession(timeout=ClientTimeout(total=None,connect=cfg.get("connect_timeout",12),sock_read=cfg.get("read_timeout",300)),connector=TCPConnector(limit=64,limit_per_host=32,ttl_dns_cache=300,force_close=False,enable_cleanup_closed=True))
    yield
    await session.close()

app=web.Application()
app.router.add_get("/health",health)
app.router.add_post("/v1/chat/completions",chat)
app.router.add_get("/v1/models",models)
app.router.add_get("/api/status",status)
app.router.add_post("/api/settings",settings)
app.router.add_post("/api/test",test)
app.router.add_get("/api/requests",lambda r:web.json_response({"events":list(events)}))
app.cleanup_ctx.append(init)

if __name__ == "__main__":
    port=int(os.getenv("PORT",cfg.get("proxy_port",8080)))
    print(f"JanitorAI Multi-Provider Proxy v9 listening on 0.0.0.0:{port}")
    print(f"Providers: OpenCode={len(keys('opencode'))} keys, Bitdeer={len(keys('bitdeer'))} keys")
    web.run_app(app,host="0.0.0.0",port=port,access_log=None)
