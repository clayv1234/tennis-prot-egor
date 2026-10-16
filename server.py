import os,json,time,sqlite3,threading
from concurrent.futures import ThreadPoolExecutor,as_completed
from http.server import ThreadingHTTPServer,BaseHTTPRequestHandler
from urllib.parse import urlparse,parse_qs,urlencode
from urllib.request import Request,urlopen
from urllib.error import HTTPError
CACHE={"time":0,"data":None}
PLAYER_CACHE={}
PLAYER_CACHE_TTL=86400
ENRICHED_CACHE={}
RATE_LIMIT_UNTIL=0
DB_LOCK=threading.Lock()
def cache_conn():
 url=os.getenv("DATABASE_URL","")
 if url:
  import psycopg2
  return psycopg2.connect(url,connect_timeout=5),True
 conn=sqlite3.connect(os.getenv("CACHE_PATH","/tmp/tennis_prot_cache.sqlite"),timeout=10)
 return conn,False
def stored_get(key):
 try:
  with DB_LOCK:
   conn,pg=cache_conn()
   try:
    cur=conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS tennis_cache (key TEXT PRIMARY KEY, payload TEXT NOT NULL, saved DOUBLE PRECISION NOT NULL)")
    cur.execute("SELECT payload,saved FROM tennis_cache WHERE key="+("%s" if pg else "?"),(key,))
    row=cur.fetchone()
    conn.commit()
    return (json.loads(row[0]),row[1]) if row else None
   finally:conn.close()
 except Exception as e:
  print("cache read failure:",type(e).__name__,flush=True)
  return None
def stored_put(key,value):
 try:
  with DB_LOCK:
   conn,pg=cache_conn()
   try:
    cur=conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS tennis_cache (key TEXT PRIMARY KEY, payload TEXT NOT NULL, saved DOUBLE PRECISION NOT NULL)")
    cur.execute("INSERT INTO tennis_cache(key,payload,saved) VALUES ("+("%s,%s,%s" if pg else "?,?,?")+") ON CONFLICT(key) DO UPDATE SET payload=excluded.payload,saved=excluded.saved",(key,json.dumps(value),time.time()))
    conn.commit()
   finally:conn.close()
 except Exception as e:print("cache write failure:",type(e).__name__,flush=True)

BASE="https://api.livetennisapi.com/api/public/v1/"
class Handler(BaseHTTPRequestHandler):
 def send(self,status,body,ctype="application/json"):
  data=body.encode() if isinstance(body,str) else body
  self.send_response(status);self.send_header("Content-Type",ctype);self.send_header("Cache-Control","no-store");self.send_header("Content-Length",str(len(data)));self.end_headers();self.wfile.write(data)
 def do_GET(self):
  path=urlparse(self.path)
  if path.path in ("/","/index.html"):
   return self.send(200,open("index.html","rb").read(),"text/html; charset=utf-8")
  if path.path=="/manifest.webmanifest":
   return self.send(200,open("manifest.webmanifest","rb").read(),"application/manifest+json")
  if path.path=="/health":
   return self.send(200,json.dumps({"ok":True,"configured":bool(os.getenv("TENNIS_API_TOKEN"))}))
  if path.path=="/api/top10":return self.top10()
  if path.path=="/api/enriched":return self.enriched(path.query)
  if path.path!="/api/fixtures":return self.send(404,'{"error":"Not found"}')
  token=os.getenv("TENNIS_API_TOKEN")
  if not token:return self.send(503,'{"error":"API token not configured on server"}')
  q=parse_qs(path.query); args={k:v[0] for k,v in q.items() if k in ("draw","limit","offset","date")}
  args.setdefault("draw","singles");args.setdefault("limit","100");args["token"]=token
  try:
   with urlopen(Request(BASE+"fixtures?"+urlencode(args),headers={"Accept":"application/json"}),timeout=25) as r: return self.send(200,r.read())
  except HTTPError as e:return self.send(502,json.dumps({"error":"Tennis API HTTP "+str(e.code)}))
  except Exception as e:return self.send(502,json.dumps({"error":"Tennis API connection failed","detail":str(e)}))



 def enriched(self,query):
  global RATE_LIMIT_UNTIL
  token=os.getenv("TENNIS_API_TOKEN")
  if not token:return self.send(503,json.dumps({"error":"API token not configured"}))
  args=parse_qs(query)
  offset=min(900,max(0,int(args.get("offset",["0"])[0])))
  limit=min(50,max(1,int(args.get("limit",["25"])[0])))
  key=(offset,limit)
  old=ENRICHED_CACHE.get(key)
  if not old:
   saved=stored_get("enriched:"+str(offset)+":"+str(limit))
   if saved:old={"data":saved[0],"at":saved[1]}
  if old and (time.time()-old["at"]<3600 or time.time()<RATE_LIMIT_UNTIL):
   cached=dict(old["data"]);cached["meta"]=dict(cached["meta"]);cached["meta"]["cached"]=True;cached["meta"]["rate_limited"]=time.time()<RATE_LIMIT_UNTIL
   return self.send(200,json.dumps(cached))
  if time.time()<RATE_LIMIT_UNTIL:return self.send(429,json.dumps({"error":"Tennis API rate limit. Retry later; cached data unavailable.","retry_seconds":int(RATE_LIMIT_UNTIL-time.time())}))
  def get(endpoint):
   with urlopen(Request(BASE+endpoint,headers={"X-API-Key":token,"Accept":"application/json"}),timeout=20) as r:return json.load(r)
  def roster(m):
   p=m.get("players")
   if isinstance(p,dict):
    sides=[p.get("p1") or p.get("player1"),p.get("p2") or p.get("player2")]
   elif isinstance(p,list):sides=p[:2]
   else:sides=[m.get("player1") or m.get("home_player"),m.get("player2") or m.get("away_player")]
   while len(sides)<2:sides.append(None)
   output=[]
   for i,s in enumerate(sides):
    s=dict(s) if isinstance(s,dict) else {"name":s} if isinstance(s,str) else {}
    if not s.get("id"):
     s["id"]=s.get("player_id") or m.get("player"+str(i+1)+"_id")
    if not s.get("name"):
     s["name"]=s.get("full_name") or s.get("player_name") or m.get("player"+str(i+1)+"_name")
    output.append(s)
   return output
  try:
   raw=get("fixtures?"+urlencode({"draw":"singles","limit":limit,"offset":offset}))
   fixtures=raw.get("data",raw.get("fixtures",[])) if isinstance(raw,dict) else raw
   if not isinstance(fixtures,list):fixtures=[]
   ids=set()
   for m in fixtures:
    for p in roster(m):
     pid=p.get("id")
     if pid and str(pid).isdigit():ids.add(int(pid))
   for pid in ids:
    if pid not in PLAYER_CACHE:
     saved=stored_get("player:"+str(pid))
     if saved:PLAYER_CACHE[pid]={"profile":saved[0],"at":saved[1]}
   missing=[pid for pid in sorted(ids) if pid not in PLAYER_CACHE or time.time()-PLAYER_CACHE[pid]["at"]>PLAYER_CACHE_TTL]
   errors={}
   def fetch(pid):
    try:
     profile=get("players/"+str(pid))
     if isinstance(profile,dict) and isinstance(profile.get("data"),dict):profile=profile["data"]
     return pid,profile,None
    except HTTPError as e:
     if e.code==429:RATE_LIMIT_UNTIL=time.time()+900
     return pid,None,"HTTP "+str(e.code)
    except Exception as e:return pid,None,type(e).__name__
   # Fetch only two new profiles per batch to preserve API quota.
   with ThreadPoolExecutor(max_workers=3) as pool:
    futures=[pool.submit(fetch,pid) for pid in missing[:2]]
    for future in as_completed(futures):
     pid,profile,error=future.result()
     if isinstance(profile,dict):
      PLAYER_CACHE[pid]={"at":time.time(),"profile":profile}
      stored_put("player:"+str(pid),profile)
     elif error:errors[str(pid)]=error
   enriched=[];with_elo=0;with_names=0;with_stats=0
   for m in fixtures:
    copied=dict(m); sides=roster(m); profiles=[]
    for side in sides:
     pid=side.get("id")
     cached=PLAYER_CACHE.get(int(pid)) if pid and str(pid).isdigit() else None
     profile=cached["profile"] if cached else {}
     if profile:
      for key in ("name","ranking","ranking_points","ranking_movement","country"):
       if side.get(key) is None and profile.get(key) is not None:side[key]=profile[key]
      side["stats"]=profile.get("stats") or {}
      side["profile_source"]="LiveTennisAPI /players/{id}"
     if side.get("name"):with_names+=1
     if side.get("stats"):with_stats+=1
     rating=(side.get("stats") or {}).get("ratings")
     if rating is not None and "elo" in json.dumps(rating).lower():with_elo+=1
     profiles.append(side)
    copied["players"]=profiles
    enriched.append(copied)
   output={"data":enriched,"meta":{"fixtures":len(enriched),"player_ids":len(ids),"profiles_cached":sum(pid in PLAYER_CACHE for pid in ids),"profiles_with_stats":with_stats,"profiles_with_elo_fields":with_elo,"named_sides":with_names,"profiles_requested":min(2,len(missing)),"profile_errors":errors,"sample_fixture_fields":sorted(list(fixtures[0].keys())) if fixtures else [],"sample_player_fields":sorted(list(roster(fixtures[0])[0].keys())) if fixtures else [],"note":"Current profile Elo is not historical 30-day Elo momentum. Full serve/return comparisons need a separate data source."}}
   ENRICHED_CACHE[key]={"at":time.time(),"data":output}
   stored_put("enriched:"+str(offset)+":"+str(limit),output)
   return self.send(200,json.dumps(output))
  except HTTPError as e:
   if e.code==429:
    RATE_LIMIT_UNTIL=time.time()+900
    if old:
     cached=dict(old["data"]);cached["meta"]=dict(cached["meta"]);cached["meta"]["cached"]=True;cached["meta"]["rate_limited"]=True
     return self.send(200,json.dumps(cached))
    return self.send(429,json.dumps({"error":"Tennis API HTTP 429: quota exhausted. Automatic retry after cooldown.","retry_seconds":900}))
   return self.send(502,json.dumps({"error":"Fixture API HTTP "+str(e.code)}))
  except Exception as e:return self.send(502,json.dumps({"error":type(e).__name__+": "+str(e)[:140]}))

 def top10(self):
  if CACHE["data"] and time.time()-CACHE["time"]<3600:return self.send(200,json.dumps(CACHE["data"]))
  saved=stored_get("top10")
  if saved and time.time()-saved[1]<3600:return self.send(200,json.dumps(saved[0]))
  token=os.getenv("TENNIS_API_TOKEN")
  if not token:return self.send(503,json.dumps({"error":"API token missing"}))
  def get(endpoint):
   with urlopen(Request(BASE+endpoint,headers={"X-API-Key":token,"Accept":"application/json"}),timeout=15) as r:return json.load(r)
  if time.time()<RATE_LIMIT_UNTIL:
   if saved:return self.send(200,json.dumps(dict(saved[0],stale=True)))
   return self.send(200,json.dumps({"top10":[],"error":"Rate-limit cooldown: market requests paused to protect fixture quota"}))
  try:
   matches=[]
   for status in ("upcoming","live"):
    payload=get("matches?"+urlencode({"status":status,"draw":"singles","has_market":"true","limit":50}))
    matches.extend(payload.get("data",payload.get("matches",[])))
   results=[]; checked=0
   for m in matches[:15]:
    mid=m.get("id")
    if mid is None:continue
    checked+=1
    try:
     payload=get("markets/"+str(mid)+"/prices?limit=20")
     ticks=payload.get("prices",payload.get("data",[]))
     if not isinstance(ticks,list):continue
     by={}
     for tick in ticks:
      if not isinstance(tick,dict):continue
      side=tick.get("player") or tick.get("side") or tick.get("outcome")
      p=tick.get("implied_probability",tick.get("mid"))
      try:p=float(p)
      except (ValueError,TypeError):continue
      if p>1:p/=100
      if side is not None and 0<p<1 and str(side).lower() not in by:by[str(side).lower()]=(p,tick.get("timestamp"))
     a=m.get("player1_name") or ((m.get("players") or {}).get("p1") or {}).get("name")
     b=m.get("player2_name") or ((m.get("players") or {}).get("p2") or {}).get("name")
     if not a or not b:continue
     def find(name,i):
      for k in (str(name).lower(),str(i),"p"+str(i),str(m.get("player"+str(i)+"_id"))):
       if k in by:return by[k]
      return None
     p1,p2=find(a,1),find(b,2)
     if not p1 or not p2:continue
     total=p1[0]+p2[0]
     if total<=0:continue
     fav=a if p1[0]>=p2[0] else b
     pct=100*max(p1[0],p2[0])/total
     results.append({"player1":a,"player2":b,"favorite":fav,"probability":round(pct,1),"surface":m.get("surface"),"tour":m.get("tour"),"match_id":mid,"timestamp":p1[1] or p2[1]})
    except HTTPError as e:
     if e.code in (401,403,429):break
    except Exception:continue
   results.sort(key=lambda x:x["probability"],reverse=True)
   output={"top10":results[:10],"matched":len(matches),"checked":checked,"priced":len(results),"source":"LiveTennisAPI mapped market (NOT independent sharp sportsbook)","note":"A favorite by market price is not a model prediction. Price coverage may require PRO plan."}
   CACHE.update(time=time.time(),data=output)
   stored_put("top10",output)
   return self.send(200,json.dumps(output))
  except HTTPError as e:return self.send(200,json.dumps({"top10":[],"matched":0,"error":"Market lookup HTTP "+str(e.code),"note":"Market prices may require PRO access."}))
  except Exception as e:return self.send(200,json.dumps({"top10":[],"matched":0,"error":str(e)[:120]}))

if __name__=="__main__":
 ThreadingHTTPServer(("0.0.0.0",int(os.getenv("PORT","10000"))),Handler).serve_forever()
