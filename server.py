import os,json,time
from concurrent.futures import ThreadPoolExecutor,as_completed
from http.server import ThreadingHTTPServer,BaseHTTPRequestHandler
from urllib.parse import urlparse,parse_qs,urlencode
from urllib.request import Request,urlopen
from urllib.error import HTTPError
CACHE={"time":0,"data":None}
PLAYER_CACHE={}
PLAYER_CACHE_TTL=21600
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
  token=os.getenv("TENNIS_API_TOKEN")
  if not token:return self.send(503,json.dumps({"error":"API token not configured"}))
  args=parse_qs(query)
  offset=min(900,max(0,int(args.get("offset",["0"])[0])))
  limit=min(40,max(1,int(args.get("limit",["25"])[0])))
  def get(endpoint):
   with urlopen(Request(BASE+endpoint,headers={"X-API-Key":token,"Accept":"application/json"}),timeout=18) as r:return json.load(r)
  try:
   result=get("fixtures?"+urlencode({"draw":"singles","limit":limit,"offset":offset}))
   fixtures=result.get("data",result.get("fixtures",[])) if isinstance(result,dict) else result
   if not isinstance(fixtures,list):fixtures=[]
   ids=set()
   def players(m):
    p=m.get("players") or {}
    if isinstance(p,dict):
     return [p.get("p1"),p.get("p2")]
    if isinstance(p,list):return p[:2]
    return [m.get("player1") or m.get("home_player"),m.get("player2") or m.get("away_player")]
   for m in fixtures:
    for p in players(m):
     if not isinstance(p,dict):continue
     pid=p.get("id") or p.get("player_id")
     if isinstance(pid,int) or (isinstance(pid,str) and pid.isdigit()):ids.add(int(pid))
   missing=[pid for pid in ids if pid not in PLAYER_CACHE or time.time()-PLAYER_CACHE[pid]["at"]>PLAYER_CACHE_TTL]
   def fetch(pid):
    try:
     profile=get("players/"+str(pid))
     if isinstance(profile,dict) and isinstance(profile.get("data"),dict):profile=profile["data"]
     return pid,profile,None
    except HTTPError as e:return pid,None,"HTTP "+str(e.code)
    except Exception as e:return pid,None,type(e).__name__
   errors={}
   # Cap profile reads to avoid burning a user's API quota in one scan.
   with ThreadPoolExecutor(max_workers=3) as pool:
    futures=[pool.submit(fetch,pid) for pid in missing[:24]]
    for future in as_completed(futures):
     pid,profile,error=future.result()
     if profile:PLAYER_CACHE[pid]={"at":time.time(),"profile":profile}
     elif error:errors[str(pid)]=error
   enriched=[]
   for m in fixtures:
    sides=players(m)
    copied=dict(m)
    names=[]
    for i,p in enumerate(sides):
     pid=(p.get("id") or p.get("player_id")) if isinstance(p,dict) else None
     cached=PLAYER_CACHE.get(int(pid)) if pid and str(pid).isdigit() else None
     profile=cached["profile"] if cached else None
     side=dict(p) if isinstance(p,dict) else {"name":str(p)} if p else {}
     if profile:
      side["stats"]=profile.get("stats") or {}
      side["ranking"]=profile.get("ranking")
      side["profile_source"]="GET /players/"+str(pid)
     names.append(side)
    copied["players"]=names
    enriched.append(copied)
   stats_found=sum(1 for m in enriched for p in m["players"] if p.get("stats"))
   output={"data":enriched,"meta":{"fixtures":len(enriched),"player_ids":len(ids),"profiles_cached":sum(pid in PLAYER_CACHE for pid in ids),"profiles_with_stats":stats_found,"profiles_requested":min(24,len(missing)),"profile_errors":errors,"note":"Cached player profiles provide season and ratings fields only where supplied. Full ten-category serve/return stats may be absent."}}
   return self.send(200,json.dumps(output))
  except HTTPError as e:return self.send(502,json.dumps({"error":"Fixture API HTTP "+str(e.code)}))
  except Exception as e:return self.send(502,json.dumps({"error":type(e).__name__+": "+str(e)[:140]}))

 def top10(self):
  if CACHE["data"] and time.time()-CACHE["time"]<300:return self.send(200,json.dumps(CACHE["data"]))
  token=os.getenv("TENNIS_API_TOKEN")
  if not token:return self.send(503,json.dumps({"error":"API token missing"}))
  def get(endpoint):
   with urlopen(Request(BASE+endpoint,headers={"X-API-Key":token,"Accept":"application/json"}),timeout=15) as r:return json.load(r)
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
   return self.send(200,json.dumps(output))
  except HTTPError as e:return self.send(200,json.dumps({"top10":[],"matched":0,"error":"Market lookup HTTP "+str(e.code),"note":"Market prices may require PRO access."}))
  except Exception as e:return self.send(200,json.dumps({"top10":[],"matched":0,"error":str(e)[:120]}))

if __name__=="__main__":
 ThreadingHTTPServer(("0.0.0.0",int(os.getenv("PORT","10000"))),Handler).serve_forever()
