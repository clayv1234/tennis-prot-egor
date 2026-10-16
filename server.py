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
COOLDOWN_SECONDS=0
def blocked_until():
 return 0
def record_limit():
 print("upstream HTTP 429: provider rate limit; no app cooldown",flush=True)

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


BASE="https://api.citoapi.com/api/v1/tennis/"
SCAN_LOCK=threading.Lock()
MAX_DAILY_REQUESTS=25
QUOTA_LOCK=threading.Lock()
def consume_quota():
 from datetime import datetime,timezone
 day=datetime.now(timezone.utc).strftime("%Y-%m-%d")
 if not os.getenv("DATABASE_URL"):raise RuntimeError("DATABASE_URL is required for the 25/day safety limit")
 # Transactional row lock ensures multiple app instances share the same hard cap.
 import psycopg2
 with QUOTA_LOCK:
  conn=psycopg2.connect(os.environ["DATABASE_URL"],connect_timeout=5)
  try:
   cur=conn.cursor()
   cur.execute("CREATE TABLE IF NOT EXISTS cito_daily_usage (day TEXT PRIMARY KEY, used INTEGER NOT NULL)")
   cur.execute("INSERT INTO cito_daily_usage(day,used) VALUES (%s,0) ON CONFLICT(day) DO NOTHING",(day,))
   cur.execute("SELECT used FROM cito_daily_usage WHERE day=%s FOR UPDATE",(day,))
   used=cur.fetchone()[0]
   if used>=MAX_DAILY_REQUESTS:
    conn.commit()
    raise RuntimeError("Daily CitoAPI safety limit reached: 25/25 requests (UTC). Try tomorrow.")
   cur.execute("UPDATE cito_daily_usage SET used=used+1 WHERE day=%s",(day,))
   conn.commit()
  except Exception:
   conn.rollback()
   raise
  finally:conn.close()
def quota_status():
 from datetime import datetime,timezone
 day=datetime.now(timezone.utc).strftime("%Y-%m-%d")
 if not os.getenv("DATABASE_URL"):return {"limit":25,"used":None,"remaining":0,"day_utc":day,"error":"DATABASE_URL required"}
 try:
  conn,pg=cache_conn()
  try:
   cur=conn.cursor()
   cur.execute("CREATE TABLE IF NOT EXISTS cito_daily_usage (day TEXT PRIMARY KEY, used INTEGER NOT NULL)")
   cur.execute("SELECT used FROM cito_daily_usage WHERE day=%s",(day,))
   row=cur.fetchone();conn.commit()
   used=int(row[0]) if row else 0
   return {"limit":25,"used":used,"remaining":max(0,25-used),"day_utc":day}
  finally:conn.close()
 except Exception:return {"limit":25,"used":None,"remaining":0,"day_utc":day,"error":"Quota database unavailable"}
def upstream(endpoint):
 token=os.getenv("CITO_API_KEY")
 if not token:raise RuntimeError("CITO_API_KEY missing in Render environment")
 consume_quota()
 try:
  with urlopen(Request(BASE+endpoint,headers={"x-api-key":token,"Accept":"application/json"}),timeout=12) as r:return json.load(r)
 except HTTPError as e:
  if e.code==429:raise RuntimeError("CitoAPI HTTP 429: provider rate limit reached (not an app cooldown)")
  raise RuntimeError("CitoAPI HTTP "+str(e.code))
 except Exception as e:
  if isinstance(e,RuntimeError):raise
  raise RuntimeError("CitoAPI connection error: "+type(e).__name__)
def response_shape(data):
 if isinstance(data,list):return {"type":"list","length":len(data)}
 if isinstance(data,dict):
  shape={"type":"object","keys":list(data.keys())[:20]}
  for k,v in data.items():
   if isinstance(v,list):shape[k]={"type":"list","length":len(v)}
   elif isinstance(v,dict):shape[k]={"type":"object","keys":list(v.keys())[:15]}
  return shape
 return {"type":type(data).__name__}
def records(data):
 if isinstance(data,list):return data
 if isinstance(data,dict):
  for k in ("data","fixtures","matches","results","events","items"):
   if isinstance(data.get(k),list):return data[k]
   if isinstance(data.get(k),dict):
    nested=records(data[k])
    if nested:return nested
 return []
def roster(m):
 p=m.get("players")
 if isinstance(p,list):s=p[:2]
 elif isinstance(p,dict):s=[p.get("p1") or p.get("player1"),p.get("p2") or p.get("player2")]
 else:s=[m.get("player1") or m.get("home_player"),m.get("player2") or m.get("away_player")]
 out=[]
 for i,v in enumerate(s):
  v=dict(v) if isinstance(v,dict) else {"name":v} if isinstance(v,str) else {}
  v["id"]=v.get("id") or v.get("player_id") or m.get("player"+str(i+1)+"_id")
  v["name"]=v.get("name") or v.get("full_name") or v.get("player_name") or m.get("player"+str(i+1)+"_name") or "Unknown player"
  out.append(v)
 return out
def start_time(m):
 from datetime import datetime,timezone
 for k in ("start_time","scheduled_at","event_date","starts_at","date"):
  v=m.get(k)
  if isinstance(v,(int,float)):
   try:return datetime.fromtimestamp(v/1000 if v>1e11 else v,timezone.utc)
   except Exception:pass
  if isinstance(v,str):
   try:
    dt=datetime.fromisoformat(v.replace("Z","+00:00"))
    if dt.tzinfo:return dt.astimezone(timezone.utc)
   except ValueError:pass
 return None
def relevant(m,now):
 if not isinstance(m,dict):return False
 if "double" in str(m.get("draw") or "").lower():return False
 status=str(m.get("status") or m.get("match_status") or m.get("state") or "").lower()
 if any(s in status for s in ("finish","complete","final","cancel","postpon","retir","walkover")):return False
 if any(s in status for s in ("live","in_progress","in progress","playing","ongoing","inplay","in-play")):return True
 t=start_time(m)
 return bool(t and 0<=(t-now).total_seconds()<=3600)
METRICS=[
 ("First serves in %",["first_serve_percentage","first_serves_in","first_serve_in_pct"],1),
 ("First-serve points won %",["first_serve_points_won_percentage","first_serve_points_won_pct"],1),
 ("Second-serve points won %",["second_serve_points_won_percentage","second_serve_points_won_pct"],1),
 ("Aces per service game",["aces_per_service_game"],1),
 ("Double faults per service game",["double_faults_per_service_game"],-1),
 ("Return points won %",["return_points_won_percentage","return_points_won_pct"],1),
 ("Break-point conversion %",["break_points_converted_percentage","break_point_conversion_pct"],1),
 ("Service games held %",["service_games_held_percentage","service_games_held_pct"],1),
 ("First-serve return points won %",["first_serve_return_points_won_percentage","first_serve_return_points_won_pct"],1),
 ("Return games won %",["return_games_won_percentage","return_games_won_pct"],1)]
def metric(p,keys):
 for source in (p,p.get("stats"),p.get("statistics"),p.get("season_stats"),p.get("surface_stats")):
  if isinstance(source,dict):
   for k in keys:
    try:
     if source.get(k) is not None:return float(source[k])
    except (ValueError,TypeError):pass
 return None
def summarize(m):
 p=roster(m)
 if len(p)<2:return None
 comparisons=[];counts=[0,0]
 for label,keys,direction in METRICS:
  a,b=metric(p[0],keys),metric(p[1],keys)
  if a is None or b is None:continue
  leader=0 if (a-b)*direction>0 else 1 if (a-b)*direction<0 else None
  if leader is not None:counts[leader]+=1
  comparisons.append({"metric":label,"a":a,"b":b,"leader":leader})
 n=len(comparisons);score=max(counts);lead=0 if counts[0]>counts[1] else 1 if counts[1]>counts[0] else None
 def elo(x):
  s=x.get("stats") or {};r=s.get("ratings") or {}
  return r.get("elo") or r.get("overall_elo") if isinstance(r,dict) else None
 def rank(x):
  r=x.get("ranking")
  return r.get("rank") or r.get("position") if isinstance(r,dict) else r
 grade=("A" if score>=9 else "B+" if score==8 else "B" if score==7 else "C") if n==10 else "Incomplete"
 # Provisional evidence only: a prediction requires at least 7 paired metrics.
 predicted=p[lead]["name"] if lead is not None and n>=7 and score>=6 else None
 close=[]
 if n:
  close.append({"filter":"ProT 2.0","detail":str(score)+"/10 advantages with "+str(n)+"/10 metrics available; "+str(max(0,7-score))+" more category advantages to reach the 7/10 benchmark" if score<7 else str(score)+"/10 advantages; "+str(max(0,10-n))+" metric(s) still missing"})
  close.append({"filter":"EGOR","detail":"Serve/return dominance "+str(score)+"/"+str(n)+"; independent favorite confirmation still needed"})
 else:
  close=[{"filter":"ProT 2.0","detail":"0/10 paired metrics available; cannot measure proximity yet"},{"filter":"EGOR","detail":"Serve/return comparison and market favorite data needed"},{"filter":"Cold Drinks 2.0","detail":"Surface Elo, dated momentum, opponent-adjusted form, live control, ranking movement and fitness needed"}]
 if n:close.append({"filter":"Cold Drinks 2.0","detail":"Historical Elo, momentum, opponent quality and injury/fitness checks still needed"})
 return {"id":str(m.get("id") or m.get("fixture_id") or (p[0]["name"]+"-"+p[1]["name"])),"players":[{"name":x["name"],"id":x.get("id"),"elo":elo(x),"ranking":rank(x)} for x in p],"start":start_time(m).isoformat() if start_time(m) else None,"status":m.get("status") or m.get("match_status"),"surface":m.get("surface"),"comparisons":comparisons,"coverage":n,"counts":counts,"leader":lead,"score":score,"grade":grade,"prot_pass":n==10 and lead is not None and 7<=score<=9,"egor_pass":False,"cold_pass":False,"reason":"Independent market favorite, historical Elo and fitness data not available","prices":None,"predicted_winner":predicted,"prediction_basis":"Provisional statistical lean, not a calibrated win probability" if predicted else "Awaiting statistical evidence","close_filters":close}

def unwrap(data):
 while isinstance(data,dict) and isinstance(data.get("data"),dict):
  data=data["data"]
 return data
def numeric_fields(obj,depth=0):
 out={}
 if not isinstance(obj,dict) or depth>5:return out
 for k,v in obj.items():
  if isinstance(v,dict):out.update(numeric_fields(v,depth+1))
  elif isinstance(v,(int,float)) and not isinstance(v,bool):out[k.lower().replace("_","").replace("-","")]=float(v)
  elif isinstance(v,str):
   try:out[k.lower().replace("_","").replace("-","")]=float(v.strip().rstrip("%"))
   except ValueError:pass
 return out
def stat_value(fields,aliases):
 for k in aliases:
  v=fields.get(k.lower().replace("_","").replace("-",""))
  if v is not None:return v
 return None
STAT_ALIASES={
 "First serves in %":["first_serve_percentage","first_serves_in","first_serve_in_pct","firstServePct","firstServePercentage","firstServeInPct"],
 "First-serve points won %":["first_serve_points_won_percentage","first_serve_points_won_pct","firstServePointsWonPct","firstServeWonPct"],
 "Second-serve points won %":["second_serve_points_won_percentage","second_serve_points_won_pct","secondServePointsWonPct","secondServeWonPct"],
 "Aces per service game":["aces_per_service_game","acesPerServiceGame"],
 "Double faults per service game":["double_faults_per_service_game","doubleFaultsPerServiceGame"],
 "Return points won %":["return_points_won_percentage","return_points_won_pct","returnPointsWonPct"],
 "Break-point conversion %":["break_points_converted_percentage","break_point_conversion_pct","breakPointsConvertedPct","breakPointConversionPct"],
 "Service games held %":["service_games_held_percentage","service_games_held_pct","serviceGamesHeldPct","holdPct"],
 "First-serve return points won %":["first_serve_return_points_won_percentage","first_serve_return_points_won_pct","firstServeReturnPointsWonPct"],
 "Return games won %":["return_games_won_percentage","return_games_won_pct","returnGamesWonPct","breakPct"]}
def extract_metrics(raw):
 fields=numeric_fields(unwrap(raw))
 out={}
 for label,_,direction in METRICS:
  v=stat_value(fields,STAT_ALIASES[label])
  if v is None:
   pairs={
    "First serves in %":(["firstServesIn","firstServesMade"],["firstServeAttempts","servicePoints"]),
    "First-serve points won %":(["firstServePointsWon"],["firstServePointsPlayed","firstServePoints"]),
    "Second-serve points won %":(["secondServePointsWon"],["secondServePointsPlayed","secondServePoints"]),
    "Return points won %":(["returnPointsWon"],["returnPointsPlayed"]),
    "Break-point conversion %":(["breakPointsConverted"],["breakPointsChances","breakPointsOpportunities"]),
    "Service games held %":(["serviceGamesHeld"],["serviceGamesPlayed"]),
    "First-serve return points won %":(["firstServeReturnPointsWon"],["firstServeReturnPointsPlayed"]),
    "Return games won %":(["returnGamesWon","breaks"],["returnGamesPlayed"]),
    "Aces per service game":(["aces"],["serviceGamesPlayed"]),
    "Double faults per service game":(["doubleFaults"],["serviceGamesPlayed"])}
   if label in pairs:
    a=stat_value(fields,pairs[label][0]);b=stat_value(fields,pairs[label][1])
    if a is not None and b and b>0:v=round(a/b*(1 if "per service game" in label else 100),2)
  if v is not None:
   if "%" in label and 0<=v<=1:v=round(v*100,2)
   out[label]=round(v,2)
 return out
def cached_fetch(endpoint,ttl=604800):
 key="cito:historical:"+endpoint
 old=stored_get(key)
 if old and time.time()-old[1]<ttl:return old[0],False
 data=upstream(endpoint)
 stored_put(key,data)
 return data,True
def analyze_match(match_id):
 saved=snapshot()
 matches=saved.get("matches",[])
 match=next((m for m in matches if str(m.get("id"))==str(match_id)),None)
 if not match:return 404,{"error":"Match not in saved scan. Run Scan first."}
 players=match.get("players",[])
 if len(players)!=2:return 422,{"error":"Two players are required"}
 if any(not str(p.get("id") or "").strip() or str(p.get("id")).lower()=="none" for p in players):
  return 422,{"error":"Fixture has no player IDs. Cannot fetch historical statistics reliably.","match":match}
 from urllib.parse import quote
 fetched=0;issues=[];stats=[];forms=[]
 for p in players:
  pid=quote(str(p["id"]),safe="")
  try:
   data,new=cached_fetch("players/"+pid+"/stats")
   fetched+=int(new);stats.append(extract_metrics(data))
   # Schema metadata is retained so a missing field can be diagnosed without further requests.
  except RuntimeError as e:issues.append(str(p["name"])+" stats: "+str(e));stats.append({})
 for p in players:
  pid=quote(str(p["id"]),safe="")
  try:
   data,new=cached_fetch("players/"+pid+"/form")
   fetched+=int(new);forms.append(numeric_fields(unwrap(data)))
  except RuntimeError as e:issues.append(str(p["name"])+" form: "+str(e));forms.append({})
 h2h=None
 try:
  a=quote(str(players[0]["id"]),safe="");b=quote(str(players[1]["id"]),safe="")
  data,new=cached_fetch("h2h?player1_id="+a+"&player2_id="+b)
  fetched+=int(new);h2h=unwrap(data)
 except RuntimeError as e:issues.append("H2H: "+str(e))
 comparisons=[];counts=[0,0]
 for label,_,direction in METRICS:
  a,b=stats[0].get(label),stats[1].get(label)
  lead=None
  if a is not None and b is not None:
   lead=0 if (a-b)*direction>0 else 1 if (a-b)*direction<0 else None
   if lead is not None:counts[lead]+=1
  comparisons.append({"metric":label,"a":a,"b":b,"leader":lead,"margin":round(abs(a-b),2) if a is not None and b is not None else None})
 coverage=sum(x["a"] is not None and x["b"] is not None for x in comparisons)
 lead=0 if counts[0]>counts[1] else 1 if counts[1]>counts[0] else None
 match=dict(match);match["comparisons"]=comparisons;match["coverage"]=coverage;match["counts"]=counts;match["score"]=max(counts)
 match["leader"]=lead
 match["predicted_winner"]=players[lead]["name"] if lead is not None and coverage>=7 and max(counts)>=6 else None
 match["prediction_basis"]="Provisional: historical category comparison; no calibrated win probability" if match["predicted_winner"] else "Historical comparisons shown; not enough complete categories for a defensible prediction"
 match["close_filters"]=[
  {"filter":"ProT 2.0","detail":str(counts[0])+"/10 "+players[0]["name"]+" vs "+str(counts[1])+"/10 "+players[1]["name"]+"; "+str(coverage)+"/10 categories comparable"},
  {"filter":"EGOR","detail":"Historical serve/return dominance "+str(max(counts))+"/"+str(coverage)+"; favorite status requires independent odds. Grade "+("A" if coverage==10 and max(counts)>=8 else "B" if coverage>=8 and max(counts)>=6 else "C (incomplete)")},
  {"filter":"Cold Drinks 2.0","detail":"Historical serve/return "+str(coverage)+"/10 categories; surface Elo, dated Elo movement, live control, opponent-adjusted form and six-month ranking momentum need separately sourced values."}]
 match["egor_grade"]="A" if coverage==10 and max(counts)>=8 else "B" if coverage>=8 and max(counts)>=6 else "C — incomplete"
 match["valuation"]="Underdog/favorite and under/overvaluation require independent two-sided market odds; CitoAPI odds are not yet available."
 match["form_fields"]=forms
 match["h2h"]=h2h if isinstance(h2h,dict) else None
 match["analysis_issues"]=issues
 match["historical_requests"]=fetched
 match["historical_scope"]="CitoAPI player career aggregates; surface/recency not assumed"
 match["prot_pass"]=coverage==10 and max(counts)>=7
 # Cache analyzed match and reuse it on page reload without extra provider requests.
 stored_put("analysis:"+str(match_id),match)
 return 200,{"match":match,"quota":quota_status()}

def snapshot():
 saved=stored_get("manual:last")
 if saved:return dict(saved[0],cached=True)
 return {"matches":[],"scanned_at":None,"cached":True,"data_state":"unavailable","note":"No successful scan saved yet. Press Run Scan to request live and next-hour matches."}
def run_scan():
 from datetime import datetime,timezone
 if not SCAN_LOCK.acquire(False):return 409,{"error":"A scan is already running"}
 try:
  try:
   raw_live=upstream("matches/live")
   all_matches=records(raw_live)
   live_shape=response_shape(raw_live)
   upcoming_shape=None
   # The second call retrieves upcoming matches; never exceed the shared 25/day cap.
   errors=[];requests=0;profiles={}
   try:
    raw_upcoming=upstream("matches/upcoming")
    upcoming_shape=response_shape(raw_upcoming)
    seen={str(m.get("id") or m.get("match_id") or i) for i,m in enumerate(all_matches) if isinstance(m,dict)}
    for m in records(raw_upcoming):
     if isinstance(m,dict):
      mid=str(m.get("id") or m.get("match_id") or "")
      if not mid or mid not in seen:
       all_matches.append(m)
       if mid:seen.add(mid)
   except RuntimeError as e:errors.append(str(e))
  except RuntimeError as e:
   result=snapshot();result["note"]=str(e);result["data_state"]="unavailable" if not result.get("scanned_at") else "stale";result["quota"]=quota_status();return 200,result
  now=datetime.now(timezone.utc)
  matches=[m for m in all_matches if relevant(m,now)]
  # Do not spend additional requests on profiles until CitoAPI's response schema is validated.
  results=[]
  for m in matches:
   copied=dict(m);sides=roster(m)
   for p in sides:
    profile=profiles.get(str(p.get("id")))
    if isinstance(profile,dict):
     for k in ("name","ranking","country"):
      if not p.get(k):p[k]=profile.get(k)
     p["stats"]=profile.get("stats") or p.get("stats") or {}
   copied["players"]=sides
   result=summarize(copied)
   if result:results.append(result)
  output={"matches":results,"scanned_at":datetime.now(timezone.utc).isoformat(),"cached":False,"meta":{"fixture_page_count":len(all_matches),"in_window":len(matches),"profile_requests":0,"profiles_available":0,"profile_errors":errors,"limited_to_first_page":len(all_matches)>=100,"response_shapes":{"live":live_shape,"upcoming":upcoming_shape}},"note":"CitoAPI manual scan. Inspect response_shapes if fixture count is zero; no secrets or player records are exposed.","quota":quota_status()}
  stored_put("manual:last",output)
  return 200,output
 finally:SCAN_LOCK.release()
class Handler(BaseHTTPRequestHandler):
 def send(self,status,obj,ctype="application/json"):
  data=obj if isinstance(obj,bytes) else (obj.encode() if isinstance(obj,str) and ctype.startswith("text/") else json.dumps(obj).encode())
  self.send_response(status);self.send_header("Content-Type",ctype);self.send_header("Cache-Control","no-store");self.send_header("Content-Length",str(len(data)));self.end_headers();self.wfile.write(data)
 def do_GET(self):
  path=urlparse(self.path).path
  if path in ("/","/index.html"):
   with open("index.html","rb") as f:return self.send(200,f.read(),"text/html; charset=utf-8")
  if path=="/manifest.webmanifest":
   with open("manifest.webmanifest","rb") as f:return self.send(200,f.read(),"application/manifest+json")
  if path=="/health":return self.send(200,{"ok":True,"configured":bool(os.getenv("CITO_API_KEY"))})
  if path=="/api/status":return self.send(200,{"retry_seconds":0,"manual_only":True,"last_scan":snapshot().get("scanned_at"),"quota":quota_status()})
  if path=="/api/results":return self.send(200,dict(snapshot(),quota=quota_status()))
  if path=="/api/analysis":
   mid=parse_qs(urlparse(self.path).query).get("id",[""])[0]
   if not mid:return self.send(400,{"error":"Match ID required"})
   saved=stored_get("analysis:"+mid)
   return self.send(200,{"match":saved[0],"cached":True,"quota":quota_status()}) if saved else self.send(404,{"error":"No saved analysis for this match"})
  if path=="/api/scan":return self.send(405,{"error":"Use POST to start a scan"})
  return self.send(404,{"error":"Not found"})
 def do_POST(self):
  path=urlparse(self.path).path
  if path=="/api/analyze":
   try:
    size=min(int(self.headers.get("Content-Length","0")),2048)
    body=json.loads(self.rfile.read(size)) if size else {}
    mid=str(body.get("id",""))
    if not mid:return self.send(400,{"error":"Match ID required"})
    status,data=analyze_match(mid)
    return self.send(status,data)
   except Exception as e:return self.send(500,{"error":"Analysis error: "+type(e).__name__})
  if path!="/api/scan":return self.send(404,{"error":"Not found"})
  status,data=run_scan();return self.send(status,data)
if __name__=="__main__":
 ThreadingHTTPServer(("0.0.0.0",int(os.getenv("PORT","10000"))),Handler).serve_forever()
