import os,json
from http.server import ThreadingHTTPServer,BaseHTTPRequestHandler
from urllib.parse import urlparse,parse_qs,urlencode
from urllib.request import Request,urlopen
from urllib.error import HTTPError
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
  if path.path!="/api/fixtures":return self.send(404,'{"error":"Not found"}')
  token=os.getenv("TENNIS_API_TOKEN")
  if not token:return self.send(503,'{"error":"API token not configured on server"}')
  q=parse_qs(path.query); args={k:v[0] for k,v in q.items() if k in ("draw","limit","offset","date")}
  args.setdefault("draw","singles");args.setdefault("limit","100");args["token"]=token
  try:
   with urlopen(Request(BASE+"fixtures?"+urlencode(args),headers={"Accept":"application/json"}),timeout=25) as r: return self.send(200,r.read())
  except HTTPError as e:return self.send(502,json.dumps({"error":"Tennis API HTTP "+str(e.code)}))
  except Exception as e:return self.send(502,json.dumps({"error":"Tennis API connection failed","detail":str(e)}))
if __name__=="__main__":
 ThreadingHTTPServer(("0.0.0.0",int(os.getenv("PORT","10000"))),Handler).serve_forever()
