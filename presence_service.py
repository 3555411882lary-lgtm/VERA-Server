"""Presence routes mounted by middleware. Observation frames use its AstrBot client."""
import os, json, time, asyncio, base64
from pathlib import Path
import httpx
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel, Field
astr_client=None
app=FastAPI(); lock=asyncio.Lock()
from threading import Lock
from collections import OrderedDict
states = OrderedDict()
mutex = Lock()
def auth(token):
 expected=os.getenv('VRM_TOKEN','')
 if not expected or token!=expected: raise HTTPException(401,'Invalid token')
def store(session,kind,text):
 if kind not in ('walk_failed','walk_finished','observation'): return
 with mutex:
  state=states.setdefault(session,{})
  key='observation' if kind=='observation' else 'action'
  state[key]=(time.monotonic(),kind,text[:400])
  states.move_to_end(session)
  if len(states)>256: states.popitem(last=False)

def current(session):
 with mutex:
  return {kind:text for stamp,kind,text in states.get(session,{}).values() if time.monotonic()-stamp < (15 if kind=='observation' else 60)}
class Event(BaseModel):
 session:str=Field(default='vrm',max_length=100)
 kind:str=Field(max_length=40)
 text:str=Field(max_length=4000)
class Observation(BaseModel):
 session:str=Field(default='vrm',max_length=100)
 image:str=Field(max_length=2000000)
@app.get('/health')
def health(): return {'ok':True,'vision_configured':astr_client is not None, 'vision_backend':'astrbot'}
@app.post('/event')
def event(body:Event,x_vrm_token:str=Header(default='')):
 auth(x_vrm_token);store(body.session,body.kind,body.text);return {'ok':True}
@app.get('/context')
def context(session:str='vrm',x_vrm_token:str=Header(default='')):
 auth(x_vrm_token)
 return current(session)
@app.post('/observe')
async def observe(body:Observation,x_vrm_token:str=Header(default='')):
 auth(x_vrm_token)
 if lock.locked():raise HTTPException(429,'Previous observation busy; drop this frame')
 try:
  raw=base64.b64decode(body.image,validate=True)
  if len(raw)>1400000 or not raw.startswith(b'\xff\xd8'):raise ValueError()
 except Exception:raise HTTPException(400,'Expected bounded JPEG')
 async with lock:
  bot = astr_client
  if bot is None: raise HTTPException(503,'AstrBot client unavailable')
  client = await bot._get_client()
  try:
   result = await client.post(bot.base+'/file', files={'file':('frame.jpg',raw,'image/jpeg')},headers={'Authorization':'Bearer '+bot.api_key})
   result.raise_for_status()
   data=result.json()['data']; attachment=data.get('id') or data.get('attachment_id')
   if not attachment: raise ValueError('Missing attachment id')
   parts=[{'type':'image','attachment_id':attachment},{'type':'plain','text':'只用一句中文描述这张图中可见的环境。'}]
   text=await asyncio.wait_for(bot.chat(parts,'vision_'+body.session),40)
   if not text.strip() or text=='（AstrBot回复为空）': raise ValueError('Empty description')
  except (httpx.HTTPError,ValueError,KeyError,asyncio.TimeoutError) as exc:
   raise HTTPException(502,'AstrBot image request failed') from exc
  store(body.session,'observation',text[:400]);return {'text':text[:400],'timestamp':time.time()}
if __name__=='__main__':
 import uvicorn
 uvicorn.run(app,host=os.getenv('VRM_BIND','0.0.0.0'),port=int(os.getenv('VRM_PORT','8771')))
