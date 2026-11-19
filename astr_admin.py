import json
import os
import secrets
import tempfile
from pathlib import Path
from urllib.parse import urlsplit
from fastapi import HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, SecretStr

FILE = Path(__file__).with_name('astrbot-settings.json')
FIELDS = ('astrbot_api_base', 'astrbot_api_key', 'astrbot_username', 'astrbot_config_id')

def load(config):
    if FILE.exists():
        saved = json.loads(FILE.read_text())
        config.update({key: saved[key] for key in FIELDS})

class Update(BaseModel):
    url: str
    key: SecretStr
    username: str
    config_id: str

PAGE = '''<!doctype html><html lang="zh"><meta charset="utf-8"><meta name="viewport" content="width=device-width"><style>body{font:18px system-ui;max-width:32rem;margin:3rem auto;padding:0 1rem}label,input,button{display:block}input,button{font:inherit;padding:.6rem;margin:.3rem 0 1rem;box-sizing:border-box;width:100%}</style><h1>AstrBot 设置</h1><form id="form"><label>管理员口令<input id="admin" type="password" required></label><label>AstrBot API 地址<input id="url" type="url" required></label><label>AstrBot API Key<input id="key" type="password" placeholder="同地址留空沿用"></label><label>用户名<input id="username" required></label><label>Config ID<input id="config_id" placeholder="可留空"></label><button>保存并应用</button></form><output id="status"></output><script>document.querySelector('form').onsubmit=async e=>{e.preventDefault();const val=id=>document.getElementById(id).value;try{const r=await fetch(location.pathname,{method:'POST',headers:{'Content-Type':'application/json','X-Admin-Token':val('admin')},body:JSON.stringify({url:val('url'),key:val('key'),username:val('username'),config_id:val('config_id')})});document.querySelector('output').textContent=r.ok?'已保存并应用':(await r.json()).detail;if(r.ok){document.querySelector('#admin').value='';document.querySelector('#key').value=''}}catch{document.querySelector('output').textContent='连接失败'}}</script></html>'''

def attach(app, config, bot):
    @app.get('/admin/astrbot', response_class=HTMLResponse, include_in_schema=False)
    def page():
        return HTMLResponse(PAGE,headers={'Cache-Control':'no-store','X-Frame-Options':'DENY'})

    @app.post('/admin/astrbot', include_in_schema=False)
    async def update(request:Request, body:Update):
        expected=os.getenv('VRM_ADMIN_TOKEN','')
        if not expected or not secrets.compare_digest(expected.encode(), request.headers.get('X-Admin-Token','').encode()):
            raise HTTPException(403,'管理员口令错误或未配置')
        url=body.url.strip().rstrip('/')
        target=urlsplit(url)
        if target.scheme not in ('https','http') or not target.hostname or target.username or target.password or target.fragment or target.query:
            raise HTTPException(400,'请填写完整的 AstrBot API 根地址')
        key=body.key.get_secret_value() or (bot.api_key if url==bot.base else '')
        if not key: raise HTTPException(400,'首次配置或更换地址时请填 API Key')
        values=dict(zip(FIELDS,(url,key,body.username.strip(),body.config_id.strip())))
        with tempfile.NamedTemporaryFile('w',dir=FILE.parent,delete=False) as out:
            tmp=Path(out.name)
            try:
                json.dump(values,out);out.flush();os.fsync(out.fileno());out.close();tmp.replace(FILE)
            finally: tmp.unlink(missing_ok=True)
        config.update(values)
        bot.base,bot.api_key,bot.username=url,key,values['astrbot_username']
        return {'ok':True}
