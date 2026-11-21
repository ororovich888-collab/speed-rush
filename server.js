// SPEED RUSH online: игра + аккаунты + комнаты + связь игроков (WebSocket)
const http=require('http'),fs=require('fs'),path=require('path'),crypto=require('crypto'),{WebSocketServer}=require('ws');
const html=()=>fs.readFileSync(path.join(__dirname,'index.html'));
const srv=http.createServer((q,r)=>{
  if(q.url=='/three.js'){try{const f=fs.readFileSync(require.resolve('three/build/three.min.js'));r.writeHead(200,{'Content-Type':'application/javascript','Cache-Control':'public,max-age=86400'});return r.end(f)}catch(e){r.writeHead(404);return r.end()}}
  r.writeHead(200,{'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-cache'});r.end(html())});
const wss=new WebSocketServer({server:srv,maxPayload:4096});
// ---- аккаунты (файл accounts.json; на бесплатном Render он сбрасывается при обновлении/перезапуске)
const DB=path.join(process.env.DATA_DIR||__dirname,'accounts.json');
let acc={};try{acc=JSON.parse(fs.readFileSync(DB,'utf8'))}catch(e){}
const saveDB=()=>{try{fs.writeFileSync(DB,JSON.stringify(acc))}catch(e){}};
const hash=(pw,salt)=>crypto.scryptSync(pw,salt,32).toString('hex');
// ---- комнаты и присутствие
const rooms=new Map(),rms=new Map();let n=0;
const ok=s=>/^(lobby|[a-z0-9]{1,16})$/.test(s);
const list=()=>[...rms.values()].map(r=>({id:r.id,nm:r.nm,host:r.hn,lock:r.pw?1:0,terr:r.terr,shape:r.shape}));
const pushRl=()=>{const m=JSON.stringify({t:'rl',list:list()});wss.clients.forEach(w=>w.user&&w.readyState==1&&w.send(m))};
const closeRoom=w=>{if(w.rid){rms.delete(w.rid);w.rid=null;pushRl()}};
function push(name,left){const m=rooms.get(name);if(!m)return;
  const l=[...m].map(([id,c])=>({id,pr:c.pr})),msg=JSON.stringify({t:'peers',room:name,list:l,left:left||[]});
  for(const c of m.values())if(c.ws.readyState==1)c.ws.send(msg)}
function leave(id,name){const m=rooms.get(name);if(!m||!m.delete(id))return;if(!m.size)rooms.delete(name);else push(name,[id])}
wss.on('connection',ws=>{
  const id='p'+(++n).toString(36)+Math.random().toString(36).slice(2,6),mine=new Set();ws.alive=true;ws.pid=id;ws.tries=0;
  const S=o=>ws.readyState==1&&ws.send(JSON.stringify(o));
  ws.on('pong',()=>ws.alive=true);
  S({t:'hello',id,v:2});
  ws.on('message',raw=>{let m;try{m=JSON.parse(raw)}catch(e){return}if(!m)return;
    if(m.t=='reg'||m.t=='login'||m.t=='auth'){
      if(++ws.tries>30)return;
      const nm=String(m.nm||'').trim().slice(0,14),k=nm.toLowerCase();
      if(m.t=='reg'){
        if(nm.length<2||!/^[\p{L}\p{N} _-]+$/u.test(nm))return S({t:'autherr',e:'Ник: 2-14 символов, буквы и цифры'});
        if(String(m.pw||'').length<4)return S({t:'autherr',e:'Пароль минимум 4 символа'});
        if(acc[k])return S({t:'autherr',e:'Этот ник занят'});
        const salt=crypto.randomBytes(8).toString('hex');acc[k]={nm,salt,h:hash(String(m.pw),salt),tok:crypto.randomBytes(16).toString('hex')};saveDB()}
      const a=acc[k];if(!a)return S({t:'autherr',e:'Аккаунт не найден'});
      if(m.t=='login'&&hash(String(m.pw||''),a.salt)!==a.h)return S({t:'autherr',e:'Неверный пароль'});
      if(m.t=='auth'&&m.tok!==a.tok)return S({t:'autherr',e:'Войди заново'});
      ws.user=a.nm;ws.tries=0;S({t:'authok',nm:a.nm,tok:a.tok});S({t:'rl',list:list()});return}
    if(m.t=='mk'){
      if(!ws.user||ws.rid)return;const nm=String(m.nm||'').trim().slice(0,20);if(!nm)return S({t:'rerr',e:'Введи название'});
      const r={id:Math.random().toString(36).slice(2,8),nm,pw:String(m.pw||'').slice(0,20),terr:m.terr==1?1:0,shape:[0,1,2].includes(m.shape)?m.shape:0,hn:ws.user,host:ws};
      rms.set(r.id,r);ws.rid=r.id;S({t:'mkd',id:r.id});pushRl();return}
    if(m.t=='rx'){closeRoom(ws);return}
    if(m.t=='rj'){
      const r=rms.get(m.id);if(!ws.user||ws.rid||!r||r.host==ws)return S({t:'rerr',e:'Комната не найдена'});
      if(r.pw&&String(m.pw||'')!==r.pw)return S({t:'rerr',e:'Неверный пароль'});
      rms.delete(r.id);r.host.rid=null;pushRl();
      const pr=w=>{const c=rooms.get('lobby')?.get(w.pid);return{nm:w.user,car:c&&c.pr.car,col:c&&c.pr.col}};
      const b={room:'g'+r.id,ti:r.terr,shape:r.shape};
      r.host.send(JSON.stringify({t:'go',...b,host:1,pp:pr(ws)}));S({t:'go',...b,host:0,pp:pr(r.host)});return}
    if(!ok(m.room))return;
    if(m.t=='join'){if(!rooms.has(m.room))rooms.set(m.room,new Map());const r=rooms.get(m.room);
      if(!r.has(id)&&r.size>=(m.room=='lobby'?200:4))return;
      if(!r.has(id))r.set(id,{ws,pr:{}});mine.add(m.room);push(m.room)}
    else if(m.t=='pres'){const c=rooms.get(m.room)?.get(id);if(!c||!m.patch||typeof m.patch!='object')return;
      for(const k in m.patch){if(m.patch[k]==null)delete c.pr[k];else c.pr[k]=m.patch[k]}push(m.room)}
    else if(m.t=='leave'){leave(id,m.room);mine.delete(m.room)}});
  ws.on('close',()=>{closeRoom(ws);mine.forEach(r=>leave(id,r))});
});
setInterval(()=>wss.clients.forEach(w=>{if(!w.alive)return w.terminate();w.alive=false;w.ping()}),25000);
const PORT=process.env.PORT||3000;
srv.listen(PORT,()=>{console.log('SPEED RUSH online: порт',PORT);
  const ni=require('os').networkInterfaces();for(const k in ni)for(const i of ni[k])if(i.family=='IPv4'&&!i.internal)console.log('Открой на устройствах в этой Wi-Fi сети: http://'+i.address+':'+PORT)});
