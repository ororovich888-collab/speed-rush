// SPEED RUSH online: раздаёт игру и соединяет игроков по WebSocket
const http=require('http'),fs=require('fs'),path=require('path'),{WebSocketServer}=require('ws');
const html=()=>fs.readFileSync(path.join(__dirname,'index.html'));
const srv=http.createServer((q,r)=>{r.writeHead(200,{'Content-Type':'text/html; charset=utf-8'});r.end(html())});
const wss=new WebSocketServer({server:srv,maxPayload:4096});
const rooms=new Map(); // имя комнаты -> Map(id -> {ws, pr})
let n=0;
const ok=s=>/^(lobby|[a-z0-9]{1,16})$/.test(s);
function push(name,left){const m=rooms.get(name);if(!m)return;
  const list=[...m].map(([id,c])=>({id,pr:c.pr})),msg=JSON.stringify({t:'peers',room:name,list,left:left||[]});
  for(const c of m.values())if(c.ws.readyState==1)c.ws.send(msg)}
function leave(id,name){const m=rooms.get(name);if(!m||!m.delete(id))return;if(!m.size)rooms.delete(name);else push(name,[id])}
wss.on('connection',ws=>{
  const id='p'+(++n).toString(36)+Math.random().toString(36).slice(2,6),mine=new Set();ws.alive=true;
  ws.on('pong',()=>ws.alive=true);
  ws.send(JSON.stringify({t:'hello',id}));
  ws.on('message',raw=>{let m;try{m=JSON.parse(raw)}catch(e){return}
    if(!m||!ok(m.room))return;
    if(m.t=='join'){if(!rooms.has(m.room))rooms.set(m.room,new Map());const r=rooms.get(m.room);
      if(!r.has(id)&&r.size>=(m.room=='lobby'?200:4))return;
      if(!r.has(id))r.set(id,{ws,pr:{}});mine.add(m.room);push(m.room)}
    else if(m.t=='pres'){const c=rooms.get(m.room)?.get(id);if(!c||!m.patch||typeof m.patch!='object')return;
      for(const k in m.patch){if(m.patch[k]==null)delete c.pr[k];else c.pr[k]=m.patch[k]}push(m.room)}
    else if(m.t=='leave'){leave(id,m.room);mine.delete(m.room)}});
  ws.on('close',()=>mine.forEach(r=>leave(id,r)));
});
setInterval(()=>wss.clients.forEach(w=>{if(!w.alive)return w.terminate();w.alive=false;w.ping()}),25000);
srv.listen(process.env.PORT||3000,()=>console.log('SPEED RUSH online: порт',process.env.PORT||3000));
