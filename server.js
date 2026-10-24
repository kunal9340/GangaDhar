const express = require('express');
const http = require('http');
const path = require('path');
const fs = require('fs');
const mqtt = require('mqtt');
const { Server } = require('socket.io');

const ROOT = __dirname, PUB = path.join(ROOT, 'public'), DATA = path.join(ROOT, 'data');
for (const f of [DATA]) if (!fs.existsSync(f)) fs.mkdirSync(f, {recursive:true});
const CFG_FILE = path.join(DATA, 'mqtt-config.json');
const HISTORY_FILE = path.join(DATA, 'history.jsonl');
const DAILY_FILE = path.join(DATA, 'daily-discharge.json');
const SAMPLE_FILE = path.join(DATA, 'sample_payload.json');
const defaultCfg = { host:'103.174.130.172', port:1883, username:'admin', password:'', topic:'test_plc', clientId:'lcepl_webscada_'+Math.random().toString(16).slice(2,8), enabled:false };
function readJson(file, fallback){ try{return JSON.parse(fs.readFileSync(file,'utf8'));}catch{return fallback;} }
function writeJsonAtomic(file,obj){ const tmp=file+'.tmp'; fs.writeFileSync(tmp,JSON.stringify(obj,null,2)); fs.renameSync(tmp,file); }
let cfg = {...defaultCfg, ...readJson(CFG_FILE,{})};
let latest = null, mqttClient = null, mqttState = {connected:false, message:'Not configured', lastMessage:null, error:null};
let lastFlow = null;
let daily = readJson(DAILY_FILE, {});
const app = express(); const server = http.createServer(app); const io = new Server(server);
app.use(express.json({limit:'2mb'})); app.use(express.static(PUB));
function nowIST(){ return new Date(new Date().toLocaleString('en-US',{timeZone:'Asia/Kolkata'})); }
function istIso(d=new Date()){ return new Intl.DateTimeFormat('sv-SE',{timeZone:'Asia/Kolkata',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false}).format(d).replace(' ','T'); }
function istDate(d=new Date()){ return new Intl.DateTimeFormat('en-CA',{timeZone:'Asia/Kolkata',year:'numeric',month:'2-digit',day:'2-digit'}).format(d); }
function findValue(name){ if(!latest || !Array.isArray(latest.data)) return null; const x=latest.data.find(v=>v.name===name); return x?Number(x.value):null; }
function applyDailyDischarge(payload){
  const flowItem = (payload.data||[]).find(v => /flow transmitter/i.test(v.name||''));
  if(!flowItem) return;
  const flow = Number(flowItem.value); const unit=(flowItem.unit||'').toLowerCase(); const ts = new Date();
  if(!Number.isFinite(flow) || flow<0) return;
  const day=istDate(ts); if(!daily[day]) daily[day]={date:day,dischargeM3:0,source:'flow integration',updatedAt:istIso(ts),samples:0};
  if(lastFlow && (ts-lastFlow.ts)>0 && (ts-lastFlow.ts)<=3600000){
    const seconds=(ts-lastFlow.ts)/1000;
    // Integrate only units explicitly supported; avoid silently treating unknown units as L/s.
    let avg=(flow+lastFlow.flow)/2, liters=0;
    if(unit==='l/s' || unit==='lps') liters=avg*seconds;
    else if(unit==='m³/h' || unit==='m3/h' || unit==='m3/hr') liters=avg*seconds/3600*1000;
    if(liters>=0){
      // If an interval crosses IST midnight, split the estimated interval at midnight.
      const prevDay=lastFlow.day;
      if(prevDay===day){ daily[day].dischargeM3 += liters/1000; }
      else { const parts=new Intl.DateTimeFormat('en-GB',{timeZone:'Asia/Kolkata',hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false}).formatToParts(ts); const part=k=>Number(parts.find(p=>p.type===k)?.value||0); const secondsAfterMidnight=part('hour')*3600+part('minute')*60+part('second')+(ts.getMilliseconds()/1000); const frac=Math.max(0,Math.min(1,secondsAfterMidnight/seconds)); const todayLiters=liters*frac; const oldLiters=liters-todayLiters; if(!daily[prevDay]) daily[prevDay]={date:prevDay,dischargeM3:0,source:'flow integration',updatedAt:istIso(ts),samples:0}; daily[prevDay].dischargeM3 += oldLiters/1000; daily[day].dischargeM3 += todayLiters/1000; }
    }
  }
  daily[day].updatedAt=istIso(ts); daily[day].samples=(daily[day].samples||0)+1; lastFlow={flow,ts,day,unit};
  try{writeJsonAtomic(DAILY_FILE,daily);}catch(e){console.error('daily write error',e.message)}
}
function normalizePayload(obj){
  if(obj && Array.isArray(obj.data)) return {timestamp:obj.timestamp||istIso(),timezone:obj.timezone||'Asia/Kolkata',data:obj.data.map(x=>({name:String(x.name??x.tag??'Unknown'),value:x.value??null,unit:String(x.unit??'')}))};
  // Accept simple flat objects as a convenience.
  if(obj && typeof obj==='object') return {timestamp:istIso(),timezone:'Asia/Kolkata',data:Object.entries(obj).filter(([k,v])=>!['timestamp','timezone'].includes(k)&&(['string','number','boolean'].includes(typeof v))).map(([name,value])=>({name,value,unit:''}))};
  throw new Error('Unsupported MQTT JSON payload: expected data[] array');
}
function ingest(obj, source='mqtt'){
  const payload=normalizePayload(obj); payload.receivedAt=istIso(); payload.source=source;
  latest=payload; mqttState.lastMessage=payload.receivedAt; mqttState.message='Receiving data'; mqttState.error=null;
  applyDailyDischarge(payload);
  try{fs.appendFileSync(HISTORY_FILE,JSON.stringify(payload)+'\n');}catch(e){console.error('history write error',e.message)}
  io.emit('data',payload); io.emit('status',getStatus());
}
function disconnectMqtt(){ if(mqttClient){ try{mqttClient.end(true); }catch{} mqttClient=null; } mqttState.connected=false; }
function connectMqtt(){
  disconnectMqtt();
  if(!cfg.enabled){mqttState={...mqttState,connected:false,message:'MQTT disabled — configure and enable it in Settings',error:null};io.emit('status',getStatus());return;}
  const url=`mqtt://${cfg.host}:${Number(cfg.port)||1883}`;
  mqttState={connected:false,message:'Connecting to broker…',lastMessage:mqttState.lastMessage,error:null};
  mqttClient=mqtt.connect(url,{username:cfg.username||undefined,password:cfg.password||undefined,clientId:cfg.clientId||defaultCfg.clientId,clean:true,connectTimeout:10000,reconnectPeriod:5000,keepalive:30});
  mqttClient.on('connect',()=>{mqttState.connected=true;mqttState.message='Connected';mqttState.error=null;mqttClient.subscribe(cfg.topic,{qos:0},err=>{if(err){mqttState.error=err.message;mqttState.message='Subscribe failed';}io.emit('status',getStatus());});});
  mqttClient.on('reconnect',()=>{mqttState.connected=false;mqttState.message='Reconnecting…';io.emit('status',getStatus());});
  mqttClient.on('message',(topic,buf)=>{try{ingest(JSON.parse(buf.toString()),'mqtt:'+topic);}catch(e){mqttState.error='Invalid JSON: '+e.message;io.emit('status',getStatus());}});
  mqttClient.on('error',e=>{mqttState.error=e.message;mqttState.message='Connection error';io.emit('status',getStatus());});
  mqttClient.on('close',()=>{mqttState.connected=false;if(cfg.enabled)mqttState.message='Disconnected / retrying';io.emit('status',getStatus());});
}
function getStatus(){return {connected:mqttState.connected,message:mqttState.message,lastMessage:mqttState.lastMessage,error:mqttState.error,host:cfg.host,port:cfg.port,topic:cfg.topic,enabled:!!cfg.enabled,tagCount:latest?.data?.length||0,timestamp:latest?.timestamp||null};}
function loadLatestFromHistory(){
  try{const lines=fs.readFileSync(HISTORY_FILE,'utf8').trim().split(/\r?\n/).filter(Boolean);if(lines.length){latest=JSON.parse(lines[lines.length-1]);}}
  catch{}
  if(!latest) {const sample=readJson(SAMPLE_FILE,null); if(sample) latest=sample;}
}
loadLatestFromHistory();
app.get('/api/status',(req,res)=>res.json(getStatus()));
app.get('/api/config',(req,res)=>res.json({...cfg,password:cfg.password?'••••••••':'',hasPassword:!!cfg.password}));
app.post('/api/config',(req,res)=>{
  const b=req.body||{}; cfg={...cfg,host:String(b.host||cfg.host).trim(),port:Number(b.port||cfg.port)||1883,username:String(b.username??cfg.username),topic:String(b.topic||cfg.topic).trim(),clientId:String(b.clientId||cfg.clientId).trim(),enabled:!!b.enabled};
  if(typeof b.password==='string' && b.password!=='••••••••' && b.password!=='') cfg.password=b.password;
  try{writeJsonAtomic(CFG_FILE,cfg);connectMqtt();res.json({ok:true,status:getStatus()});}catch(e){res.status(500).json({ok:false,error:e.message});}
});
app.get('/api/latest',(req,res)=>res.json(latest||{timestamp:istIso(),timezone:'Asia/Kolkata',data:[]}));
app.get('/api/tags',(req,res)=>res.json((latest?.data||[]).map(x=>({...x,category:categorize(x.name)}))));
function categorize(n){n=(n||'').toLowerCase();if(n.includes('sump'))return'Level';if(n.includes('pressure'))return'Pressure';if(n.includes('flow'))return'Flow';if(n.includes('running status'))return'Motor Status';if(n.includes('vibration'))return'Vibration';if(n.includes('temperature'))return'Temperature';if(n.includes('voltage'))return'Voltage';if(n.includes('current'))return'Current';if(n.includes('energy'))return'Energy';if(n.includes('power'))return'Power';if(n.includes('position')||n.includes('valve'))return'Valve';return'Other';}
app.get('/api/history',(req,res)=>{
  const from=req.query.from||'0000-01-01',to=req.query.to||'9999-12-31',tag=req.query.tag||'';let out=[];
  try{if(fs.existsSync(HISTORY_FILE)){const lines=fs.readFileSync(HISTORY_FILE,'utf8').split(/\r?\n/).filter(Boolean);for(const line of lines){try{const p=JSON.parse(line);const date=(p.receivedAt||p.timestamp||'').slice(0,10);if(date<from||date>to)continue;if(tag)p.data=(p.data||[]).filter(x=>x.name===tag);out.push(p);}catch{}}}}catch{}
  res.json(out.slice(-10000));
});
app.get('/api/daily-discharge',(req,res)=>res.json(Object.values(daily).sort((a,b)=>a.date.localeCompare(b.date))));
app.post('/api/demo-ingest',(req,res)=>{try{ingest(req.body,'manual/demo');res.json({ok:true});}catch(e){res.status(400).json({ok:false,error:e.message});}});
app.get('/api/sample',(req,res)=>res.json(readJson(SAMPLE_FILE,{timestamp:istIso(),timezone:'Asia/Kolkata',data:[]})));
const PORT=Number(process.env.PORT)||8086;
server.listen(PORT,'0.0.0.0',()=>{console.log(`LCEPL WebSCADA running at http://localhost:${PORT}`); if(cfg.enabled)connectMqtt(); else mqttState.message='MQTT disabled — configure in Settings';});
