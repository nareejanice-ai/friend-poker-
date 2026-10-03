const http=require('http');
const fs=require('fs');
const path=require('path');
const crypto=require('crypto');
const {WebSocketServer,WebSocket}=require('ws');
const PORT=Number(process.env.PORT||10000);
const ROOT=path.join(__dirname,'public');
const rooms=new Map();
const sockets=new Map();
const ranks='23456789TJQKA', suits=['♠','♥','♦','♣'];
const uid=()=>crypto.randomBytes(8).toString('hex');
const roomCode=()=>crypto.randomBytes(3).toString('hex').toUpperCase();
const makeDeck=()=>{const d=[];for(const s of suits)for(const r of ranks)d.push({r,s});for(let i=d.length-1;i>0;i--){const j=crypto.randomInt(i+1);[d[i],d[j]]=[d[j],d[i]]}return d};
const clamp=(n,a,b)=>Math.max(a,Math.min(b,n));
function evaluate5(cs){const vals=cs.map(c=>ranks.indexOf(c.r)+2).sort((a,b)=>b-a);const counts={};vals.forEach(v=>counts[v]=(counts[v]||0)+1);const uniq=[...new Set(vals)];if(uniq[0]===14)uniq.push(1);let sh=0;for(let i=0;i<=uniq.length-5;i++)if(uniq[i]-uniq[i+4]===4){sh=uniq[i];break}const flush=cs.every(c=>c.s===cs[0].s);const groups=Object.entries(counts).map(([v,n])=>({v:+v,n})).sort((a,b)=>b.n-a.n||b.v-a.v);let cat,k;if(flush&&sh){cat=8;k=[sh]}else if(groups[0].n===4){cat=7;k=[groups[0].v,groups.find(g=>g.n===1).v]}else if(groups[0].n===3&&groups[1]?.n===2){cat=6;k=[groups[0].v,groups[1].v]}else if(flush){cat=5;k=vals}else if(sh){cat=4;k=[sh]}else if(groups[0].n===3){cat=3;k=[groups[0].v,...groups.filter(g=>g.n===1).map(g=>g.v).sort((a,b)=>b-a)]}else if(groups[0].n===2&&groups[1]?.n===2){const ps=groups.filter(g=>g.n===2).map(g=>g.v).sort((a,b)=>b-a);cat=2;k=[...ps,groups.find(g=>g.n===1).v]}else if(groups[0].n===2){cat=1;k=[groups[0].v,...groups.filter(g=>g.n===1).map(g=>g.v).sort((a,b)=>b-a)]}else{cat=0;k=vals}return{cat,k}}
function combos(a,k){const out=[];const rec=(s,c)=>{if(c.length===k){out.push(c.slice());return}for(let i=s;i<=a.length-(k-c.length);i++){c.push(a[i]);rec(i+1,c);c.pop()}};rec(0,[]);return out}
const cmp=(a,b)=>{if(a.cat!==b.cat)return a.cat-b.cat;for(let i=0;i<Math.max(a.k.length,b.k.length);i++){const d=(a.k[i]||0)-(b.k[i]||0);if(d)return d}return 0};
function bestHand(hole,board,variant){let best=null,bestCards=null;if(variant==='plo'){for(const h of combos(hole,2))for(const b of combos(board,3)){const c=[...h,...b],e=evaluate5(c);if(!best||cmp(e,best)>0){best=e;bestCards={hole:h,board:b}}}}else{for(const c of combos([...hole,...board],5)){const e=evaluate5(c);if(!best||cmp(e,best)>0){best=e;bestCards=c}}}return{score:best,cards:bestCards}}
const handNames=['ไพ่สูง','หนึ่งคู่','สองคู่','ตอง','สเตรท','ฟลัช','ฟูลเฮาส์','โฟร์การ์ด','สเตรทฟลัช'];
function publicState(g,viewer){
 const now=Date.now(),levelRemain=g.mode==='tournament'&&g.tournamentStartedAt?Math.max(0,480-Math.floor((now-g.tournamentStartedAt)%480000/1000)):null;
 return{rev:g.rev||0,room:g.room,hostId:g.hostId,mode:g.mode,variant:g.variant,seatCount:g.seatCount,startingChips:g.startingChips,started:g.started,street:g.street,board:g.board,pot:g.pot,livePot:g.pot+g.players.reduce((s,p)=>s+p.bet,0),currentBet:g.currentBet,minRaise:g.minRaise,dealer:g.dealer,turn:g.turn,deadline:g.deadline||0,handNo:g.handNo,level:g.level,levelRemain,blinds:blindsFor(g),resultText:g.resultText,allInReveal:g.allInReveal,players:g.players.map((p,i)=>{
   let hole=p.hole.map(()=>null);
   if(!p.fold){
     if(p.id===viewer)hole=p.hole.map(x=>x);
     else if(g.street==='showdown'&&g.variant==='plo'&&p.best?.cards?.hole)hole=p.best.cards.hole.map(x=>x);
     else if(g.street==='showdown'||g.allInReveal)hole=p.hole.map(x=>x);
   }
   return{id:p.id,name:p.name,seat:i,chips:p.chips,bet:p.bet,fold:p.fold,allIn:p.allIn,connected:p.connected,avatar:p.avatar,hole,sittingOut:!!p.sittingOut,inHand:!!p.inHand,leaving:!!p.leaving,handName:g.street==='showdown'&&p.showScore?handNames[p.showScore.cat]:null,winner:!!p.winner};
 }),chat:g.chat.slice(-30)}
}
function broadcast(g){g.rev=(g.rev||0)+1;for(const p of g.players){const ws=sockets.get(p.id);if(ws&&ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify({type:'state',state:publicState(g,p.id)}))}}
function postSystem(g,text){g.chat.push({id:uid(),system:true,text,ts:Date.now()})}
function nextLive(g,from){for(let n=1;n<=g.players.length;n++){const i=(from+n)%g.players.length,p=g.players[i];if(p&&!p.fold&&p.chips+p.bet>0)return i}return -1}
function nextActor(g,from){for(let n=1;n<=g.players.length;n++){const i=(from+n)%g.players.length,p=g.players[i];if(p&&!p.fold&&!p.allIn&&p.chips>0)return i}return -1}
function blindsFor(g){if(g.mode==='cash')return[Math.max(1,Math.floor(g.cashBB/2)),g.cashBB];if(g.tournamentStartedAt)g.level=Math.min(9,Math.floor((Date.now()-g.tournamentStartedAt)/480000));return[[50,100],[75,150],[100,200],[150,300],[200,400],[300,600],[400,800],[600,1200],[800,1600],[1000,2000]][g.level||0]}
function take(g,i,n){const p=g.players[i],x=Math.min(p.chips,n);p.chips-=x;p.bet+=x;if(p.chips===0)p.allIn=true;return x}
function clearTurnTimer(g){if(g._turnTimer){clearTimeout(g._turnTimer);g._turnTimer=null}g.deadline=0}
function setTurn(g,i){clearTurnTimer(g);g.turn=i;if(i<0){broadcast(g);return}const token=(g._turnToken||0)+1;g._turnToken=token;g.deadline=Date.now()+60000;broadcast(g);g._turnTimer=setTimeout(()=>{if(g._turnToken!==token||g.turn!==i||g.street==='showdown'||!g.started)return;const p=g.players[i];if(!p||p.fold||p.allIn)return;const need=Math.max(0,g.currentBet-p.bet);applyAction(g,p.id,{kind:need>0?'fold':'call',timeout:true})},60050)}
function eligibleNext(p){return !!(p&&p.chips>0&&!p.sittingOut&&!p.leaving&&p.connected)}
function ensureHost(g){if(g.players.some(p=>p.id===g.hostId&&!p.leaving))return;const h=g.players.find(p=>p.connected&&!p.leaving)||g.players.find(p=>!p.leaving);g.hostId=h?h.id:null}
function cleanupLeavers(g){const dealerId=g.players[g.dealer]?.id||null;g.players=g.players.filter(p=>!p.leaving);ensureHost(g);g.dealer=dealerId?g.players.findIndex(p=>p.id===dealerId):-1;if(g.dealer<0)g.dealer=-1;if(!g.players.length){rooms.delete(g.room);return false}return true}
function maybeWaitOrStart(g){if(!g.started)return;if(['preflop','flop','turn','river'].includes(g.street))return;const n=g.players.filter(eligibleNext).length;if(n>=2)startHand(g);else{clearTurnTimer(g);g.street='waiting';g.turn=-1;g.board=[];g.pot=0;g.resultText='รอผู้เล่นพร้อมอย่างน้อย 2 คน';broadcast(g)}}
function externalFold(g,i){const p=g.players[i];if(!p||p.fold||!p.inHand)return broadcast(g);p.fold=true;p.acted=true;if(remaining(g).length===1)return foldWin(g);if(g.turn===i){if(roundDone(g)){const actors=g.players.filter(q=>!q.fold&&!q.allIn&&q.chips>0);if(actors.length===0)return runout(g);return advance(g)}return setTurn(g,nextActor(g,i))}broadcast(g)}
function startHand(g){
 if(!cleanupLeavers(g))return;
 const active=g.players.filter(eligibleNext);
 if(active.length<2){clearTurnTimer(g);g.street='waiting';g.turn=-1;g.board=[];g.pot=0;g.resultText='รอผู้เล่นพร้อมอย่างน้อย 2 คน';broadcast(g);return}
 if(g.mode==='tournament'&&!g.tournamentStartedAt)g.tournamentStartedAt=Date.now();blindsFor(g);g.handNo++;g.deck=makeDeck();g.board=[];g.pot=0;g.street='preflop';g.currentBet=0;g.minRaise=blindsFor(g)[1];g.resultText='';g.allInReveal=false;
 g.players.forEach(p=>{p.bet=0;p.totalContrib=0;p.inHand=eligibleNext(p);p.fold=!p.inHand;p.allIn=false;p.hole=[];p.acted=false;p.showScore=null;p.best=null;p.winner=false});
 g.dealer=nextLive(g,g.dealer);const live=g.players.filter(p=>p.inHand&&!p.fold).length,heads=live===2;const sb=heads?g.dealer:nextLive(g,g.dealer);const bb=nextLive(g,sb),[s,b]=blindsFor(g);const sbPaid=take(g,sb,s),bbPaid=take(g,bb,b);g.players[sb].totalContrib+=sbPaid;g.players[bb].totalContrib+=bbPaid;g.currentBet=Math.max(g.players[sb].bet,g.players[bb].bet);const hc=g.variant==='plo'?4:2;for(const p of g.players)if(p.inHand&&!p.fold)for(let i=0;i<hc;i++)p.hole.push(g.deck.pop());const first=heads?sb:nextActor(g,bb);postSystem(g,`Hand #${g.handNo} • ${s}/${b}`);if(first<0)runout(g);else setTurn(g,first)
}
function collect(g){for(const p of g.players){g.pot+=p.bet;p.bet=0;p.acted=false}g.currentBet=0}
function remaining(g){return g.players.filter(p=>!p.fold)}
function roundDone(g){const a=g.players.filter(p=>!p.fold&&!p.allIn&&p.chips>0);return a.length===0||a.every(p=>p.acted&&p.bet===g.currentBet)}
function advance(g){collect(g);if(g.street==='preflop'){g.deck.pop();g.board.push(g.deck.pop(),g.deck.pop(),g.deck.pop());g.street='flop'}else if(g.street==='flop'){g.deck.pop();g.board.push(g.deck.pop());g.street='turn'}else if(g.street==='turn'){g.deck.pop();g.board.push(g.deck.pop());g.street='river'}else return showdown(g);g.minRaise=blindsFor(g)[1];const a=g.players.findIndex(p=>!p.fold&&!p.allIn&&p.chips>0);if(a<0){if(g.allInReveal){broadcast(g);return}return runout(g)}setTurn(g,nextActor(g,g.dealer))}
function runout(g){clearTurnTimer(g);g.allInReveal=true;broadcast(g);const step=()=>{if(g.street==='river')return showdown(g);advance(g);if(g.street!=='showdown'&&g.allInReveal)setTimeout(step,700)};setTimeout(step,700)}
function sidePots(g){const contrib=g.players.map(p=>p.totalContrib||0);const levels=[...new Set(contrib.filter(x=>x>0))].sort((a,b)=>a-b);let prev=0;const pots=[];for(const lv of levels){const elig=g.players.map((p,i)=>({p,i,c:contrib[i]})).filter(x=>x.c>=lv);const amount=(lv-prev)*elig.length;const contenders=elig.filter(x=>!x.p.fold).map(x=>x.i);if(amount>0)pots.push({amount,contenders});prev=lv}return pots}
function showdown(g){clearTurnTimer(g);collect(g);g.street='showdown';for(const p of g.players){p.winner=false;if(!p.fold){const b=bestHand(p.hole,g.board,g.variant);p.showScore=b.score;p.best=b}}const pots=sidePots(g);if(!pots.length)pots.push({amount:g.pot,contenders:g.players.map((p,i)=>!p.fold?i:-1).filter(i=>i>=0)});for(const pot of pots){let best=null,win=[];for(const i of pot.contenders){const s=g.players[i].showScore;if(!best||cmp(s,best)>0){best=s;win=[i]}else if(cmp(s,best)===0)win.push(i)}const share=Math.floor(pot.amount/win.length),rem=pot.amount-share*win.length;win.forEach((i,k)=>{g.players[i].chips+=share+(k<rem?1:0);g.players[i].winner=true})}g.pot=0;const ws=g.players.filter(p=>p.winner);g.resultText=ws.map(p=>p.name).join(', ')+' ชนะ';postSystem(g,g.resultText);broadcast(g);setTimeout(()=>{if(g.started)startHand(g)},4000)}
function foldWin(g){clearTurnTimer(g);const w=remaining(g)[0];collect(g);w.chips+=g.pot;g.pot=0;g.street='showdown';g.resultText=w.name+' ชนะ';w.winner=true;broadcast(g);setTimeout(()=>{if(g.started)startHand(g)},2500)}
function applyAction(g,pid,a){const i=g.players.findIndex(p=>p.id===pid);if(i<0||i!==g.turn||g.street==='showdown'||!g.started)return false;const p=g.players[i];if(p.fold||p.allIn)return false;const need=Math.max(0,g.currentBet-p.bet);p.totalContrib=(p.totalContrib||0);if(a.kind==='fold')p.fold=true;else if(a.kind==='call'){const x=take(g,i,need);p.totalContrib+=x}else if(a.kind==='allin'){const x=take(g,i,p.chips);p.totalContrib+=x;if(p.bet>g.currentBet){g.minRaise=Math.max(g.minRaise,p.bet-g.currentBet);g.currentBet=p.bet;g.players.forEach(q=>{if(q.id!==p.id&&!q.fold&&!q.allIn)q.acted=false})}}else if(a.kind==='raise'){const target=clamp(Number(a.target)||0,g.currentBet+g.minRaise,p.bet+p.chips);if(target<=g.currentBet)return false;const x=take(g,i,target-p.bet);p.totalContrib+=x;g.minRaise=Math.max(g.minRaise,target-g.currentBet);g.currentBet=target;g.players.forEach(q=>{if(q.id!==p.id&&!q.fold&&!q.allIn)q.acted=false})}else return false;p.acted=true;if(remaining(g).length===1){foldWin(g);return true}if(roundDone(g)){const actors=g.players.filter(q=>!q.fold&&!q.allIn&&q.chips>0);if(actors.length===0){runout(g);return true}advance(g);return true}setTurn(g,nextActor(g,i));return true}
function newRoom(o,host){return{room:o.room,hostId:host.id,mode:o.mode==='cash'?'cash':'tournament',variant:o.variant==='plo'?'plo':'holdem',seatCount:clamp(+o.seatCount||6,2,10),startingChips:clamp(+o.startingChips||5000,1000,100000),cashBB:clamp(+o.cashBB||50,20,100),players:[host],started:false,street:'waiting',deck:[],board:[],pot:0,currentBet:0,minRaise:100,dealer:-1,turn:-1,handNo:0,level:0,tournamentStartedAt:0,deadline:0,blinds:[50,100],chat:[],resultText:'',allInReveal:false,rev:0}}
function serve(req,res){let p=req.url.split('?')[0];if(p==='/')p='/index.html';const fp=path.join(ROOT,p);if(!fp.startsWith(ROOT)||!fs.existsSync(fp)){res.writeHead(404);return res.end('not found')}const ext=path.extname(fp);const types={'.html':'text/html; charset=utf-8','.js':'application/javascript; charset=utf-8','.css':'text/css; charset=utf-8'};res.writeHead(200,{'Content-Type':types[ext]||'application/octet-stream','Cache-Control':'no-store'});fs.createReadStream(fp).pipe(res)}
const server=http.createServer((req,res)=>{if(req.url.startsWith('/api/health')){res.writeHead(200,{'Content-Type':'application/json'});return res.end(JSON.stringify({ok:true,rooms:rooms.size}))}serve(req,res)});
const wss=new WebSocketServer({server,path:'/ws',perMessageDeflate:false});
wss.on('connection',ws=>{try{ws._socket&&ws._socket.setNoDelay(true)}catch{};let pid=null;ws.on('message',raw=>{let m;try{m=JSON.parse(raw)}catch{return}if(m.type==='ping'){try{ws.send(JSON.stringify({type:'pong',t:m.t,serverAt:Date.now()}))}catch{};return}if(m.type==='hello'){pid=String(m.playerId||uid());sockets.set(pid,ws);ws.send(JSON.stringify({type:'hello',playerId:pid}));return}if(!pid)return;if(m.type==='create'){let code;do{code=roomCode()}while(rooms.has(code));const p={id:pid,name:String(m.name||'PLAYER').slice(0,20),chips:+m.startingChips||5000,bet:0,hole:[],fold:false,allIn:false,connected:true,avatar:m.avatar||null,totalContrib:0,sittingOut:false,inHand:false,leaving:false};const g=newRoom({...m,room:code},p);rooms.set(code,g);ws.room=code;broadcast(g)}else if(m.type==='join'){
 const code=String(m.room||'').toUpperCase(),g=rooms.get(code);if(!g)return ws.send(JSON.stringify({type:'error',message:'ไม่พบห้อง'}));
 let p=g.players.find(x=>x.id===pid);
 if(!p){
   if(g.players.length>=g.seatCount)return ws.send(JSON.stringify({type:'error',message:'โต๊ะเต็ม'}));
   p={id:pid,name:String(m.name||'PLAYER').slice(0,20),chips:g.startingChips,bet:0,hole:[],fold:true,allIn:false,connected:true,avatar:m.avatar||null,totalContrib:0,sittingOut:!!g.started,inHand:false,leaving:false};
   g.players.push(p)
 }else{p.connected=true;p.leaving=false;if(g.started&&!p.inHand)p.sittingOut=true}
 ws.room=code;ensureHost(g);broadcast(g)
}else if(m.type==='start'){
 const g=rooms.get(ws.room);if(g&&g.hostId===pid&&!g.started&&g.players.filter(eligibleNext).length>=2){g.started=true;g.tournamentStartedAt=g.mode==='tournament'?Date.now():0;g.players.forEach(p=>{if(p.chips<=0)p.chips=g.startingChips});startHand(g)}
}else if(m.type==='action'){
 const g=rooms.get(ws.room);
 if(g&&m.actionId&&!g.lastActions?.has?.(m.actionId)){
   try{ws.send(JSON.stringify({type:'action_ack',actionId:m.actionId,receivedAt:Date.now()}))}catch{};
   if(!g.lastActions)g.lastActions=new Set();g.lastActions.add(m.actionId);if(g.lastActions.size>200)g.lastActions.delete(g.lastActions.values().next().value);
   const ok=applyAction(g,pid,m.action||{});
   try{ws.send(JSON.stringify({type:'action_result',actionId:m.actionId,ok,state:publicState(g,pid)}))}catch{}
 }
}else if(m.type==='sitout'){
 const g=rooms.get(ws.room);if(g){const p=g.players.find(x=>x.id===pid);if(p){p.sittingOut=!!m.value;broadcast(g);if(!p.sittingOut&&g.started&&g.street==='waiting')maybeWaitOrStart(g)}}
}else if(m.type==='leave'){
 const g=rooms.get(ws.room);if(g){const i=g.players.findIndex(x=>x.id===pid),p=g.players[i];if(p){p.sittingOut=true;p.leaving=true;p.connected=false;ensureHost(g);if(p.inHand&&!p.fold&&['preflop','flop','turn','river'].includes(g.street))externalFold(g,i);else if(!g.started||g.street==='waiting'){cleanupLeavers(g);if(rooms.has(g.room))broadcast(g)}else broadcast(g)}}
 ws.room=null;try{ws.send(JSON.stringify({type:'left'}))}catch{}
}else if(m.type==='chat'){
 const g=rooms.get(ws.room);if(g){const p=g.players.find(x=>x.id===pid);if(p){g.chat.push({id:uid(),name:p.name,text:String(m.text||'').slice(0,120),ts:Date.now()});broadcast(g)}}
}else if(m.type==='rejoin'){
 const g=rooms.get(String(m.room||'').toUpperCase());if(g){const p=g.players.find(x=>x.id===pid);if(p&&!p.leaving){p.connected=true;ws.room=g.room;broadcast(g)}}
}}}});ws.on('close',()=>{if(pid)sockets.delete(pid);const g=rooms.get(ws.room);if(g){const p=g.players.find(x=>x.id===pid);if(p)p.connected=false;broadcast(g)}})});
function startServer(){server.listen(PORT,'0.0.0.0',()=>{console.log('FRIEND POKER v2 listening',PORT);setInterval(()=>{for(const ws of wss.clients){if(ws.isAlive===false){try{ws.terminate()}catch{};continue}ws.isAlive=false;try{ws.ping()}catch{}}},25000)});return server}
wss.on('connection',ws=>{ws.isAlive=true;ws.on('pong',()=>ws.isAlive=true)});
if(require.main===module)startServer();
module.exports={evaluate5,bestHand,cmp,startServer,server,wss};