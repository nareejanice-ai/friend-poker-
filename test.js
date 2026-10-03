process.env.PORT='19123';
const {evaluate5,bestHand,startServer}=require('./server');
const WebSocket=require('ws');

function cards(s){return [...s.matchAll(/(10|[2-9TJQKA])([SHDC])/g)].map(m=>({r:m[1]==='10'?'T':m[1],s:{S:'♠',H:'♥',D:'♦',C:'♣'}[m[2]]}))}
function assert(v,msg){if(!v)throw new Error(msg)}
assert(evaluate5(cards('AS KS QS JS 10S')).cat===8,'straight flush');
assert(evaluate5(cards('AH AD AC KS KD')).cat===6,'full house');
assert(evaluate5(cards('AS 5H 4D 3C 2S')).cat===4,'wheel straight');
assert(bestHand(cards('AS AH 2C 3D'),cards('AC AD KS QS JS'),'plo').score.cat===7,'PLO exact 2+3');
console.log('ENGINE TEST PASS');

const delay=ms=>new Promise(r=>setTimeout(r,ms));
function client(id){
 return new Promise((resolve,reject)=>{
  const ws=new WebSocket('ws://127.0.0.1:19123/ws');
  const q=[],waiters=[];
  ws.on('open',()=>ws.send(JSON.stringify({type:'hello',playerId:id})));
  ws.on('message',raw=>{
   const m=JSON.parse(raw);
   if(waiters.length){for(let i=0;i<waiters.length;i++){if(waiters[i].pred(m)){const w=waiters.splice(i,1)[0];clearTimeout(w.t);w.ok(m);return}}}
   q.push(m);
  });
  ws.on('error',reject);
  const wait=(pred,ms=2500)=>new Promise((ok,no)=>{
   const hit=q.findIndex(pred);if(hit>=0)return ok(q.splice(hit,1)[0]);
   const w={pred,ok,no,t:setTimeout(()=>{const i=waiters.indexOf(w);if(i>=0)waiters.splice(i,1);no(new Error('timeout '+id))},ms)};waiters.push(w);
  });
  ws.once('open',async()=>{try{const hello=await wait(m=>m.type==='hello');resolve({ws,wait,id:hello.playerId})}catch(e){reject(e)}});
 });
}
(async()=>{
 const server=startServer();
 await delay(120);
 const h=await client('HOST-T'),g=await client('GUEST-T');
 h.ws.send(JSON.stringify({type:'create',name:'HOST',variant:'holdem',seatCount:6,startingChips:4000,mode:'tournament'}));
 const hs=await h.wait(m=>m.type==='state'&&m.state.players.length===1);
 const room=hs.state.room;
 g.ws.send(JSON.stringify({type:'join',room,name:'GUEST'}));
 await g.wait(m=>m.type==='state'&&m.state.players.length===2);
 await h.wait(m=>m.type==='state'&&m.state.players.length===2);
 h.ws.send(JSON.stringify({type:'start'}));
 let st=await h.wait(m=>m.type==='state'&&m.state.started&&m.state.street==='preflop');
 assert(st.state.turn===0,'heads-up host should act first');
 h.ws.send(JSON.stringify({type:'action',actionId:'a-host',action:{kind:'call'}}));
 await h.wait(m=>m.type==='action_result'&&m.actionId==='a-host'&&m.ok);
 const guestTurn=await g.wait(m=>m.type==='state'&&m.state.players[m.state.turn]?.id==='GUEST-T');
 assert(guestTurn.state.street==='preflop','guest receives host action state');
 g.ws.send(JSON.stringify({type:'action',actionId:'a-guest',action:{kind:'call'}}));
 const gr=await g.wait(m=>m.type==='action_result'&&m.actionId==='a-guest'&&m.ok);
 assert(gr.state.street==='flop','secondary player receives direct authoritative state after action');

 const p3=await client('LATE-T');
 p3.ws.send(JSON.stringify({type:'join',room,name:'LATE'}));
 const late=await p3.wait(m=>m.type==='state'&&m.state.players.length===3);
 const lp=late.state.players.find(p=>p.id==='LATE-T');
 assert(lp&&lp.sittingOut&&!lp.inHand,'late join starts sitting out');
 p3.ws.send(JSON.stringify({type:'sitout',value:false}));
 const ready=await p3.wait(m=>m.type==='state'&&m.state.players.find(p=>p.id==='LATE-T')?.sittingOut===false);
 assert(ready.state.players.find(p=>p.id==='LATE-T').inHand===false,'late player waits until next hand');
 p3.ws.send(JSON.stringify({type:'leave'}));
 await p3.wait(m=>m.type==='left');
 console.log('MULTIPLAYER TEST PASS');
 [h,g,p3].forEach(c=>{try{c.ws.close()}catch{}});
 server.close(()=>process.exit(0));
 setTimeout(()=>process.exit(0),300);
})().catch(e=>{console.error('TEST FAIL',e);process.exit(1)});