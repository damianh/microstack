import http from 'node:http';
import {readFile,readdir} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
const root=fileURLToPath(new URL('.',import.meta.url));
const iconFiles=(await readdir(path.join(root,'icons'))).filter(name=>/^[a-z0-9-]+\.svg$/.test(name));
const allowed=new Set(['index.html','styles.css','app.js','logo.png','notes.html','qa.html',...iconFiles.map(name=>'icons/'+name)]);
const types={'.html':'text/html; charset=utf-8','.css':'text/css; charset=utf-8','.js':'text/javascript; charset=utf-8','.png':'image/png','.svg':'image/svg+xml'};
const server=http.createServer(async(req,res)=>{
  const name=new URL(req.url,'http://localhost').pathname.slice(1)||'index.html';
  if(!allowed.has(name)){res.writeHead(404);res.end('Not found');return;}
  try{
    const file=await readFile(path.join(root,name));
    res.writeHead(200,{'Content-Type':types[path.extname(name)],'Cache-Control':'no-store','X-Content-Type-Options':'nosniff'});
    res.end(file);
  }catch(error){console.error(error);res.writeHead(500);res.end('Prototype asset could not be read.');}
});
server.listen(process.env.PORT?Number(process.env.PORT):0,'127.0.0.1',()=>console.log(`PROTOTYPE_URL=http://127.0.0.1:${server.address().port}/`));
for(const signal of ['SIGINT','SIGTERM'])process.on(signal,()=>server.close(()=>process.exit(0)));
