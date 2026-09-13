'use strict';

const icons = {
  search:'<circle cx="10" cy="10" r="6"/><path d="M15 15l5 5"/>',
  copy:'<rect x="8" y="8" width="12" height="13" rx="2"/><path d="M15 8V3H3v13h5"/>',
  refresh:'<path d="M20 8a8 8 0 1 0 0 8M20 3v5h-5"/>',
  info:'<circle cx="12" cy="12" r="9"/><path d="M12 11v6M12 7h.01"/>',
  arrow:'<path d="M4 12h15M14 7l5 5-5 5"/>',
  chevron:'<path d="m7 10 5 5 5-5"/>',
  back:'<path d="M20 12H5M10 7l-5 5 5 5"/>',
  file:'<path d="M5 3h9l5 5v13H5zM14 3v6h5M9 13h6M9 17h6"/>',
  folder:'<path d="M3 6h7l2 3h9v11H3z"/>',
  warning:'<path d="M12 3L2 21h20zM12 9v5M12 17h.01"/>',
  link:'<path d="M10 14l4-4M7 16l-1 1a4 4 0 0 1-6-6l5-5a4 4 0 0 1 6 0M17 8l1-1a4 4 0 0 1 6 6l-5 5a4 4 0 0 1-6 0" transform="translate(1 0) scale(.9)"/>'
};
const icon = name => serviceIconNames.has(name)?`<img class="aws-service-icon" src="icons/${name}.svg" width="24" height="24" alt="">`:`<svg viewBox="0 0 24 24" aria-hidden="true">${icons[name] || icons.file}</svg>`;
const esc = value => String(value ?? '').replace(/[&<>"']/g, char => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[char]));
const tag = (text, tone='') => `<span class="tag ${tone}">${esc(text)}</span>`;
const account = '000000000000';
const arn = (service, name) => service === 's3' ? `arn:aws:s3:::${name}` : `arn:aws:${service}:us-east-1:${account}:${name}`;
const message = (id, status, order, extra={}) => ({
  name:id, status, meta:'2026-09-11T10:23:42Z · 482 B', kind:'json',
  body:{eventType:'OrderPlaced', orderId:order, customerId:'cus_2048', total:{amount:129.90,currency:'EUR'}, items:[{sku:'FIELD-NOTES-A5',quantity:2}], shipping:{city:'Zürich',country:'CH'}, ...extra},
  facts:{'Content type':'application/json','Receive count':status==='In flight'?'1':'0','Message group':'Not a FIFO queue'}
});
const link = (label, service, resource, relation, extra={}) => ({label,service,resource,relation,...extra});
const services = {
  sqs:{
    label:'SQS', name:'Simple Queue Service', plural:'Queues', singular:'Queue', content:'Messages',
    description:'Inspect queue snapshots, message contents, and configured producers.',
    resources:[
      {name:'orders-processing',type:'Standard queue',status:'Available',
        counts:[['12','Visible'],['2','Delayed'],['1','In flight']], total:15,
        items:[message('8f3a2c10 · ORD-1042','Visible','ORD-1042'),message('bc72e934 · ORD-1043','Visible','ORD-1043'),message('de19a280 · ORD-1044','In flight','ORD-1044')],
        configuration:{'Queue type':'Standard','Visibility timeout':'30 seconds','Message retention':'4 days','Delivery delay':'0 seconds','Maximum message size':'256 KiB','Dead-letter queue':'orders-dlq'},
        connections:[link('order-events','sns',0,'Subscription destination'),link('order-routing','events',0,'EventBridge rule target'),link('orders-dlq','sqs',1,'Dead-letter destination')],
      },
      {name:'orders-dlq',type:'Standard queue',status:'Available',counts:[['1','Visible'],['0','Delayed'],['0','In flight']],total:1,
        items:[message('a29411ce · ORD-1031','Visible','ORD-1031',{retryReason:'InventoryReservationFailed'})],
        configuration:{'Queue type':'Standard','Message retention':'14 days','Visibility timeout':'30 seconds'},
        connections:[link('orders-processing','sqs',0,'Configured redrive source')]},
      {name:'notifications-outbox.fifo',type:'FIFO queue',status:'Available',counts:[['0','Visible'],['0','Delayed'],['0','In flight']],total:0,items:[],
        configuration:{'Queue type':'FIFO','Content-based deduplication':'Enabled','Message retention':'4 days'},connections:[]}
    ]
  },
  s3:{
    label:'S3',name:'Simple Storage Service',plural:'Buckets',singular:'Bucket',content:'Objects',
    description:'Browse object keys and inspect metadata without downloading everything.',
    resources:[
      {name:'order-exports',type:'Bucket',status:'Available',root:'2026/09/11/',total:5,
        items:[
          {name:'2026/09/11/orders-001.json',kind:'json',meta:'2.4 KiB · application/json',status:'Object',body:{exportId:'exp_20260911_001',generatedAt:'2026-09-11T10:20:00Z',orders:[{orderId:'ORD-1042',state:'placed',total:129.90}],nextCursor:null},facts:{'Content type':'application/json','Size':'2.4 KiB','Last modified':'2026-09-11T10:20:00Z','ETag':'"d41d8cd98f00b204e9800998ecf8427e"'}},
          {name:'2026/09/11/manifest.csv',kind:'text',meta:'182 B · text/csv',status:'Object',body:'order_id,state,total,currency\nORD-1042,placed,129.90,EUR\nORD-1043,placed,45.00,EUR',facts:{'Content type':'text/csv','Size':'182 B'}},
          {name:'2026/09/11/invoice-1042.pdf',kind:'binary',meta:'48 KiB · application/pdf',status:'Object',facts:{'Content type':'application/pdf','Size':'48 KiB'}},
          {name:'2026/09/11/full-export.json',kind:'oversized',meta:'24 MiB · application/json',status:'Object',facts:{'Content type':'application/json','Size':'24 MiB','Preview policy':'Bounded preview required; limit undecided'}},
          {name:'README.txt',kind:'text',meta:'124 B · text/plain',status:'Object',body:'Synthetic export examples for the MicroStack resource explorer.\nThese objects are demo fixtures, not real customer records.',facts:{'Content type':'text/plain','Size':'124 B'}}
        ],
        configuration:{'Region':'us-east-1','Versioning':'Disabled','Object count':'5','Encryption':'Not configured','Tags':'environment=local'},connections:[]},
      {name:'microstack-assets',type:'Bucket',status:'Available',items:[{name:'brand/logo.png',kind:'binary',meta:'17 KiB · image/png',status:'Object',facts:{'Content type':'image/png','Size':'17 KiB'}}],configuration:{'Region':'us-east-1','Versioning':'Disabled'},connections:[]},
      {name:'audit-archive',type:'Bucket',status:'Available',items:[],configuration:{'Region':'us-east-1','Versioning':'Enabled'},connections:[]}
    ]
  },
  dynamodb:{
    label:'DynamoDB',name:'DynamoDB',plural:'Tables',singular:'Table',content:'Items',
    description:'Find keys and inspect typed item values, including nested documents.',
    resources:[
      {name:'Orders',type:'Table',status:'Active',total:128,
        counts:[['PK','Partition key'],['SK','Sort key'],['128','Items in sample table']],
        items:[
          {name:'ORDER#1042 / META',meta:'PK: ORDER#1042 · SK: META',status:'Item',kind:'json',body:{PK:{S:'ORDER#1042'},SK:{S:'META'},status:{S:'PLACED'},total:{N:'129.90'},customer:{M:{id:{S:'cus_2048'},city:{S:'Zürich'}}},expedited:{BOOL:false}},facts:{'Partition key':'ORDER#1042 (S)','Sort key':'META (S)','Display':'DynamoDB typed JSON'}},
          {name:'ORDER#1043 / META',meta:'PK: ORDER#1043 · SK: META',status:'Item',kind:'json',body:{PK:{S:'ORDER#1043'},SK:{S:'META'},status:{S:'PLACED'},total:{N:'45.00'},tags:{SS:['local','demo']}},facts:{'Partition key':'ORDER#1043 (S)','Sort key':'META (S)'}},
          {name:'ORDER#1042 / LINE#001',meta:'PK: ORDER#1042 · SK: LINE#001',status:'Item',kind:'json',body:{PK:{S:'ORDER#1042'},SK:{S:'LINE#001'},sku:{S:'FIELD-NOTES-A5'},quantity:{N:'2'}},facts:{'Partition key':'ORDER#1042 (S)','Sort key':'LINE#001 (S)'}}
        ],configuration:{'Status':'ACTIVE','Partition key':'PK (String)','Sort key':'SK (String)','Billing mode':'PAY_PER_REQUEST','Item count':'128','Indexes':'0'},connections:[]},
      {name:'Customers',type:'Table',status:'Active',items:[],configuration:{'Partition key':'CustomerId (String)','Billing mode':'PAY_PER_REQUEST'},connections:[]}
    ]
  },
  sns:{
    label:'SNS',name:'Simple Notification Service',plural:'Topics',singular:'Topic',content:'Subscriptions',
    description:'Inspect subscriptions and follow configured delivery destinations.',
    resources:[
      {name:'order-events',type:'Standard topic',status:'Available',items:[],
        subscriptions:[
          {name:'orders-processing',protocol:'sqs',endpoint:arn('sqs','orders-processing'),state:'Confirmed',service:'sqs',resource:0,filter:'{"eventType":["OrderPlaced"]}'},
          {name:'analytics-ingest',protocol:'https',endpoint:'https://analytics.example.test/events',state:'External endpoint',filter:'No filter policy'}
        ],
        configuration:{'Type':'Standard','Subscription count':'2','Display name':'Order events','Tags':'environment=local'},
        connections:[link('orders-processing','sqs',0,'SQS subscription'),link('analytics-ingest',null,null,'HTTPS subscription',{external:true})]},
      {name:'customer-updates',type:'Standard topic',status:'Available',items:[],subscriptions:[],configuration:{'Type':'Standard','Subscription count':'0'},connections:[]}
    ]
  },
  events:{
    label:'EventBridge',name:'EventBridge',plural:'Event buses',singular:'Event bus',content:'Rules',
    description:'Inspect rule patterns and navigate the targets they are configured to use.',
    resources:[
      {name:'order-routing',type:'Custom event bus',status:'Available',items:[],
        rules:[
          {name:'route-new-orders',status:'Enabled',description:'OrderPlaced events from checkout',pattern:{source:['checkout'], 'detail-type':['OrderPlaced']},targets:[link('orders-processing','sqs',0,'SQS target')]},
          {name:'notify-order-updates',status:'Disabled',description:'OrderUpdated events from checkout',pattern:{source:['checkout'],'detail-type':['OrderUpdated']},targets:[link('order-events','sns',0,'SNS target')]},
          {name:'retired-integration',status:'Enabled',description:'Example with a missing destination',pattern:{source:['legacy-import']},targets:[link('legacy-orders',null,null,'SQS target',{missing:true})]}
        ],
        configuration:{'Bus name':'order-routing','Type':'Custom','Rules':'3','Tags':'environment=local'},
        connections:[link('orders-processing','sqs',0,'route-new-orders → SQS target'),link('order-events','sns',0,'notify-order-updates → SNS target'),link('legacy-orders',null,null,'retired-integration → SQS target',{missing:true})]},
      {name:'default',type:'Default event bus',status:'Available',items:[],rules:[],configuration:{'Bus name':'default','Rules':'0'},connections:[]}
    ]
  }
};
const directoryGroups = {
  'Messaging & workflows':[['sqs','SQS'],['sns','SNS'],['events','EventBridge'],['ses','SES'],['stepfunctions','Step Functions']],
  'Storage & databases':[['s3','S3'],['s3files','S3 Files'],['efs','EFS'],['dynamodb','DynamoDB'],['rds','RDS'],['rdsdata','RDS Data'],['elasticache','ElastiCache']],
  'Compute & containers':[['lambda','Lambda'],['ec2','EC2'],['ecs','ECS'],['ecr','ECR']],
  'Networking & delivery':[['apigateway','API Gateway REST'],['apigatewayv2','API Gateway HTTP / WebSocket'],['alb','Application Load Balancer'],['appsync','AppSync'],['cloudfront','CloudFront'],['route53','Route 53'],['servicediscovery','Cloud Map']],
  'Security & identity':[['acm','Certificate Manager'],['cognitoidp','Cognito User Pools'],['cognitoidentity','Cognito Identity Pools'],['iam','IAM'],['kms','KMS'],['secretsmanager','Secrets Manager'],['sts','STS'],['waf','WAF']],
  'Analytics & streaming':[['athena','Athena'],['emr','EMR'],['firehose','Data Firehose'],['glue','Glue'],['kinesis','Kinesis']],
  'Management & observability':[['cloudformation','CloudFormation'],['cloudwatch','CloudWatch'],['logs','CloudWatch Logs'],['ssm','Systems Manager']]
};
const directoryCount = Object.values(directoryGroups).reduce((total,entries)=>total+entries.length,0);
const serviceIconNames = new Set(Object.values(directoryGroups).flat().map(([id])=>id));
const sortedServices = Object.values(directoryGroups).flat().map(([id,label])=>[id,{
  label,name:services[id]?.name||label,available:!!services[id]
}]).sort(([,a],[,b])=>a.label.localeCompare(b.label,'en'));
const query = new URLSearchParams(location.search);
const state = {home:query.get('view')==='home'||!query.has('service'),serviceFilter:'',inspectableOnly:false,service:services[query.get('service')]?query.get('service'):'sqs',resource:0,item:0,tab:'contents',scenario:'populated',filter:'',resourceFilter:'',page:0,prefix:'',browsing:false,trail:[]};
let timer, toastTimer;
let servicePickerOpen=false, activeServiceIndex=0;
const svc = () => services[state.service];
const resource = () => svc().resources[state.resource];
const $ = selector => document.querySelector(selector);
const notice = (text, warning=false) => `<div class="notice ${warning?'warning':''}">${icon(warning?'warning':'info')}<p>${text}</p></div>`;
function searchInput(id, placeholder, value='') {
  return `<div class="search">${icon('search')}<input id="${id}" aria-label="${esc(placeholder)}" placeholder="${esc(placeholder)}" value="${esc(value)}" type="search" autocomplete="off"></div>`;
}
function copyButton(type, label='Copy identifier') {
  return `<button class="icon-button" data-action="copy" data-type="${type}" aria-label="${label}" title="${label}">${icon('copy')}</button>`;
}
function jsonHtml(object) {
  const text = typeof object==='string'?object:JSON.stringify(object,null,2);
  return esc(text).replace(/(&quot;(?:[^&]|&(?!quot;))*?&quot;)(\s*:)?|\b(true|false|null|\d+(?:\.\d+)?)\b/g,(all,string,colon,number) => {
    if(string) return `<span class="${colon?'json-key':'json-string'}">${string}</span>${colon||''}`;
    return `<span class="json-number">${number}</span>`;
  });
}
function render() {
  servicePickerOpen=false;
  document.body.className = 'focused'+(state.home?' home':'')+(state.browsing?' browsing':'');
  $('#service-home').hidden = !state.home;
  $('#service-context').hidden = state.home;
  $('.header-title').textContent = state.home?'Services':'Resource explorer';
  $('.skip').textContent = state.home?'Skip to services':'Skip to resource inspector';
  if(state.home) renderHome();
  $('#service-context').innerHTML = `<div class="service-breadcrumb"><button class="link-button" data-action="home">All services</button><span aria-hidden="true">/</span>
    <div class="service-picker">
      <button id="service-switcher" class="service-picker-trigger" data-action="toggle-service-picker" aria-label="Switch service, current service ${esc(svc().label)}" aria-haspopup="listbox" aria-expanded="false" aria-controls="service-options">${icon(state.service)}${esc(svc().label)}${icon('chevron')}</button>
      <div id="service-picker-panel" class="service-picker-panel" hidden>
        <div class="search">${icon('search')}<input id="service-filter" type="text" role="combobox" aria-label="Find a service" aria-autocomplete="list" aria-expanded="false" aria-controls="service-options" autocomplete="off" spellcheck="false" placeholder="Find a service..."></div>
        <div id="service-options" class="service-options" role="listbox" aria-label="Services"></div>
        <p id="service-match-count" class="service-match-count" role="status"></p>
      </div>
    </div></div>`;
  $('#scenario').value = state.scenario;
  $('#scenario option[value="large"]').disabled = !resource().items.length;
  $('#service-title').innerHTML = `<div class="service-title-line">${icon(state.service)}<h1>${svc().name}</h1></div><p>${svc().description}</p>`;
  renderIndex();
  renderInspector();
  const url = new URL(location.href);
  url.searchParams.delete('layout');
  if(state.home) {url.searchParams.set('view','home');url.searchParams.delete('service');}
  else {url.searchParams.delete('view');url.searchParams.set('service',state.service);}
  history.replaceState({mockup:state},'',url);
}
function matchingServices() {
  const search=$('#service-filter').value.trim().toLowerCase();
  return sortedServices.filter(([,service])=>`${service.label} ${service.name}`.toLowerCase().includes(search));
}
function updateActiveService() {
  const options=Array.from($('#service-options').children);
  options.forEach((option,index)=>{
    const active=index===activeServiceIndex;
    option.classList.toggle('active',active);
    option.tabIndex=active?0:-1;
  });
  const active=options[activeServiceIndex];
  if(active) {
    $('#service-filter').setAttribute('aria-activedescendant',active.id);
    active.scrollIntoView({block:'nearest'});
  } else $('#service-filter').removeAttribute('aria-activedescendant');
}
function moveActiveService(key,focusResult=false) {
  const count=matchingServices().length;
  if(!count) return;
  activeServiceIndex=key==='Home'?0:key==='End'?count-1:(activeServiceIndex+(key==='ArrowDown'?1:count-1))%count;
  updateActiveService();
  if(focusResult) $('#service-options').children[activeServiceIndex].focus({preventScroll:true});
}
function renderServiceOptions() {
  const matches=matchingServices();
  activeServiceIndex=matches.length?0:-1;
  $('#service-options').innerHTML=matches.map(([id,service])=>`<button id="service-option-${id}" class="service-option" role="option" aria-selected="${id===state.service}" aria-disabled="${!service.available}" tabindex="-1" data-action="choose-service" data-value="${id}"><span class="service-option-label">${icon(id)}<span><strong>${esc(service.label)}</strong><small>${esc(service.available?service.name:id==='emr'?'Disabled (sample)':'Inspector not in mockup')}</small></span></span>${id===state.service?'<span class="service-option-current">Current</span>':''}</button>`).join('');
  $('#service-match-count').textContent=matches.length?`${matches.length} ${matches.length===1?'service':'services'} · Tab to results, ↑ ↓ to navigate, Enter to select`:'No matching services. Try another name.';
  updateActiveService();
}
function openServicePicker(search='',last=false) {
  servicePickerOpen=true;
  $('#service-picker-panel').hidden=false;
  $('#service-switcher').setAttribute('aria-expanded','true');
  $('#service-filter').setAttribute('aria-expanded','true');
  $('#service-filter').value=search;
  renderServiceOptions();
  if(last) {activeServiceIndex=matchingServices().length-1;updateActiveService();}
  $('#service-filter').focus({preventScroll:true});
}
function closeServicePicker(restoreFocus=false) {
  servicePickerOpen=false;
  $('#service-picker-panel').hidden=true;
  $('#service-switcher').setAttribute('aria-expanded','false');
  $('#service-filter').setAttribute('aria-expanded','false');
  if(restoreFocus) $('#service-switcher').focus({preventScroll:true});
}
function chooseService(id) {
  if(!services[id]) toast(id==='emr'?'EMR is disabled in this sample.':'This service does not have an inspector in the mockup yet.');
  else if(id===state.service) closeServicePicker(true);
  else go(id);
}
function renderHome() {
  $('#service-home').innerHTML = `<div class="directory-heading"><h1>All services</h1><p>Choose a service, then inspect its resources in a dedicated workspace.</p></div>
    <div class="directory-tools">${searchInput('service-search','Find a service',state.serviceFilter)}<label><input id="inspectable-only" type="checkbox" ${state.inspectableOnly?'checked':''}> With an inspector</label></div>
    <p class="directory-note">Service names reflect the repository. Five inspectors are clickable; availability and resource counts are demo examples, not live status.</p>
    <div id="directory-results"></div>`;
  renderDirectory();
}
function renderDirectory() {
  let count=0;
  const groups=Object.entries(directoryGroups).map(([group,entries])=>{
    const matching=entries.filter(([id,name])=>(!state.inspectableOnly||services[id])&&`${group} ${id} ${name} ${services[id]?.name||''}`.toLowerCase().includes(state.serviceFilter.toLowerCase()));
    count+=matching.length;
    if(!matching.length) return '';
    return `<section class="directory-group"><h2>${esc(group)}<span class="count">${matching.length}</span></h2><ul>${matching.map(([id,name])=>{
      const service=services[id];
      const content=`<span class="directory-name">${icon(id)}<strong>${esc(name)}</strong></span><span class="directory-detail">${service?`${service.resources.length} sample ${service.plural.toLowerCase()}`:id==='emr'?'Disabled (sample)':'Inspector not in mockup'}</span>${service?icon('arrow'):''}`;
      return `<li>${service?`<button class="directory-service" data-action="open-service" data-value="${id}">${content}</button>`:`<div class="directory-service unavailable">${content}</div>`}</li>`;
    }).join('')}</ul></section>`;
  }).join('');
  $('#directory-results').innerHTML = `<p class="directory-result-count" role="status">${count} of ${directoryCount} services${state.serviceFilter?' match your search':''}</p>${count?`<div class="directory-grid">${groups}</div>`:statePanel('No matching services','Try a service name such as SQS, storage, or Lambda.',`<button class="button secondary" data-action="clear-service-search">Clear filters</button>`,'search')}`;
}
function renderIndex() {
  const matches = svc().resources.map((r,i)=>({r,i})).filter(({r})=>r.name.toLowerCase().includes(state.resourceFilter.toLowerCase()));
  $('#resource-index').innerHTML = `<div class="index-heading">${svc().plural}<span class="count">${svc().resources.length}</span></div>
    ${searchInput('resource-search','Filter '+svc().plural.toLowerCase(),state.resourceFilter)}
    <div class="resource-list">${matches.map(({r,i})=>`<button class="resource-button ${i===state.resource?'selected':''}" data-action="resource" data-value="${i}" aria-pressed="${i===state.resource}"><strong>${esc(r.name)}</strong><small>${esc(r.type)}</small></button>`).join('') || '<p class="empty-list">No matching resources.</p>'}</div>
    <p class="index-note">Only resources from the sample account are shown.</p>`;
}
function currentArn() {
  const name = state.service==='dynamodb'?'table/'+resource().name:state.service==='events'?'event-bus/'+resource().name:resource().name;
  return arn(state.service,name);
}
function renderInspector() {
  const r=resource();
  const tabs = [['contents',svc().content],['configuration','Configuration'],['connections','Connections'],['activity','Activity']];
  $('#inspector').innerHTML = `<div class="resource-heading">
    ${state.trail.length?`<button class="link-button return-link" data-action="back">${icon('back')}Back to ${esc(state.trail.at(-1).name)}</button>`:''}
    <div class="resource-top"><div><h2>${esc(r.name)}</h2><div class="resource-type">${esc(r.type)} ${tag(r.status,'green')}</div></div>
      <button class="button secondary" data-action="refresh" ${state.scenario==='loading'?'disabled':''}>${icon('refresh')}Refresh snapshot</button></div>
    <div class="arn-row"><code>${esc(currentArn())}</code>${copyButton('arn')}</div>
    </div>
    <div class="tabs" role="tablist" aria-label="Resource sections">${tabs.map(([id,label])=>`<button role="tab" id="tab-${id}" aria-controls="panel" tabindex="${state.tab===id?'0':'-1'}" aria-selected="${state.tab===id}" data-action="tab" data-value="${id}">${label}${id==='connections'?`<span class="tab-count">${r.connections.length}</span>`:''}</button>`).join('')}</div>
    <div class="tab-body" id="panel" role="tabpanel" aria-labelledby="tab-${state.tab}" tabindex="0">${panel()}</div>`;
}
function statePanel(title,text,button='',symbol='info') {
  return `<div class="state-panel"><span class="state-symbol">${icon(symbol)}</span><h3>${title}</h3><p>${text}</p>${button}</div>`;
}
function panel() {
  if(state.scenario==='loading') return `<div role="status" aria-label="Loading resource snapshot"><p>Loading resource snapshot…</p>${Array.from({length:6},()=>'<div class="skeleton"></div>').join('')}<p class="index-note">Preview state. Choose Populated to finish loading.</p></div>`;
  if(state.scenario==='error') return statePanel('Unable to reach MicroStack','The resource snapshot could not be loaded. Check that your local API is running, then try again.',`<button class="button primary" data-action="retry">Retry connection</button><p class="muted" style="margin-top:14px">Simulated error · retry restores sample data.</p>`,'warning');
  if(state.scenario==='disabled') return statePanel('This service is disabled',`${svc().label} is excluded from the instance’s enabled services. An unavailable service is not an empty resource.`,`<button class="button secondary" data-action="retry">Return to populated example</button>`);
  if(state.scenario==='unsupported') return statePanel('Inspector not available','This state represents an enabled service without an implemented resource inspector. Continue using your SDK or CLI; no empty-resource claim is made.',`<button class="button secondary" data-action="retry">Return to supported inspector</button>`);
  let html=state.scenario==='stale'?notice('Snapshot is 18 minutes old. Counts and contents may have changed. <button class="link-button" data-action="retry">Refresh sample snapshot</button>',true):'';
  if(state.tab==='configuration') return html+configuration();
  if(state.tab==='connections') return html+connections();
  if(state.tab==='activity') return html+activity();
  if(state.scenario==='missing') return html+connections(true);
  if(state.scenario==='empty') return html+statePanel(`No ${svc().content.toLowerCase()} in this example`,'The resource is available, but this snapshot has no entries. Create data using your application, SDK, or CLI.',`<button class="button secondary" data-action="retry">Show populated sample</button>`,state.service==='s3'?'folder':'file');
  if(state.service==='sns') return html+subscriptions();
  if(state.service==='events') return html+rules();
  return html+contents();
}
function allItems() {
  const original=resource().items;
  if(state.scenario!=='large' || !original.length) return original;
  return Array.from({length:125},(_,i)=>({...original[i%original.length], name:state.service==='s3'?`batch-${String(i+1).padStart(3,'0')}/export.json`:`${original[i%original.length].name} · sample ${String(i+1).padStart(3,'0')}`}));
}
function visibleItems() {
  const items=allItems();
  if(state.service==='s3' && state.scenario!=='large') {
    const folders=new Map(), entries=[];
    items.forEach((item,i)=>{
      if(!item.name.startsWith(state.prefix)) return;
      const suffix=item.name.slice(state.prefix.length), slash=suffix.indexOf('/');
      if(slash>=0) {
        const name=state.prefix+suffix.slice(0,slash+1);
        folders.set(name,{name,kind:'folder',meta:'Virtual prefix',status:'Prefix',i:-1});
      } else entries.push({...item,i});
    });
    return [...folders.values(),...entries].filter(item=>item.name.toLowerCase().includes(state.filter.toLowerCase()));
  }
  return items.map((item,i)=>({...item,i})).filter(item=>item.name.toLowerCase().includes(state.filter.toLowerCase()) || JSON.stringify(item.body||'').toLowerCase().includes(state.filter.toLowerCase()));
}
function contents() {
  const r=resource(), all=allItems(), matches=visibleItems();
  const counts=state.scenario==='large' && state.service==='sqs'?
    [[String(all.filter(i=>i.status==='Visible').length),'Visible'],['0','Delayed'],[String(all.filter(i=>i.status==='In flight').length),'In flight']]:r.counts;
  const pageCount=Math.max(1,Math.ceil(matches.length/10));
  state.page=Math.min(state.page,pageCount-1);
  const records=matches.slice(state.page*10,state.page*10+10);
  const selected=all[state.item];
  const disclaimer=state.service==='sqs'?'Proposed non-consuming snapshot. Viewing these sample messages does not receive, hide, or delete them. This needs a dedicated admin API.':
    state.service==='dynamodb'?'Loaded sample items only. Filtering here is not a DynamoDB query or a full-table scan. Types are preserved in the item inspector.':
    'Object previews and metadata are sample fixtures. Prefixes group object keys; they are not physical folders.';
  const prefixParts=(state.scenario==='large'?'':state.prefix).split('/').filter(Boolean);
  const prefixNav=state.service==='s3'?`<div class="return-link" aria-label="Object prefix"><button class="link-button" data-action="prefix" data-value="">Bucket root</button>${prefixParts.map((p,i)=>`<span>/</span><button class="link-button" data-action="prefix" data-value="${esc(prefixParts.slice(0,i+1).join('/')+'/')}">${esc(p)}</button>`).join('')}</div>`:'';
  return notice(disclaimer)+
    (counts?`<div class="measurements">${counts.map(([n,label])=>`<div class="measurement"><strong>${n}</strong><span>${label}</span></div>`).join('')}</div>`:'')+prefixNav+
    `<div class="content-tools">${searchInput('item-search','Filter loaded '+svc().content.toLowerCase(),state.filter)}<span class="snapshot-time">${all.length} sample entries loaded</span></div>
    <div class="data-split">
      <div class="records"><div class="list-heading"><span>${svc().content}</span><span>${matches.length} ${state.filter?'matching':'in view'}</span></div>
      ${records.map(item=>`<button class="record-button ${item.i===state.item&&item.kind!=='folder'?'selected':''}" data-action="${item.kind==='folder'?'prefix':'item'}" data-value="${item.kind==='folder'?esc(item.name):item.i}" aria-pressed="${item.i===state.item&&item.kind!=='folder'}">
        <div class="record-title">${item.kind==='folder'?'▸ ':''}${esc(state.service==='s3'&&item.name.startsWith(state.prefix)?item.name.slice(state.prefix.length):item.name)}</div>
        <div class="record-meta"><span>${esc(item.meta)}</span>${tag(item.status,item.status==='Visible'?'green':item.status==='In flight'?'amber':'')}</div></button>`).join('') || '<div class="empty-list">No matching entries.<br>Try a different filter or prefix.</div>'}</div>
      <div class="payload">${selected && matches.some(x=>x.i===state.item)?payload(selected):statePanel('Select an entry','Choose a message, object, or item to inspect its contents.','','file')}</div>
    </div>
    <div class="pagination"><span>${matches.length?state.page*10+1:0}–${Math.min((state.page+1)*10,matches.length)} of ${matches.length} in this view${r.total&&r.total>all.length?` · ${r.total} in resource snapshot`:''}</span>
      <div><button class="button secondary" data-action="page" data-value="-1" ${state.page===0?'disabled':''}>Previous</button><span>${state.page+1} / ${pageCount}</span><button class="button secondary" data-action="page" data-value="1" ${state.page===pageCount-1?'disabled':''}>Next</button></div></div>${contextLinks()}`;
}
function payload(item) {
  return `<div class="payload-head"><strong>${item.kind==='json'?'JSON':['binary','oversized'].includes(item.kind)?'Metadata':'Text'} inspector</strong>${item.body?copyButton('payload','Copy payload'):''}</div>
    ${item.kind==='binary'?statePanel('Preview unavailable','Binary content is not rendered in this prototype. Metadata remains available; no download or external viewer is invoked.','','file'):item.kind==='oversized'?statePanel('Object exceeds preview size','This example intentionally withholds a large payload. A production preview must be bounded; the exact size limit is still a design decision.','','file'):`<pre tabindex="0" aria-label="Selected entry content"><code>${jsonHtml(item.body)}</code></pre>`}
    <dl class="payload-facts">${Object.entries(item.facts||{}).map(([k,v])=>`<div><dt>${esc(k)}</dt><dd>${esc(v)}</dd></div>`).join('')}</dl>`;
}
function targetButton(item,label=item.label) {
  return item.service?`<button class="link-button destination" data-action="connection" data-service="${item.service}" data-resource="${item.resource}">${esc(label)}</button>`:`<span class="destination">${esc(label)}</span>`;
}
function contextLinks() {
  const links=resource().connections;
  if(!links.length) return `<div class="context-links"><h3>Related context</h3><p>No configured connections in this sample. Matching names do not imply a relationship.</p><button class="link-button" data-action="tab" data-value="activity">View ${svc().label} activity for this account →</button></div>`;
  return `<div class="context-links"><h3>Connected resources</h3>${links.slice(0,3).map(l=>`<p class="inline-connection">${icon('link')}${targetButton(l)}<span>${esc(l.relation)}</span></p>`).join('')}<p>Configured connections only · not evidence of delivery.</p></div>`;
}
function configuration() {
  return `<div class="section-heading"><h3>Resource configuration</h3>${tag('Read only')}</div><dl class="metadata">${Object.entries(resource().configuration).map(([k,v])=>`<div><dt>${esc(k)}</dt><dd>${esc(v)}</dd></div>`).join('')}</dl>
    <div class="subsection"><h3>Scope</h3><dl class="metadata"><div><dt>Account</dt><dd><code>${account}</code></dd></div><div><dt>Configured region</dt><dd><code>us-east-1</code></dd></div></dl><p class="index-note">This is instance configuration, not a guarantee of per-region resource isolation.</p></div>`;
}
function connections(forceMissing=false) {
  const links=forceMissing?[link('legacy-orders',null,null,'Configured SQS target',{missing:true})]:resource().connections;
  return notice('These links describe configured subscriptions, targets, or redrive policies. They do not prove that a message matched, was delivered, or was processed.')+
    `<div class="section-heading"><h3>Configured connections</h3><span class="count">${links.length}</span></div>`+
    (links.length?`<div class="connection-list">${links.map(l=>`<div class="connection">${icon(l.service||'link')}<div><strong>${targetButton(l)}</strong><small>${esc(l.relation)}</small>${l.missing?'<small>The destination is not present in this sample account.</small>':l.external?'<small>External destination · no MicroStack inspector.</small>':''}</div>${tag(l.missing?'Not found':l.external?'External':'Configured',l.missing?'amber':'')}</div>`).join('')}</div>`:statePanel('No configured connections','This sample has no supported resource relationships. Connections are never inferred from resource names.','','link'));
}
function subscriptions() {
  const subs=resource().subscriptions;
  return notice('Subscription configuration is shown below. A confirmed subscription is not proof of a successful delivery.')+
    `<div class="section-heading"><h3>Subscriptions</h3><span class="count">${subs.length}</span></div>`+
    (subs.length?`<div class="connection-list">${subs.map(s=>`<div class="connection">${icon(s.protocol==='sqs'?'sqs':'link')}<div><strong>${targetButton(s,s.name)}</strong><small>${esc(s.protocol.toUpperCase())} · <code>${esc(s.endpoint)}</code></small><small>Filter: <code>${esc(s.filter)}</code></small></div>${tag(s.state,s.state==='Confirmed'?'green':'')}</div>`).join('')}</div>`:statePanel('No subscriptions','This topic exists but has no sample subscriptions.','','sns'))+
    `<div class="subsection"><h3>About topic inspection</h3><p class="index-note">SNS is not a message queue. This view inspects subscription configuration, not a retained message inbox.</p></div>`;
}
function rules() {
  const rules=resource().rules;
  if(!rules.length) return statePanel('No rules on this bus','The event bus exists but no sample routing rules are configured.','','events');
  const current=rules[Math.min(state.item,rules.length-1)];
  return notice('Enabled means the rule is configured to participate in routing. Event matching and target delivery are not observed in this mockup.')+
    `<div class="data-split"><div class="records"><div class="list-heading"><span>Rules</span><span>${rules.length} total</span></div>${rules.map((rule,i)=>`<button class="record-button ${rule===current?'selected':''}" data-action="rule" data-value="${i}" aria-pressed="${rule===current}"><div class="record-title">${esc(rule.name)}</div><div class="record-meta"><span>${esc(rule.description)}</span>${tag(rule.status,rule.status==='Enabled'?'green':'')}</div></button>`).join('')}</div>
    <div class="payload"><div class="payload-head"><strong>Event pattern</strong>${tag('Configuration')}</div><pre tabindex="0"><code>${jsonHtml(current.pattern)}</code></pre><div class="payload-facts"><strong style="font-size:.8125rem">Targets</strong>${current.targets.map(t=>`<p style="font-size:.8125rem;margin:6px 0">${targetButton(t)} ${t.missing?tag('Not found','amber'):tag('Configured')}<small style="display:block;margin-top:5px;color:var(--muted)">${esc(t.relation)}</small></p>`).join('')}</div></div></div>`;
}
function activity() {
  const actions={sqs:['GetQueueAttributes','SendMessage','ReceiveMessage','ListQueues'],s3:['GetObject','ListObjectsV2','HeadObject','ListBuckets'],dynamodb:['GetItem','PutItem','Query','ListTables'],sns:['GetTopicAttributes','ListSubscriptionsByTopic','Publish','ListTopics'],events:['ListTargetsByRule','DescribeRule','PutEvents','ListEventBuses']}[state.service];
  return notice(`<strong>Service/account scope, not resource history.</strong> The current log does not carry reliable resource identifiers. These sample calls belong to ${svc().label} / ${account}; none is claimed to target this selected resource.`)+
    `<div class="section-heading"><h3>Recent ${svc().label} API activity</h3>${tag('Sample log')}</div><div class="table-scroll"><table><thead><tr><th scope="col">Time (UTC)</th><th scope="col">Action</th><th scope="col">Account</th><th scope="col">Status</th><th scope="col">Duration</th></tr></thead><tbody>${actions.map((a,i)=>`<tr><td>2026-09-11T10:23:${42-i*5}Z</td><td><code>${a}</code></td><td><code>${account}</code></td><td>${tag(i===2?'400':'200',i===2?'amber':'green')}</td><td>${[3,8,2,1][i]} ms</td></tr>`).join('')}</tbody></table></div><p class="index-note">Metadata only. No request bodies, response bodies, or causal delivery traces are captured.</p>`;
}
function beginNavigation() {
  history.replaceState({mockup:state},'',location.href);
  history.pushState(null,'',location.href);
}
function go(service,index=0,connected=false) {
  clearTimeout(timer);
  beginNavigation();
  if(connected) state.trail.push({service:state.service,resource:state.resource,item:state.item,tab:state.tab,prefix:state.prefix,filter:state.filter,resourceFilter:state.resourceFilter,page:state.page,scenario:state.scenario,name:resource().name});
  else state.trail=[];
  Object.assign(state,{home:false,service,resource:index,item:0,tab:'contents',filter:'',resourceFilter:'',page:0,scenario:'populated',browsing:false,prefix:services[service].resources[index].root||''});
  render();
  $('#main').focus({preventScroll:true});
}
function goHome() {
  clearTimeout(timer);
  beginNavigation();
  Object.assign(state,{home:true,browsing:false,trail:[]});
  render();
  $('#service-search').focus({preventScroll:true});
  window.scrollTo(0,0);
}
function toast(text) {
  clearTimeout(toastTimer);$('#toast').textContent=text;
  toastTimer=setTimeout(()=>$('#toast').textContent='',3500);
}
document.addEventListener('click',async event=>{
  const button=event.target.closest('[data-action]');
  if(!button||button.disabled) return;
  const {action,value}=button.dataset;
  if(action==='home') goHome();
  else if(action==='toggle-service-picker') {if(servicePickerOpen) closeServicePicker(true);else openServicePicker();}
  else if(action==='choose-service') chooseService(value);
  else if(action==='open-service') {go(value);window.scrollTo(0,0);}
  else if(action==='clear-service-search') {state.serviceFilter='';state.inspectableOnly=false;renderHome();$('#service-search').focus();}
  else if(action==='resource') go(state.service,Number(value));
  else if(action==='connection') go(button.dataset.service,Number(button.dataset.resource),true);
  else if(action==='back') {clearTimeout(timer);beginNavigation();Object.assign(state,state.trail.pop(),{browsing:false});render();$('#main').focus({preventScroll:true});}
  else if(action==='tab') {state.tab=value;renderInspector();$(`#tab-${value}`).focus();}
  else if(action==='item'||action==='rule') {state.item=Number(value);renderInspector();$(`[data-action="${action}"][data-value="${value}"]`)?.focus();}
  else if(action==='prefix') {state.prefix=value;state.filter='';state.page=0;state.item=-1;renderInspector();}
  else if(action==='page') {state.page+=Number(value);state.item=-1;renderInspector();}
  else if(action==='show-resources') {state.browsing=!state.browsing;render();if(state.browsing) $('#resource-search').focus();}
  else if(action==='retry') {clearTimeout(timer);state.scenario='populated';render();toast('Sample snapshot restored. No API request was made.');}
  else if(action==='refresh') {
    const previous=state.scenario==='loading'?'populated':state.scenario;
    state.scenario='loading';render();
    timer=setTimeout(()=>{state.scenario=previous==='stale'?'populated':previous;render();toast('Sample snapshot refreshed. No live API calls.');},650);
  } else if(action==='copy') {
    const text=button.dataset.type==='arn'?currentArn():allItems()[state.item]?.body;
    try {await navigator.clipboard.writeText(typeof text==='string'?text:JSON.stringify(text,null,2));toast(button.dataset.type==='arn'?'ARN copied':'Sample payload copied');}
    catch {toast('Clipboard access is unavailable. Select the visible text to copy it manually.');}
  }
});
document.addEventListener('input',event=>{
  if(event.target.id==='service-filter') renderServiceOptions();
  if(event.target.id==='service-search') {state.serviceFilter=event.target.value;renderDirectory();}
  if(event.target.id==='resource-search') {
    state.resourceFilter=event.target.value;
    const caret=event.target.selectionStart;renderIndex();$('#resource-search').focus();
    if(caret!==null && $('#resource-search').type!=='search') $('#resource-search').setSelectionRange(caret,caret);
  }
  if(event.target.id==='item-search') {
    state.filter=event.target.value;state.page=0;renderInspector();$('#item-search').focus();
  }
});
document.addEventListener('change',event=>{
  if(event.target.id==='inspectable-only') {state.inspectableOnly=event.target.checked;renderDirectory();}
  if(event.target.id==='scenario') {clearTimeout(timer);state.scenario=event.target.value;state.page=0;state.filter='';state.item=0;render();}
});
document.addEventListener('keydown',event=>{
  if(event.isComposing) return;
  if(servicePickerOpen&&event.key==='Escape') {event.preventDefault();closeServicePicker(true);return;}
  if(event.target.id==='service-switcher') {
    if(['ArrowDown','ArrowUp'].includes(event.key)) {event.preventDefault();openServicePicker('',event.key==='ArrowUp');return;}
    if(event.key.length===1&&event.key!==' '&&!event.ctrlKey&&!event.altKey&&!event.metaKey) {event.preventDefault();openServicePicker(event.key);return;}
  }
  if(event.target.id==='service-filter') {
    const matches=matchingServices();
    if(['ArrowDown','ArrowUp'].includes(event.key)) {
      event.preventDefault();
      moveActiveService(event.key);
    } else if(event.key==='Enter') {
      event.preventDefault();
      if(matches[activeServiceIndex]) chooseService(matches[activeServiceIndex][0]);
    }
    return;
  }
  if(event.target.matches('#service-options [role="option"]')&&['ArrowDown','ArrowUp','Home','End'].includes(event.key)) {
    event.preventDefault();
    activeServiceIndex=matchingServices().findIndex(([id])=>id===event.target.dataset.value);
    moveActiveService(event.key,true);
    return;
  }
  if(event.target.matches('[role="tab"]') && ['ArrowRight','ArrowLeft','Home','End'].includes(event.key)) {
    event.preventDefault();
    const tabs=['contents','configuration','connections','activity'], current=tabs.indexOf(state.tab);
    state.tab=tabs[event.key==='Home'?0:event.key==='End'?3:(current+(event.key==='ArrowRight'?1:3))%4];
    renderInspector();$(`#tab-${state.tab}`).focus();
  }
});
document.addEventListener('pointerdown',event=>{
  if(servicePickerOpen&&!event.target.closest('.service-picker')) closeServicePicker();
});
document.addEventListener('focusout',event=>{
  if(servicePickerOpen&&!$('.service-picker').contains(event.relatedTarget)) closeServicePicker();
});
window.addEventListener('popstate',event=>{
  if(!event.state?.mockup) return;
  clearTimeout(timer);
  Object.assign(state,event.state.mockup);
  render();
  $('#main').focus({preventScroll:true});
});
render();
