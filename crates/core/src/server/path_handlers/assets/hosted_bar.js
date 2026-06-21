(function(){
  var acct=document.getElementById('fnacct');
  var pop=document.getElementById('fnpop');
  var ok=document.getElementById('fnok');
  function setOk(m){ok.textContent=m;setTimeout(function(){if(ok.textContent===m)ok.textContent='';},2500);}
  document.getElementById('fnacctbtn').addEventListener('click',function(e){e.stopPropagation();pop.classList.toggle('open');});
  document.addEventListener('click',function(e){if(!acct.contains(e.target))pop.classList.remove('open');});
  document.getElementById('fncopy').addEventListener('click',function(){
    var t=(typeof __freenet_user_token!=='undefined')?__freenet_user_token:null;
    if(!t){setOk('No key on this connection');return;}
    if(navigator.clipboard&&navigator.clipboard.writeText){navigator.clipboard.writeText(t).then(function(){setOk('Copied to clipboard');},function(){window.prompt('Copy your access key:',t);});}
    else{window.prompt('Copy your access key:',t);}
  });
  document.getElementById('fnrestore').addEventListener('click',function(){
    var v=window.prompt('Paste your saved access key to restore access to your data:');
    if(!v){return;} v=v.trim(); if(!v){return;}
    try{localStorage.setItem('__freenet_user_token__',v);location.reload();}catch(e){setOk('Storage unavailable');}
  });
  document.getElementById('fnexport').addEventListener('click',function(){
    alert('Export your delegate data to your own Freenet peer.\n\nThis downloads an encrypted bundle you can import on your own node with: freenet secrets import\n\n(Coming soon.)');
  });
})();