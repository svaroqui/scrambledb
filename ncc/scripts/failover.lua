require('md5')
require('Memcached')
-- local memcache = Memcached.Connect()

memcache= Memcached.Connect('127.0.0.1',11211)
function connect_server()
i = memcache:get('activeConnection')
if i == nil then
    proxy.connection.backend_ndx=1
else
    proxy.connection.backend_ndx = i   
end    
if proxy.global.backends[proxy.connection.backend_ndx].state == proxy.BACKEND_STATE_DOWN then
for i = 1, #proxy.global.backends do
local s = proxy.global.backends[i]
if s.state ~= proxy.BACKEND_STATE_DOWN then
proxy.connection.backend_ndx = i
memcache:set('activeConnection',i,0)
return
end
end
end 
end

function read_query(packet)
 i = memcache:get('activeConnection')
 if i == nil then
    proxy.connection.backend_ndx=1
 else
    proxy.connection.backend_ndx = i   
 end    
 if proxy.global.backends[proxy.connection.backend_ndx].state == proxy.BACKEND_STATE_DOWN then
  for i = 1, #proxy.global.backends do
   local s = proxy.global.backends[i]
   if s.state ~= proxy.BACKEND_STATE_DOWN then
    proxy.connection.backend_ndx = i
    memcache:set('activeConnection',i,0)
    return
   end
  end
 end
end
