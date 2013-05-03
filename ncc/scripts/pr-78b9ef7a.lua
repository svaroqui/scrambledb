require('Memcached')
-- require('md5')
-- local memcache = Memcached.Connect()

local sql = ""

-- memcache= Memcached.Connect({'127.0.0.1' , 11211})

local cluster="ar-3b555bd6"
local backend_id_server = { 5013,5010,5012,5011}
 local replication_dsn = {{ port = 5011, ip = '127.0.0.1', user = 'skysql', password  = 'skyvodka'}}

local memcache_master="127.0.0.1"
local memcache_port = 11211
if not proxy.global.config.rwsplit then
proxy.global.config.rwsplit = {
       com_queries_ro   = 0,
       com_queries_rw   = 0,
	is_debug = true 
}
end
-- insert here --

-- function connect_server()
-- end 

function todo(SQL)
  if string.find(string.upper(SQL),"CREATE" ) == nil and  
     string.find(string.upper(SQL),"DROP" ) == nil and
     string.find(string.upper(SQL),"ALTER TABLE" ) == nil then 

    return nil 
  end
  if not (string.find(string.upper(SQL),"SHOW" ) == nil) then
	 return nil 
  end 
  return 1 	
end

function read_query( packet )
   database=""     
     
  if string.byte(packet) == proxy.COM_QUERY then
    if todo(packet:sub(2)) == nil then
           proxy.queries:append(1, packet, {resultset_is_needed = true} )
           return proxy.PROXY_SEND_QUERY
  
    else
	
        database= proxy.connection.client.default_db
   	if  database == nil then 
		database=""
	end 
   	sql= packet:sub(2)
        sql = string.gsub(sql,"\'", "\\'")      
	sql = "SELECT gman_do('cluster_cmd','{level:\"services\",command: {action:\"sql\" ,group:\"".. cluster .."\", type:\"all\", database: \"" .. database .. "\", query: \"" .. sql  ..  "\"}}')"
  		proxy.queries:append(1 ,string.char(proxy.COM_QUERY) .. sql,  {resultset_is_needed = true})
             
	return proxy.PROXY_SEND_QUERY
      
    end
  end
end


function read_query_result(inj)

end


