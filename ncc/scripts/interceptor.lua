require('Memcached')
-- require('md5')
-- local memcache = Memcached.Connect()

local sql = ""

memcache= Memcached.Connect({'127.0.0.1' , 11211})



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
  if string.byte(packet) == proxy.COM_QUERY then
  --  proxy.queries:append(1, packet, {resultset_is_needed = true} )
    if todo(packet:sub(2)) == nil then
           return proxy.PROXY_SEND_QUERY
    else
	proxy.queries:append(2, string.char(proxy.COM_QUERY) .. "SELECT DATABASE()", {resultset_is_needed = true} )
	sql= packet:sub(2)
	return proxy.PROXY_SEND_QUERY
    end
  end
end


function read_query_result(inj)
    database=""     
    if inj.id == 2 then
                for row in inj.resultset.rows do
                        database= row[1]
                end
	if  database == nil then 
		database=""
	end 
	sql = string.gsub(sql,"\'", "\\'")      
	sql = "SELECT gman_do('cluster_cmd','{command: {action:\"sql\" ,group:\"all\", type:\"all\", database: \"" .. database .. "\", query: \"" .. sql  ..  "\"}}')"
  		proxy.queries:append(3 ,string.char(proxy.COM_QUERY) .. sql)
              return proxy.PROXY_IGNORE_RESULT
    end
end


