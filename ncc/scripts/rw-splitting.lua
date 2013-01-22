--[[ $%BEGINLICENSE%$
 Copyright (c) 2007, 2009, Oracle and/or its affiliates. All rights reserved.

 This program is free software; you can redistribute it and/or
 modify it under the terms of the GNU General Public License as
 published by the Free Software Foundation; version 2 of the
 License.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 02110-1301  USA

 $%ENDLICENSE%$ --]]

---
-- a flexible statement based load balancer with connection pooling
--
-- * build a connection pool of min_idle_connections for each backend and maintain
--   its size
-- * 
-- 
-- 
require('Memcached')
require('CRC32')



local commands    = require("proxy.commands")
local tokenizer   = require("proxy.tokenizer")
local lb          = require("proxy.balance")
local auto_config = require("proxy.auto-config")
local parser          = require("proxy.parser")


--- config
--
-- connection pool
if not proxy.global.config.rwsplit then
	proxy.global.config.rwsplit = {
		min_idle_connections = 1,
		max_idle_connections = 2,

		is_debug = true  
	}
end

---
-- read/write splitting sends all non-transactional SELECTs to the slaves
--
-- is_in_transaction tracks the state of the transactions
local is_in_transaction       = false

-- if this was a SELECT SQL_CALC_FOUND_ROWS ... stay on the same connections
local is_in_select_calc_found_rows = false

local min_idle_connections = 4
local max_idle_connections = 8

function connect_server()
  
  local least_idle_conns_ndx = 0
  local least_idle_conns = 0

  for i = 1, #proxy.global.backends do
    local s = proxy.global.backends[i]

    if s.state ~= proxy.BACKEND_STATE_DOWN then
      -- try to connect to each backend once at least
      if s.idling_connections == 0 then
        proxy.connection.backend_ndx = i
        return
      end

      -- try to open at least min_idle_connections
      if least_idle_conns_ndx == 0 or
         ( s.pool.users[""].cur_idle_connections < min_idle_connections and
           s.pool.users[""].cur_idle_connections < least_idle_conns ) then
        least_idle_conns_ndx = i
        least_idle_conns = s.pool.users[""].cur_idle_connections
      end
    end
  end

  if least_idle_conns_ndx > 0 then
    proxy.connection.backend_ndx = least_idle_conns_ndx
  end

  if proxy.connection.backend_ndx > 0 and
     proxy.servers[proxy.connection.backend_ndx].idling_connections >= min_idle_connections then
    -- we have idling connections in the pool, that's good enough

    return proxy.PROXY_IGNORE_RESULT
  end

  -- open a new connection
end


function read_auth_result( auth )
	if is_debug then
		print("[read_auth_result] " .. proxy.connection.client.src.name)
	end
	if auth.packet:byte() == proxy.MYSQLD_PACKET_OK then
		-- auth was fine, disconnect from the server
		proxy.connection.backend_ndx = 0
	elseif auth.packet:byte() == proxy.MYSQLD_PACKET_EOF then
		-- we received either a 
		-- 
		-- * MYSQLD_PACKET_ERR and the auth failed or
		-- * MYSQLD_PACKET_EOF which means a OLD PASSWORD (4.0) was sent
		print("(read_auth_result) ... not ok yet");
	elseif auth.packet:byte() == proxy.MYSQLD_PACKET_ERR then
		-- auth failed
	end
end


--- 
-- read/write splitting
function read_query( packet )
	local is_debug = proxy.global.config.rwsplit.is_debug
	local cmd      = commands.parse(packet)
	local c        = proxy.connection.client

	local r = auto_config.handle(cmd)
	if r then return r end

	local tokens
	local norm_query
        local stmt

	-- looks like we have to forward this statement to a backend
	if is_debug then
		print("[read_query] " .. proxy.connection.client.src.name)
		print("  current backend   = " .. proxy.connection.backend_ndx)
		print("  client default db = " .. c.default_db)
		print("  client username   = " .. c.username)
		if cmd.type == proxy.COM_QUERY then 
			print("  query             = "        .. cmd.query)
		end
	end

	if cmd.type == proxy.COM_QUIT then
		-- don't send COM_QUIT to the backend. We manage the connection
		-- in all aspects.
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK,
		}
	
		if is_debug then
			print("  (QUIT) current backend   = " .. proxy.connection.backend_ndx)
		end

		return proxy.PROXY_SEND_RESULT
	end

	proxy.queries:append(1, packet, { resultset_is_needed = true })
        if cmd.type == proxy.COM_QUERY then
          tokens     = tokens or assert(tokenizer.tokenize(cmd.query))
	  stmt = tokenizer.first_stmt_token(tokens)
        end  

-- read/write splitting 
	--
	-- send all non-transactional SELECTs to a slave
	if not is_in_transaction and
	   cmd.type == proxy.COM_QUERY then
		if stmt.token_name == "TK_SQL_SELECT" then
			is_in_select_calc_found_rows = false
			local is_insert_id = false

			for i = 1, #tokens do
				local token = tokens[i]
				-- SQL_CALC_FOUND_ROWS + FOUND_ROWS() have to be executed 
                    		-- on the same connection
				-- print("token: " .. token.token_name)
				-- print("  val: " .. token.text)
				
				if not is_in_select_calc_found_rows and token.token_name == "TK_SQL_SQL_CALC_FOUND_ROWS" then
					is_in_select_calc_found_rows = true
				elseif not is_insert_id and token.token_name == "TK_LITERAL" then
					local utext = token.text:upper()

					if utext == "LAST_INSERT_ID" or
					   utext == "@@INSERT_ID" then
						is_insert_id = true
					end
				end

				-- we found the two special token, we can't find more
				if is_insert_id and is_in_select_calc_found_rows then
					break
				end
			end

			-- if we ask for the last-insert-id we have to ask it on the original 
			-- connection
			if not is_insert_id then
				local backend_ndx = lb.idle_ro()

				if backend_ndx > 0 then
					proxy.connection.backend_ndx = backend_ndx
				end
			else
				print("   found a SELECT LAST_INSERT_ID(), staying on the same backend")
			end
		end
	end

	-- no backend selected yet, pick a master
	if proxy.connection.backend_ndx == 0 then
		-- we don't have a backend right now
		-- 
		-- let's pick a master as a good default
		--
		proxy.connection.backend_ndx = lb.idle_failsafe_rw()
	end

	-- by now we should have a backend
	--
	-- in case the master is down, we have to close the client connections
	-- otherwise we can go on
	if proxy.connection.backend_ndx == 0 then
		return proxy.PROXY_SEND_QUERY
	end

	local s = proxy.connection.server

	-- if client and server db don't match, adjust the server-side 
	--
	-- skip it if we send a INIT_DB anyway
	if cmd.type ~= proxy.COM_INIT_DB and 
	   c.default_db and c.default_db ~= s.default_db then
		print("    server default db: " .. s.default_db)
		print("    client default db: " .. c.default_db)
		print("    syncronizing")
		proxy.queries:prepend(2, string.char(proxy.COM_INIT_DB) .. c.default_db, { resultset_is_needed = true })
	end

	-- send to master
 
      
	if is_debug then
		if proxy.connection.backend_ndx > 0 then
			local b = proxy.global.backends[proxy.connection.backend_ndx]
			print("  sending to backend : " .. b.dst.name);
			print("    is_slave         : " .. tostring(b.type == proxy.BACKEND_TYPE_RO));
			print("    server default db: " .. s.default_db)
			print("    server username  : " .. s.username)
		end
		print("    in_trans        : " .. tostring(is_in_transaction))
		print("    in_calc_found   : " .. tostring(is_in_select_calc_found_rows))
		print("    COM_QUERY       : " .. tostring(cmd.type == proxy.COM_QUERY))
	end
        local tbls  = {} 
        tbls = parser.get_tables(tokens)
       local memcache= Memcached.Connect({'127.0.0.1' , 11211}) 
      -- while ( memcache:add("_lock","lock",1) == nil ) do 
      --      print("lock")
     --  end
   
        if  cmd.type == proxy.COM_QUERY  and stmt.token_name ~= "TK_SQL_SELECT" then
            print ("injection GTID")
            proxy.queries:append(3, string.char(proxy.COM_QUERY)  .. "SET binlog_format =\"STATEMENT\"", { resultset_is_needed = true } )
            for tbl,v in pairs(tbls) do 
  print ("INSERT into  mysql.TBLGTID select \"" .. CRC32.Hash(tbl) .. "\", 0 , memc_set(concat(\"" .. CRC32.Hash(tbl) .. "\",@@server_id),0)  on duplicate key update gtid=gtid+1, memres=memc_set(concat(\"" ..  CRC32.Hash(tbl) .. "\",@@server_id),gtid+1)") 
                              
proxy.queries:append(4, string.char(proxy.COM_QUERY)  .. "INSERT into  mysql.TBLGTID select \"" .. CRC32.Hash(tbl) .. "\", 0 , memc_set(concat(\"" .. CRC32.Hash(tbl) .. "\",@@server_id),0)  on duplicate key update gtid=gtid+1, memres=memc_set(concat(\"" ..  CRC32.Hash(tbl) .. "\",@@server_id),gtid+1)",{ resultset_is_needed = true } )
            end 
            proxy.queries:append(5, string.char(proxy.COM_QUERY)  .. "SET binlog_format =\"MIXED\"", { resultset_is_needed = true } )
        elseif (proxy.global.backends[proxy.connection.backend_ndx].type == proxy.BACKEND_TYPE_RO ) then
            
        
            local slaveGTID=0  
            local masterGTID=0  
            for tbl,v in pairs(tbls) do
                
             masterGTID=memcache:get(CRC32.Hash(tbl) .. "5010")   
	                
	    slaveGTID=memcache:get(CRC32.Hash(tbl) .. (5009 + proxy.connection.backend_ndx))
           
            if is_debug then
		 print(" master GTID : " ..  masterGTID .." for table : " .. tbl)   
             print(" slave GTID : " ..  slaveGTID .." for table : " .. tbl)   
            end  
            if  masterGTID ~=slaveGTID then 
                proxy.connection.backend_ndx = lb.idle_failsafe_rw()
                print ("fail back to master replication delay....")
                break
             end 
            end 

      end 
      -- memcache:delete( "_lock")            
       memcache:disconnect_all()

              
       return proxy.PROXY_SEND_QUERY
end

---
-- as long as we are in a transaction keep the connection
-- otherwise release it so another client can use it
function read_query_result( inj ) 
	local is_debug = proxy.global.config.rwsplit.is_debug
	local res      = assert(inj.resultset)
  	local flags    = res.flags

	if inj.id ~= 1 then
		-- ignore the result of the USE <default_db>
		-- the DB might not exist on the backend, what do do ?
		--
		if inj.id == 2 then
			-- the injected INIT_DB failed as the slave doesn't have this DB
			-- or doesn't have permissions to read from it
			if res.query_status == proxy.MYSQLD_PACKET_ERR then
				proxy.queries:reset()

				proxy.response = {
					type = proxy.MYSQLD_PACKET_ERR,
					errmsg = "can't change DB ".. proxy.connection.client.default_db ..
						" to on slave " .. proxy.global.backends[proxy.connection.backend_ndx].dst.name
				}

				return proxy.PROXY_SEND_RESULT
			end
		end
                if (inj.id == 4) then
                    if res.query_status == proxy.MYSQLD_PACKET_ERR then
                     proxy.response = {
					type = proxy.MYSQLD_PACKET_ERR,
					errmsg = "can't inject GTID ".. proxy.connection.client.default_db ..
						" to on slave " .. proxy.global.backends[proxy.connection.backend_ndx].dst.name
				}

				return proxy.PROXY_SEND_RESULT
                    end
                end
		return proxy.PROXY_IGNORE_RESULT
	end

	is_in_transaction = flags.in_trans
	local have_last_insert_id = (res.insert_id and (res.insert_id > 0))

	if not is_in_transaction and 
	   not is_in_select_calc_found_rows and
	   not have_last_insert_id then
		-- release the backend
		proxy.connection.backend_ndx = 0
	elseif is_debug then
		print("(read_query_result) staying on the same backend")
		print("    in_trans        : " .. tostring(is_in_transaction))
		print("    in_calc_found   : " .. tostring(is_in_select_calc_found_rows))
		print("    have_insert_id  : " .. tostring(have_last_insert_id))
	end
end

function disconnect_client()
  if proxy.connection.backend_ndx == 0 then
    -- currently we don't have a server backend assigned
    --
    -- pick a server which has too many idling connections and close one
    for i = 1, #proxy.global.backends do
      local s = proxy.global.backends[i]
      if s.state ~= proxy.BACKEND_STATE_DOWN and 
	s.pool.users[proxy.connection.client.username].cur_idle_connections > max_idle_connections then
        -- try to disconnect a backend
        proxy.connection.backend_ndx = i
        return
      end
    end
  end
end

