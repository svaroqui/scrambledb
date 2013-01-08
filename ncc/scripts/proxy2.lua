--[[ $%BEGINLICENSE%$
 Copyright (c) 2011 SkySQL  All rights reserved.

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

require('Memcached')
require('CRC32')


local commands    = require("proxy.commands")
local tokenizer   = require("proxy.tokenizer")
local lb          = require("proxy.balance")
local auto_config = require("proxy.auto-config")


local backend_id_server = { 5010,5012,5011}
local memcache_master="127.0.0.1"
local memcache_port = 11211
-- insert here --	    

-- connection pool
if not proxy.global.config.rwsplit then
	proxy.global.config.rwsplit = {
		min_idle_connections = 5,
		max_idle_connections = 20,

		is_debug = true  
	}
end

local is_in_transaction       = false
local is_in_select_calc_found_rows = false

-- get a connection to a backend
-- if not enough in pool, create new connections

function tablename_expand(tblname, db_name)
	if not db_name then db_name = proxy.connection.client.default_db end
	if db_name then
		tblname = db_name .. "." .. tblname
	end

	return tblname
end

---
-- extract the table-names from tokenized SQL token stream
--
-- @see proxy.tokenize
function get_sql_tables(tokens)
	local sql_stmt = nil
	local in_tablelist = false 
	local next_is_tblname = false
	local in_braces = 0
	local db_name = nil

	local tables = {}

	for i = 1, #tokens do
		local token = tokens[i]

          --      print(i .. " token: " .. token["token_name"] .. token.text) 
		if token["token_name"] == "TK_COMMENT" or
		   token["token_name"] == "TK_UNKNOWN" then

		elseif not sql_stmt then
			-- try to get the SQL stmt
			sql_stmt = token.text:upper()
			-- print(i .. " sql-stmt: " .. sql_stmt)
			
			if sql_stmt == "UPDATE" or
			   sql_stmt == "INSERT" then
				in_tablelist = true
			end
		elseif sql_stmt == "SELECT" or 
		       sql_stmt == "DELETE" then
			-- we have sql_stmt token already, try to get the table names
			--
			-- SELECT ... FROM tbl AS alias, ... 
			-- DELETE FROM ... 
		
			if in_tablelist then
				if token["token_name"] == "TK_COMMA" then
					next_is_tblname = true
				elseif in_braces > 0 then
					-- ignore sub-queries
				elseif token["token_name"] == "TK_SQL_AS" then
					-- FROM tbl AS alias ...
					next_is_tblname = false
				elseif token["token_name"] == "TK_SQL_JOIN" then
					-- FROM tbl JOIN tbl ...
					next_is_tblname = true
				elseif token["token_name"] == "TK_SQL_LEFT" or
				       token["token_name"] == "TK_SQL_RIGHT" or
				       token["token_name"] == "TK_SQL_OUTER" or
				       token["token_name"] == "TK_SQL_USING" or
				       token["token_name"] == "TK_SQL_ON" or
				       token["token_name"] == "TK_SQL_AND" or
				       token["token_name"] == "TK_SQL_OR" then
					-- ignore me
				elseif token["token_name"] == "TK_LITERAL" and next_is_tblname then
					-- we have to handle <tbl> and <db>.<tbl>
					if not db_name and tokens[i + 1] and tokens[i + 1].token_name == "TK_DOT" then
						db_name = token.text
					else
						-- print ("Found table_name : " .. token.text) 
                                                tables[tablename_expand(token.text, db_name)] = (sql_stmt == "SELECT" and "read" or "write")

						db_name = nil
					end

					next_is_tblname = false
				elseif token["token_name"] == "TK_OBRACE" then
					in_braces = in_braces + 1
				elseif token["token_name"] == "TK_CBRACE" then
					in_braces = in_braces - 1
				elseif token["token_name"] == "TK_SQL_WHERE" or
				       token["token_name"] == "TK_SQL_GROUP" or
				       token["token_name"] == "TK_SQL_ORDER" or
				       token["token_name"] == "TK_SQL_LIMIT" or
				       token["token_name"] == "TK_CBRACE" then
					in_tablelist = false
				elseif token["token_name"] == "TK_DOT" then
					-- FROM db.tbl
					next_is_tblname = true
				else
					print("(parser) unknown, found token: " .. token["token_name"] .. " -> " .. token.text)
					-- in_tablelist = false
				end
			elseif token["token_name"] == "TK_SQL_FROM" then
				in_tablelist = true
				next_is_tblname = true
			end
			
			-- print(i .. " in-from: " .. (in_from and "true" or "false"))
			-- print(i .. " next-is-tblname: " .. (next_is_tblname and "true" or "false"))
		elseif sql_stmt == "CREATE" or 
		       sql_stmt == "DROP" or 
		       sql_stmt == "ALTER" or 
		       sql_stmt == "RENAME" then
			-- CREATE TABLE <tblname>
			if not ddl_type then
				ddl_type = token.text:upper()
				in_tablelist = true
			elseif ddl_type == "TABLE" or ddl_type == "VIEW" then
				if in_tablelist and  token["token_name"] == "TK_LITERAL" then
                                    if not db_name and tokens[i + 1] and tokens[i + 1].token_name == "TK_DOT" then
					db_name = token.text
				    else
                                        -- print ("Found table_name : " .. token.text) 
                                       	tables[tablename_expand(token.text, db_name)] = (sql_stmt == "SELECT" and "read" or "write")
                                        in_tablelist = false
                                    end       
				
				end
			end
		elseif sql_stmt == "INSERT" then
			-- INSERT INTO ...

                        -- print ( "We are in insert into and token :" .. token["token_name"])      
                       if ( token["token_name"] == "TK_SQL_INSERT") then  
                            in_tablelist = true
                        elseif  in_tablelist then
				if token["token_name"] == "TK_LITERAL" and in_tablelist  then
                                    if not db_name and tokens[i + 1] and tokens[i + 1].token_name == "TK_DOT" then
						db_name = token.text
				    else
                                     --   print ("Found table_name : " .. token.text) 
					tables[tablename_expand(token.text, db_name)] = (sql_stmt == "SELECT" and "read" or "write")
                                        in_tablelist = false
                                    end    
				elseif token["token_name"] == "TK_SQL_INTO" then
                                      in_tablelist = true
				end 
			end
		elseif sql_stmt == "UPDATE" then
			-- UPDATE <tbl> SET ..
                        if ( token["token_name"] == "TK_SQL_UPDATE") then  
                            in_tablelist = true
                        end        
			if in_tablelist then
				if token["token_name"] == "TK_LITERAL" then
                                   if not db_name and tokens[i + 1] and tokens[i + 1].token_name == "TK_DOT" then
						db_name = token.text
				    else
                                     --   print ("Found table_name : " .. token.text) 
					tables[tablename_expand(token.text, db_name)] = (sql_stmt == "SELECT" and "read" or "write")
                                        in_tablelist = false
    
                                    end    
                                elseif token["token_name"] == "TK_SQL_SET" then
					in_tablelist = false

					break
				end
			end
		end
	end

	return tables
end



function connect_server() 
	local is_debug = proxy.global.config.rwsplit.is_debug
	-- make sure that we connect to each backend at least ones to 
	-- keep the connections to the servers alive
	--
	-- on read_query we can switch the backends again to another backend

	if is_debug then
		
		print("[connect_server] " .. proxy.connection.client.src.name)
	end

	local rw_ndx = 0

	-- init all backends 
	for i = 1, #proxy.global.backends do
		local s        = proxy.global.backends[i]
		local pool     = s.pool -- we don't have a username yet, try to find a connections which is idling
		local cur_idle = pool.users[""].cur_idle_connections

		pool.min_idle_connections = proxy.global.config.rwsplit.min_idle_connections
		pool.max_idle_connections = proxy.global.config.rwsplit.max_idle_connections
		
		if is_debug then
			print("  [".. i .."].connected_clients = " .. s.connected_clients)
			print("  [".. i .."].pool.cur_idle     = " .. cur_idle)
			print("  [".. i .."].pool.max_idle     = " .. pool.max_idle_connections)
			print("  [".. i .."].pool.min_idle     = " .. pool.min_idle_connections)
			print("  [".. i .."].type = " .. s.type)
			print("  [".. i .."].state = " .. s.state)
		end

		-- prefer connections to the master 
		if s.type == proxy.BACKEND_TYPE_RW and
		   s.state ~= proxy.BACKEND_STATE_DOWN and
		   cur_idle < pool.min_idle_connections then
			proxy.connection.backend_ndx = i
			break
		elseif s.type == proxy.BACKEND_TYPE_RO and
		       s.state ~= proxy.BACKEND_STATE_DOWN 
		       and cur_idle < pool.min_idle_connections 
                        then
			proxy.connection.backend_ndx = i
			break
		elseif s.type == proxy.BACKEND_TYPE_RW and
		       s.state ~= proxy.BACKEND_STATE_DOWN and
		       rw_ndx == 0 then
                        rw_ndx = i
		end
	end

	if proxy.connection.backend_ndx == 0 then
		if is_debug then
			print("  [" .. rw_ndx .. "] taking master as default")
		end
		proxy.connection.backend_ndx = rw_ndx
	end

	-- pick a random backend
	--
	-- we someone have to skip DOWN backends

	-- ok, did we got a backend ?

	if proxy.connection.server then 
		if is_debug then
			print("  using pooled connection from: " .. proxy.connection.backend_ndx)
		end

		-- stay with it
		return proxy.PROXY_IGNORE_RESULT
	end

	if is_debug then
		print("  [" .. proxy.connection.backend_ndx .. "] idle-conns below min-idle")
	end

	-- open a new connection 
end

-- add  authed connection into the connection pool
-- @param auth the context information for the auth

function read_auth_result( auth )
	if is_debug then
		print("[read_auth_result] " .. proxy.connection.client.src.name)
	end
	if auth.packet:byte() == proxy.MYSQLD_PACKET_OK then
		proxy.connection.backend_ndx = 0
	elseif auth.packet:byte() == proxy.MYSQLD_PACKET_EOF then
		print("(read_auth_result) ... not ok yet");
	elseif auth.packet:byte() == proxy.MYSQLD_PACKET_ERR then
		-- auth failed
	end
end


function read_query( packet )
	local is_debug = proxy.global.config.rwsplit.is_debug
	local cmd      = commands.parse(packet)
	local c        = proxy.connection.client

	local r = auto_config.handle(cmd)
	if r then return r end

	local tokens
	local norm_query
        local stmt

	

	if cmd.type == proxy.COM_QUIT then
		-- don't send COM_QUIT  We keep the connection open
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK,
		}
	
		if is_debug then
			print("  (QUIT) current backend   = " .. proxy.connection.backend_ndx)
		end

		return proxy.PROXY_SEND_RESULT
	end

	
        if cmd.type == proxy.COM_QUERY then
          if is_debug then
		print("[read_query] " .. proxy.connection.client.src.name)
		print("  current backend   = " .. proxy.connection.backend_ndx)
		print("  client default db = " .. c.default_db)
		print("  client username   = " .. c.username)
		if cmd.type == proxy.COM_QUERY then 
			print("  query             = "        .. cmd.query)
		end
	  end  
          tokens     = tokens or assert(tokenizer.tokenize(cmd.query))
	  stmt = tokenizer.first_stmt_token(tokens)
        end  

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
				elseif not is_insert_id and token.token_name == "TK_FUNCTION" then
				local utext = token.text:upper()

					if utext == "LAST_INSERT_ID" or
					   utext == "@@INSERT_ID" then
						is_insert_id = true
					end
                                end

				-- Found special statement 
				if is_insert_id and is_in_select_calc_found_rows then
					break
				end
			end

			--  last-insert-id from on the original 
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
                proxy.queries:append(1, packet, { resultset_is_needed = true })
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
--       while ( memcache:add("_lock","lock",1) == nil ) do 
--            print("lock")
--       end
   
        if  cmd.type == proxy.COM_QUERY  and stmt.token_name ~= "TK_SQL_SELECT" then
           tokens2     = tokens or assert(tokenizer.tokenize(cmd.query))
          print ("    Injection GTID")
            tbls = get_sql_tables(tokens2)
	    proxy.queries:append(3, string.char(proxy.COM_QUERY)  .. "SET binlog_format =\"STATEMENT\"", { resultset_is_needed = true } )
            
             local i=4
            for tbl,v in pairs(tbls) do 
                print (tbl)
                print ("INSERT into  mysql.TBLGTID select \"" .. CRC32.Hash(tbl) .. "\", 0 , memc_set(concat(\"" .. CRC32.Hash(tbl) .. "\",@@server_id),0)  on duplicate key update gtid=gtid+1, memres=memc_set(concat(\"" ..  CRC32.Hash(tbl) .. "\",@@server_id),gtid+1)") 
                proxy.queries:append(i, string.char(proxy.COM_QUERY)  .. "INSERT into  mysql.TBLGTID select \"" .. CRC32.Hash(tbl) .. "\", 0 , memc_set(concat(\"" .. CRC32.Hash(tbl) .. "\",@@server_id),0)  on duplicate key update gtid=gtid+1, memres=memc_set(concat(\"" ..  CRC32.Hash(tbl) .. "\",@@server_id),gtid+1)",{ resultset_is_needed = true } )
                i=i+1
             end 
             proxy.queries:append(i, string.char(proxy.COM_QUERY)  .. "SET binlog_format =\"MIXED\"", { resultset_is_needed = true } )
 
          
      elseif (proxy.global.backends[proxy.connection.backend_ndx].type == proxy.BACKEND_TYPE_RO and cmd.type == proxy.COM_QUERY ) then
          if  (not (tokens == nil)) then    
            tbls = get_sql_tables(tokens)
            local memcache= Memcached.Connect({memcache_host , memcache_port}) 
            local slaveGTID=0  
            local masterGTID=0  
            for tbl,v in pairs(tbls) do
             if is_debug then 
               print("    Get table from memcache   : " .. tbl .." sercer-id " ..  backend_id_server[1])
             end
             masterGTID=memcache:get(CRC32.Hash(tbl) .. backend_id_server[1] )   
	     if (masterGTID ==  nil) then
                proxy.connection.backend_ndx = lb.idle_failsafe_rw()
                if is_debug then
                  print ("   Fail back to master : No memcache entry in master")
                end
                memcache:disconnect_all()   
                break
             end 
             if is_debug then 
               print("    Get table from memcache   : " .. tbl .." sercer-id " ..  backend_id_server[proxy.connection.backend_ndx])
             end
             slaveGTID=memcache:get(CRC32.Hash(tbl) ..  backend_id_server[proxy.connection.backend_ndx] )
             if slaveGTID==nil then
                proxy.connection.backend_ndx = lb.idle_failsafe_rw()
                if is_debug then
                 print ("   Fail back to master : No memcache entry in slave")
                end
                memcache:disconnect_all()   
                break
             end  
             memcache:disconnect_all()        
             if is_debug then
		print(" master GTID : " ..  masterGTID .." for table : " .. tbl)   
                print(" slave GTID : " ..  slaveGTID .." for table : " .. tbl)   
             end
             
             if  masterGTID ~=slaveGTID then 
                proxy.connection.backend_ndx = lb.idle_failsafe_rw()
                print ("    Fail back to master : Replication Table Delay")
                break
              end 
            
            
            end
          else 
                 print ("    Fail back to master : Can't understand Query")
                 proxy.connection.backend_ndx = lb.idle_failsafe_rw()
          end 
         
      end 
 --      memcache:delete( "_lock")            
 proxy.queries:append(1, packet, { resultset_is_needed = true })
             
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
                if (inj.id > 2) then
                    if res.query_status == proxy.MYSQLD_PACKET_ERR then
                    -- proxy.queries:reset()
                     proxy.response = {
					type = proxy.MYSQLD_PACKET_ERR,
					errmsg = "can't inject GTID ".. proxy.connection.client.default_db ..
						" to node " .. proxy.global.backends[proxy.connection.backend_ndx].dst.name
				}

		--		return proxy.PROXY_SEND_RESULT
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

--- 
-- close the connections if we have enough connections in the pool
--
-- @return nil - close connection 
--         IGNORE_RESULT - store connection in the pool
function disconnect_client()
	local is_debug = proxy.global.config.rwsplit.is_debug
	if is_debug then
		print("[disconnect_client] " .. proxy.connection.client.src.name)
	end

	-- make sure we are disconnection from the connection
	-- to move the connection into the pool
	-- if  proxy.connection and  proxy.connection.backend_ndx then 
        --     proxy.connection.backend_ndx = 0
        -- end      
end

