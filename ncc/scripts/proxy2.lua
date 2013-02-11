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
-- require('CRC32')
luasql = require 'luasql.mysql'
local _sqlEnv = assert(luasql.mysql())
local _con = nil



local commands    = require("proxy.commands")
local tokenizer   = require("proxy.tokenizer")
local lb          = require("proxy.balance")
local auto_config = require("proxy.auto-config")
local bit = require 'bit.numberlua'.bit32


local backend_id_server = { 5010,5011}
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


       



local is_in_transaction       = false
local is_in_select_calc_found_rows = false


function tablename_expand(tblname, db_name)
	if not db_name then db_name = proxy.connection.client.default_db end
	if db_name then
		tblname = db_name .. "." .. tblname
	end

	return tblname
end


function crc(s)
local bit_band, bit_bxor, bit_rshift, str_byte, str_len = bit.band, bit.bxor, bit.rshift, string.byte, string.len
 local consts = { 0x00000000, 0x77073096, 0xEE0E612C, 0x990951BA, 0x076DC419, 0x706AF48F, 0xE963A535, 0x9E6495A3, 0x0EDB8832, 0x79DCB8A4, 0xE0D5E91E, 0x97D2D988, 0x09B64C2B, 0x7EB17CBD, 0xE7B82D07, 0x90BF1D91, 0x1DB71064, 0x6AB020F2, 0xF3B97148, 0x84BE41DE, 0x1ADAD47D, 0x6DDDE4EB, 0xF4D4B551, 0x83D385C7, 0x136C9856, 0x646BA8C0, 0xFD62F97A, 0x8A65C9EC, 0x14015C4F, 0x63066CD9, 0xFA0F3D63, 0x8D080DF5, 0x3B6E20C8, 0x4C69105E, 0xD56041E4, 0xA2677172, 0x3C03E4D1, 0x4B04D447, 0xD20D85FD, 0xA50AB56B, 0x35B5A8FA, 0x42B2986C, 0xDBBBC9D6, 0xACBCF940, 0x32D86CE3, 0x45DF5C75, 0xDCD60DCF, 0xABD13D59, 0x26D930AC, 0x51DE003A, 0xC8D75180, 0xBFD06116, 0x21B4F4B5, 0x56B3C423, 0xCFBA9599, 0xB8BDA50F, 0x2802B89E, 0x5F058808, 0xC60CD9B2, 0xB10BE924, 0x2F6F7C87, 0x58684C11, 0xC1611DAB, 0xB6662D3D, 0x76DC4190, 0x01DB7106, 0x98D220BC, 0xEFD5102A, 0x71B18589, 0x06B6B51F, 0x9FBFE4A5, 0xE8B8D433, 0x7807C9A2, 0x0F00F934, 0x9609A88E, 0xE10E9818, 0x7F6A0DBB, 0x086D3D2D, 0x91646C97, 0xE6635C01, 0x6B6B51F4, 0x1C6C6162, 0x856530D8, 0xF262004E, 0x6C0695ED, 0x1B01A57B, 0x8208F4C1, 0xF50FC457, 0x65B0D9C6, 0x12B7E950, 0x8BBEB8EA, 0xFCB9887C, 0x62DD1DDF, 0x15DA2D49, 0x8CD37CF3, 0xFBD44C65, 0x4DB26158, 0x3AB551CE, 0xA3BC0074, 0xD4BB30E2, 0x4ADFA541, 0x3DD895D7, 0xA4D1C46D, 0xD3D6F4FB, 0x4369E96A, 0x346ED9FC, 0xAD678846, 0xDA60B8D0, 0x44042D73, 0x33031DE5, 0xAA0A4C5F, 0xDD0D7CC9, 0x5005713C, 0x270241AA, 0xBE0B1010, 0xC90C2086, 0x5768B525, 0x206F85B3, 0xB966D409, 0xCE61E49F, 0x5EDEF90E, 0x29D9C998, 0xB0D09822, 0xC7D7A8B4, 0x59B33D17, 0x2EB40D81, 0xB7BD5C3B, 0xC0BA6CAD, 0xEDB88320, 0x9ABFB3B6, 0x03B6E20C, 0x74B1D29A, 0xEAD54739, 0x9DD277AF, 0x04DB2615, 0x73DC1683, 0xE3630B12, 0x94643B84, 0x0D6D6A3E, 0x7A6A5AA8, 0xE40ECF0B, 0x9309FF9D, 0x0A00AE27, 0x7D079EB1, 0xF00F9344, 0x8708A3D2, 0x1E01F268, 0x6906C2FE, 0xF762575D, 0x806567CB, 0x196C3671, 0x6E6B06E7, 0xFED41B76, 0x89D32BE0, 0x10DA7A5A, 0x67DD4ACC, 0xF9B9DF6F, 0x8EBEEFF9, 0x17B7BE43, 0x60B08ED5, 0xD6D6A3E8, 0xA1D1937E, 0x38D8C2C4, 0x4FDFF252, 0xD1BB67F1, 0xA6BC5767, 0x3FB506DD, 0x48B2364B, 0xD80D2BDA, 0xAF0A1B4C, 0x36034AF6, 0x41047A60, 0xDF60EFC3, 0xA867DF55, 0x316E8EEF, 0x4669BE79, 0xCB61B38C, 0xBC66831A, 0x256FD2A0, 0x5268E236, 0xCC0C7795, 0xBB0B4703, 0x220216B9, 0x5505262F, 0xC5BA3BBE, 0xB2BD0B28, 0x2BB45A92, 0x5CB36A04, 0xC2D7FFA7, 0xB5D0CF31, 0x2CD99E8B, 0x5BDEAE1D, 0x9B64C2B0, 0xEC63F226, 0x756AA39C, 0x026D930A, 0x9C0906A9, 0xEB0E363F, 0x72076785, 0x05005713, 0x95BF4A82, 0xE2B87A14, 0x7BB12BAE, 0x0CB61B38, 0x92D28E9B, 0xE5D5BE0D, 0x7CDCEFB7, 0x0BDBDF21, 0x86D3D2D4, 0xF1D4E242, 0x68DDB3F8, 0x1FDA836E, 0x81BE16CD, 0xF6B9265B, 0x6FB077E1, 0x18B74777, 0x88085AE6, 0xFF0F6A70, 0x66063BCA, 0x11010B5C, 0x8F659EFF, 0xF862AE69, 0x616BFFD3, 0x166CCF45, 0xA00AE278, 0xD70DD2EE, 0x4E048354, 0x3903B3C2, 0xA7672661, 0xD06016F7, 0x4969474D, 0x3E6E77DB, 0xAED16A4A, 0xD9D65ADC, 0x40DF0B66, 0x37D83BF0, 0xA9BCAE53, 0xDEBB9EC5, 0x47B2CF7F, 0x30B5FFE9, 0xBDBDF21C, 0xCABAC28A, 0x53B39330, 0x24B4A3A6, 0xBAD03605, 0xCDD70693, 0x54DE5729, 0x23D967BF, 0xB3667A2E, 0xC4614AB8, 0x5D681B02, 0x2A6F2B94, 0xB40BBE37, 0xC30C8EA1, 0x5A05DF1B, 0x2D02EF8D }
     
local crc, l, i = 0xFFFFFFFF, str_len(s)
  for i = 1, l, 1 do
   crc = bit_bxor(bit_rshift(crc, 8), consts[bit_band(bit_bxor(crc, str_byte(s, i)), 0xFF) + 1])
  end
 return bit_bxor(crc, -1)
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

                       -- print ( "We are in insert into and token :" .. token["token_name"] .. "token value :" )      
                       if ( token["token_name"] == "TK_SQL_INSERT") then  
                            in_tablelist = true
                        elseif  in_tablelist then
				if (token["token_name"] == "TK_LITERAL"  or token["token_name"] == "TK_FUNCTION" )and in_tablelist  then
                                    if not db_name and tokens[i + 1] and tokens[i + 1].token_name == "TK_DOT" then
						db_name = token.text
				    else
                                        print ("Found insert table_name : " .. token.text) 
					tables[tablename_expand(token.text, db_name)] = (sql_stmt == "SELECT" and "read" or "write")
                                        in_tablelist = false
                                    end    
				elseif token["token_name"] == "TK_SQL_INTO" then
                                      in_tablelist = true
				end 
			end
		elseif sql_stmt == "UPDATE" then
			-- UPDATE <tbl> SET ..
                        if ( token["token_name"] == "TK_SQL_UPDATE" ) then  
                            in_tablelist = true
                        end        
			if in_tablelist then
				if token["token_name"] == "TK_LITERAL"  then
                                   if not db_name and tokens[i + 1] and tokens[i + 1].token_name == "TK_DOT" then
						db_name = token.text
				    else
                                        print ("Found update table_name : " .. token.text  ) 
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

local is_debug =1

function connect_server() 

end

--- 
-- put the successfully authed connection into the connection pool
--
-- @param auth the context information for the auth
--
-- auth.packet is the packet
function read_auth_result( auth )

end


function disconnect_client()
  
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

			if not is_insert_id   then
                           local backend_ndx=found_replicate(tokens);
                          
                           if backend_ndx >0 then 
                            local replicat=replication_dsn[backend_ndx]
                            _con = assert(_sqlEnv:connect(proxy.connection.client.default_db, replicat.user, replicat.password, replicat.ip, replicat.port))
			    local result = nil
                            local cur = assert(_con:execute(cmd.query))
                            if (type(cur) == "number") then
                                proxy.response.type = proxy.MYSQLD_PACKET_RAW;
                                proxy.response.packets = {
                                    "\000" .. -- fields
                                    string.char(cur) ..
                                    "\000" -- insert_id
                                }
                                result = proxy.PROXY_SEND_RESULT
                            else
                                -- Build up the result set.
                                local fields = {}
                                local colNames = cur:getcolnames()
                                local colTypes = cur:getcoltypes()
                                for a = 1, #colNames, 1 do
                                    table.insert(fields, {name = colNames[a], type=proxy.MYSQL_TYPE_STRING})
                                end
                                local curRow = {}
                                local rows = {}
                                while (cur:fetch(curRow)) do
                                    table.insert(rows, curRow)
                                end
                                proxy.response = {
                                    type = proxy.MYSQLD_PACKET_OK,
                                    resultset = {
                                        fields = fields,
                                        rows = rows
                                    }
                                }
                                result = proxy.PROXY_SEND_RESULT
                                
                            end

                            if (result ~= nil) then
                                proxy.global.config.rwsplit.com_queries_ro=proxy.global.config.rwsplit.com_queries_ro+1
                                print("com_queries_to_slave:" .. proxy.global.config.rwsplit.com_queries_ro)
                                print("com_queries_to_master:" ..  proxy.global.config.rwsplit.com_queries_rw)
                                return result
                            end
                          end -- we found a replicate 
                             
			else
			   print("   found a SELECT LAST_INSERT_ID(), staying on the same backend")
                                
			end
		end -- TK_SQL_SELECT

                

	end -- is_not_in_transaction and COM_QUERY

	
        proxy.global.config.rwsplit.com_queries_rw=proxy.global.config.rwsplit.com_queries_rw+1
        print("com_queries_to_slave:" .. proxy.global.config.rwsplit.com_queries_ro)
        print("com_queries_to_master:" ..  proxy.global.config.rwsplit.com_queries_rw)
        local tbls  = {} 
   
       if  cmd.type == proxy.COM_QUERY  and stmt.token_name ~= "TK_SQL_SELECT" then
            tokens2     = tokens or assert(tokenizer.tokenize(cmd.query))
            print ("    Injection GTID")
            tbls = get_sql_tables(tokens2)
	    proxy.queries:append(3, string.char(proxy.COM_QUERY)  .. "SET binlog_format =\"STATEMENT\"", { resultset_is_needed = true } )
            local i=4
            for tbl,v in pairs(tbls) do 
                print (tbl)
                print ("INSERT into  mysql.TBLGTID select CRC32(\"" .. tbl .. "\", 0 , memc_set(concat(\"" .. CRC32.Hash(tbl) .. "\",@@server_id),0)  on duplicate key update gtid=gtid+1, memres=memc_set(concat(\"" ..  crc(tbl) .. "\",@@server_id),gtid+1)") 
                proxy.queries:append(i, string.char(proxy.COM_QUERY)  .. "INSERT into mysql.TBLGTID select CRC32(\"" .. tbl .. "\"), 0 , memc_set(concat(\"" .. crc(tbl) .. "\",@@server_id),0)  on duplicate key update gtid=gtid+1, memres=memc_set(concat(\"" ..  crc(tbl) .. "\",@@server_id),gtid+1)",{ resultset_is_needed = true } )
                i=i+1
            end 
            proxy.queries:append(i, string.char(proxy.COM_QUERY)  .. "SET binlog_format =\"MIXED\"", { resultset_is_needed = true } )
   
       end 
        
      
       proxy.queries:append(1, packet, { resultset_is_needed = true })
       return proxy.PROXY_SEND_QUERY
end


function found_replicate(tokens ) 
  local is_debug = proxy.global.config.rwsplit.is_debug  
  
  --       while ( memcache:add("_lock","lock",1) == nil ) do 
  --            print("lock")
  --       end
 if  (not (tokens == nil)) then    
     
  local memcache= Memcached.Connect(memcache_master , memcache_port) 
   
  for k, v in ipairs(replication_dsn) do
    local isgood=1
    local tbls  = {} 
    print(" is_query_to_slave")
            tbls = get_sql_tables(tokens)
           
            local slaveGTID=0  
            local masterGTID=0  
            
            for tbl,v in pairs(tbls) do
                if is_debug then 
                  print("    Get table from memcache   : " .. tbl .." sercer-id " ..  backend_id_server[1])
                end
                masterGTID=memcache:get(crc(tbl) .. backend_id_server[1] )   
                if (masterGTID ==  nil) then
                 
                   if is_debug then
                     print ("   Fail back to master : No memcache entry in master")
                   end
                 
                   isgood= 0
                end 
                if is_debug then 
                  print("    Get table from memcache   : " .. tbl .." sercer-id " ..  backend_id_server[k+1])
                end
                slaveGTID=memcache:get(crc(tbl) ..  backend_id_server[k+1] )
                if slaveGTID==nil then
                   if is_debug then
                    print ("   Fail back to master : No memcache entry in slave")
                   end
                 
                   isgood= 0
                   
                end  
                if is_debug then
                   print(" master GTID : " ..  masterGTID .." for table : " .. tbl)   
                   print(" slave GTID : " ..  slaveGTID .." for table : " .. tbl)   
                end

                if  masterGTID ~=slaveGTID then 
                   print ("    Fail back to master : Replication Table Delay")
                   isgood= 0
                end 
                if isgood == 1 then 
                   -- we pass all test on this replicate  
                   memcache:disconnect_all()
                   return k
                end    

            end -- for each table 
                
            
          
           
      end  -- for each slave     
       memcache:disconnect_all()
 end  -- tokens NOT nil
 
 return 0   


--  memcache:increment( backend_id_server[proxy.connection.backend_ndx],1)                     
 --      memcache:delete( "_lock")       
end 


function read_query_result( inj ) 
	local is_debug = proxy.global.config.rwsplit.is_debug
	local res      = assert(inj.resultset)
  	local flags    = res.flags
       

	if inj.id ~= 1 then
	
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

	
end





   