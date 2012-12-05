DROP TABLE IF EXISTS TBLGTID;

CREATE TABLE `TBLGTID` (
  `crc32` int(10) unsigned NOT NULL DEFAULT '0',
  `gtid` int(1) NOT NULL DEFAULT '0',
  `memres` tinyint(4) DEFAULT NULL,
  UNIQUE KEY `idxcrc` (`crc32`) USING HASH
) ENGINE=MEMORY DEFAULT CHARSET=latin1;

replace into  TBLGTID select crc32(table_name) as crc32 ,0 as gtid, 0 from information_schema.tables; 

SET binlog_format ="STATEMENT";
insert into  mysql.TBLGTID select crc32("user"), 0 , memc_set(concat(crc32("user"),@@server_id),0)  on duplicate key update gtid=gtid+1, memres=memc_set(concat(crc32("user"),@@server_id),gtid+1) ;
insert into  mysql.TBLGTID select crc32("user"), 0 , memc_set(concat(crc32("user"),@@server_id),0)  on duplicate key update gtid=gtid+1, memres=memc_set(concat(crc32("user"),@@server_id),gtid+1) ;
insert into  mysql.TBLGTID select crc32("user"), 0 , memc_set(concat(crc32("user"),@@server_id),0)  on duplicate key update gtid=gtid+1, memres=memc_set(concat(crc32("user"),@@server_id),gtid+1) ;

select memc_get(concat(crc32("user"),5010));

select memc_get(concat(crc32("user"),5011));
select memc_get(concat(crc32("user"),5012));

