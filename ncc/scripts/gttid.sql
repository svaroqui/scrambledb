USE mysql;
CREATE TABLE `TBLGTID` (
  `crc32` int(10) unsigned NOT NULL DEFAULT '0',
  `gtid` int(1) NOT NULL DEFAULT '0',
  `memres` tinyint(4) DEFAULT NULL,
  UNIQUE KEY `idxcrc` (`crc32`) USING HASH
) ENGINE=MEMORY DEFAULT CHARSET=latin1;
