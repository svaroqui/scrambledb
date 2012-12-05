CREATE FUNCTION gman_do RETURNS STRING
       SONAME "libgearman_mysql_udf.so";
CREATE FUNCTION gman_do_high RETURNS STRING
       SONAME "libgearman_mysql_udf.so";
CREATE FUNCTION gman_do_low RETURNS STRING
       SONAME "libgearman_mysql_udf.so";
CREATE FUNCTION gman_do_background RETURNS STRING
       SONAME "libgearman_mysql_udf.so";
CREATE FUNCTION gman_do_high_background RETURNS STRING
       SONAME "libgearman_mysql_udf.so";
CREATE FUNCTION gman_do_low_background RETURNS STRING
       SONAME "libgearman_mysql_udf.so";
CREATE AGGREGATE FUNCTION gman_sum RETURNS INTEGER
       SONAME "libgearman_mysql_udf.so";
CREATE FUNCTION gman_servers_set RETURNS STRING
       SONAME "libgearman_mysql_udf.so";

