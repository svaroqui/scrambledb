#!/usr/bin/env perl
#  Copyright (C) 2012 Stephane Varoqui @SkySQL AB Co.,Ltd.
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#   along with this program; if not, write to the Free Software
#  Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
## Note: This is a sample script and is not complete. Modify the script based on your environment.

use strict;
use Class::Struct;
use warnings FATAL => 'all';
use Scramble::ClusterConfig;
use Scramble::ClusterUtils;
use Scramble::ClusterLog;
use Scramble::ClusterTransport;

use Sys::Hostname;
use Gearman::XS qw(:constants);
use Gearman::XS::Client;
use Gearman::XS::Worker;
use JSON;
use Cache::Memcached;
use DBI;
use Data::Dumper;



our  %ERRORMESSAGE = (
    "000000" => "OK",
    "ER0101" => "Error writing mysql_proxy config",
    "ER0102" => "Error writing ha_proxy config",
    "ER0103" => "Error writing memcached config",
    "ER0104" => "Error writing mysql config",
    "ER0105" => "Error writing lua script",
    "ER0107" => "Error writing mha config",
    "ER0106" => "Error writing keepalived config"
);


our $database = "";
our $SKYBASEDIR = $ ENV {SKYBASEDIR};
our $SKYDATADIR = $ ENV {SKYDATADIR};
our $gearman_timeout = 2000;
our $mysql_connect_timeout=3;
our $console = "{result:{status:'00000'}}";
our $config = new Scramble::ClusterConfig::;
our $log = new Scramble::ClusterLog;
my $conf=$SKYBASEDIR . "/ncc/etc/cloud.cnf";
$config->read($conf);
$config->check('SANDBOX');
$log->set_logs($config);

my $worker = new Gearman::XS::Worker;
my $ret = $worker->add_server('',0);
if ($ret != GEARMAN_SUCCESS) {
    printf(STDERR "%s\n", $worker->error());
    exit(1);
}

$ret = $worker->add_function("write_config", 0, \&write_cmd, 0);
if ($ret != GEARMAN_SUCCESS) {
    printf(STDERR "%s\n", $worker->error());
}

while (1) {

    my $ret = $worker->work();
    if ($ret != GEARMAN_SUCCESS) {
         printf(STDERR "%s\n", $worker->error());
    }
}



sub write_cmd {
    my ($job, $options) = @_;
    
    my $command = $job->workload();
    my $json = new JSON;
    my $json_text = $json->allow_nonref->utf8->relaxed->escape_slash->loose->allow_singlequote->allow_barekey->decode($command);
    $console= "";

    $config->read($SKYBASEDIR."/ncc/etc/cloud.cnf");
    $config->check('SANDBOX');
    my $json_config   = $json->allow_blessed->convert_blessed->encode($config );
  
    $log->log_debug("[worker_config] Receive command ".$command,1,"write_config");
  
    write_mysql_proxy_config();
    $log->log_debug("[worker_config] Info: mysql-proxy ",1,"write_config");
    write_mha_config($SKYBASEDIR. "/ncc/etc/mha.cnf");
    $log->log_debug("[worker_config] Info: mha ",1,"write_config");
    write_keepalived_config();
    $log->log_debug("[worker_config] Info: keepalived ",1,"write_config");
    write_haproxy_config();
    $log->log_debug("[worker_config] Info: haproxy ",1,"write_config");
    write_memcached_config();
    $log->log_debug("[worker_config] Info: memcached ",1,"write_config");
    write_lua_script();
    $log->log_debug("[worker_config] Info: lua scripts ",1,"write_config");
    write_write_alias($SKYBASEDIR."/ncc/bin/alias.sh");
    $log->log_debug("[worker_config] Info: alias ",1,"write_config");
    write_tarantool_config();
    $log->log_debug("[worker_config] Info: tarantool ",1,"write_config");
    write_mysql_config(); 
    $log->log_debug("[worker_config] Info: mysql ",1,"write_config");
    write_cassandra_config();
      $log->log_debug("[worker_config] Info: cassandra ",1,"write_config");
    write_httpd_config();  
      $log->log_debug("[worker_config] Info: http ",1,"write_config");
    return  $console ;

}



sub replace_patern_infile($$$){
    my $file= shift;
    my $strin= shift;
    my $strout= shift;
    open my $in,  '<',  $file      or die "Can't read old file: $!";
    open my $out, '>', "$file.new" or die "Can't write new file: $!";
    while( <$in> )
    {
        s/^$strin(.*)$/$strout/gi;
        print $out $_;
    }
    close $out;
    system("rm -f $file.old");
    system("mv $file $file.old");
    system("mv $file.new $file");
    system("chmod 660 $file" );
}

sub write_cassandra_config(){

my $host_nosql;
 foreach my $nosql (keys(%{$config->{nosql}})) {
   my $dir=$SKYBASEDIR. "/ncc/etc/".$nosql; 
   $host_nosql = $config->{nosql}->{default};
   $host_nosql = $config->{nosql}->{$nosql}; 
   if  ($host_nosql->{mode} eq "cassandra") {     
       $log->log_debug("[worker_config] Info:  cp -Rf $SKYBASEDIR/ncc/etc/cassandra.template $SKYBASEDIR/ncc/etc/$nosql ",1,"write_config");
       system("mkdir  $SKYBASEDIR/ncc/etc/$nosql");
       system("cp -rf $SKYBASEDIR/ncc/etc/cassandra.template/* $SKYBASEDIR/ncc/etc/$nosql");
       replace_patern_infile( "$SKYBASEDIR/ncc/etc/$nosql/cassandra.yaml","saved_caches_directory:" , "saved_caches_directory: $SKYDATADIR/$nosql/saved_caches");
       replace_patern_infile( "$SKYBASEDIR/ncc/etc/$nosql/cassandra.yaml","data_file_directories:" , "data_file_directories: \n\t- $SKYDATADIR/$nosql/data");
       replace_patern_infile( "$SKYBASEDIR/ncc/etc/$nosql/cassandra.yaml","commitlog_directory:" , "commitlog_directory: $SKYDATADIR/$nosql/commitlog");
       replace_patern_infile( "$SKYBASEDIR/ncc/etc/$nosql/cassandra.yaml","listen_address:" , "listen_address: $host_nosql->{ip}");
       my $seed = Scramble::ClusterUtils::get_all_cassandra($config);
       replace_patern_infile( "$SKYBASEDIR/ncc/etc/$nosql/cassandra.yaml","- seeds:" , "- seeds: \"$seed\"");
       
       replace_patern_infile( "$SKYBASEDIR/ncc/etc/$nosql/log4j-server.properties","log4j.appender.R.File=" , "log4j.appender.R.File=$SKYDATADIR/$nosql/log/system.log");
     
           

   }        
  }     
}


sub write_httpd_config(){

my $host_httpd;
 foreach my $httpd (keys(%{$config->{http}})) {
    
   $host_httpd = $config->{http}->{default};
   $host_httpd = $config->{http}->{$httpd}; 
   if  ($host_httpd->{mode} eq "apache") {     
       $log->log_debug("[worker_config] Info:  cp -f $SKYBASEDIR/ncc/etc/httpd.template $SKYBASEDIR/ncc/etc/$httpd/",1,"write_config");
       system("cp -f $SKYBASEDIR/ncc/etc/httpd.template $SKYBASEDIR/ncc/etc/".$httpd."/".$httpd.".cnf");
       replace_patern_infile( "$SKYBASEDIR/ncc/etc/$httpd/".$httpd. ".cnf",  '<Directory "/var/lib/skysql/www/">', '<Directory "'.$SKYDATADIR.'/'.$httpd .'/htdocs/">');
       replace_patern_infile( "$SKYBASEDIR/ncc/etc/$httpd/".$httpd. ".cnf",  'Listen 80', 'Listen ' . $host_httpd->{port} );
       replace_patern_infile( "$SKYBASEDIR/ncc/etc/$httpd/".$httpd. ".cnf", 'DocumentRoot "/var/lib/skysql/www"', 'DocumentRoot "'.$SKYDATADIR.'/'.$httpd .'/htdocs"' ); 
       replace_patern_infile( "$SKYBASEDIR/ncc/etc/$httpd/".$httpd. ".cnf", 'AuthUserFile ""', 'AuthUserFile "'.$SKYDATADIR.'/'.$httpd .'/passwords"' ); 
   }        
  }     
}

sub write_keepalived_config(){
 my $i=100;
 my $lb_info;
 my $cloud_name = Scramble::ClusterUtils::get_active_cloud_name($config);  
    

 
 foreach my $lb (keys(%{$config->{lb}})) {
    my $dir=$SKYBASEDIR. "/ncc/etc/".$lb; 
    mkdir($dir) unless(-d $dir) ;
    $lb_info = $config->{lb}->{default};
    $lb_info = $config->{lb}->{$lb};
    if  ( $lb_info->{mode} eq "keepalived"){
    open my $out, '>', "$dir/$lb.cnf" or die "Can't write new file: $!";

    print $out "global_defs {\n";
    print $out "  notification_email {\n";
    print $out "    stephane\@skysql.com\n" ;
    print $out "  }\n";
    print $out "  notification_email_from keepalived@".$lb. "\n";
    print $out "  smtp_server localhost\n";
    print $out "  smtp_connect_timeout 30\n";
    print $out "  # This should be unique.\n";
    print $out "  router_id 55\n";
    print $out "}\n";
    print $out "\n";
    print $out "vrrp_instance mysql_pool {\n";
    print $out "  # The interface we listen on.\n";
    print $out "  interface ". $lb_info->{device}."\n";
    my $peerlb = $config->{lb}->{$lb_info->{peer}};
    print $out "  vrrp_unicast_bind " .$lb_info->{ip}."\n";
    print $out "  vrrp_unicast_peer " . $peerlb->{ip}."\n";
#$config->{db}->{ $host_info->{peer}[0] };

    print $out "  # The default state, one should be master, the others should be set to SLAVE.\n";
    if  ( $lb_info->{status} eq "master" ) {
        print $out "  state MASTER\n";
    }     
    else
    {
        print $out "  state SLAVE\n";
    } 
    print $out "\n";
    print $out " # This should be the same on all participating load balancers.\n";
    print $out "  virtual_router_id 1\n";
    print $out "\n";
    print $out "  priority ". $i."\n";
    print $out "\n";
    print $out '  notify_master "'.$SKYBASEDIR.'/ncc/bin/clmgr services switch_vip"' ."\n";
    #print $out 'notify_fault "'.$SKYBASEDIR.'/scripts/vrrp.sh FAULT"\n';

    print $out "  # Set the interface whose status to track to trigger a failover.  \n";
    print $out "  track_interface {\n";
    print $out "    ". $lb_info->{device}."\n";
    print $out "  }\n";
    print $out "\n";
    print $out "  # Password for the loadbalancers to share.\n";
    print $out "  authentication {\n";
    print $out "    auth_type PASS\n";
    print $out "    auth_pass 1111\n";
    print $out "  }\n";
    print $out "\n";
    print $out "  # This is the IP address that floats between the loadbalancers.\n";
    print $out "  virtual_ipaddress {\n";
    print $out "    ". $lb_info->{vip}."/24\n";
    print $out "  }\n";
    print $out "}\n";
    print $out "\n";

    if ( $lb_info->{balance} ne "none")  {
    print $out "# Here we add the virtal mysql node\n";
    print $out "virtual_server ". $lb_info->{vip}." ".$lb_info->{port}." {\n";
    print $out "  delay_loop 6\n";
    print $out "  # Round robin, but you can use whatever fits your needs.\n";
    print $out "  lb_algo rr\n";
    print $out "  lb_kind DR\n";
  #  print $out "  lb_kind NAT\n";

    print $out "nat_mask 255.255.255.0\n";
    print $out "persistence_timeout 50\n";

    print $out "  protocol TCP\n";
    my $host_info;
    foreach my $host (keys(%{$config->{proxy}})) {
       if ($host_info->{cloud} eq $cloud_name){
        $host_info = $config->{proxy}->{default};
        $host_info = $config->{proxy}->{$host};
        print $out "\n";
        print $out "   real_server ". $host_info->{ip}." ". $host_info->{port} . "  {\n";
        print $out "     weight 10\n";
        print $out "    TCP_CHECK {\n";
   #     print $out "      connect_port 22\n";
        print $out "      connect_timeout 3\n";
        print $out "   } \n";
        print $out " }\n";
       } 
     }
     print $out " }\n";
     }
     close $out;
     $i++;

    }
    }

}

sub write_lua_script(){

 my $i=100;
 my $cloud_name = Scramble::ClusterUtils::get_active_cloud_name($config);

 my $host_info;
 my $cluster;   
 foreach my $host (keys(%{$config->{proxy}})) {
    $host_info = $config->{proxy}->{default};
    $host_info = $config->{proxy}->{$host};
  
    open my $in ,  '<', $SKYBASEDIR."/ncc/scripts/". $host_info->{script} or die "Can't write new file: $!";
    open my $out, '>', $SKYBASEDIR."/ncc/scripts/$host.lua" or die "Can't write new file: $!";
    while (<$in>) {
       # s/^$strin(.*)$/$strout/gi;
         if (/-- insert here --/) {
          print $out "local cluster=\"$host_info->{cluster}\"\n" ;
          my $nosql_info; 
          foreach my $nosql (keys(%{$config->{nosql}})) {
            $nosql_info = $config->{nosql}->{default};
            $nosql_info = $config->{nosql}->{$nosql};
            if  ( $nosql_info->{status} eq "master" && $nosql_info->{mode} eq "memcache" && $nosql_info->{cloud} eq $cloud_name ){
                print $out  get_lua_connection_pool_server_id() ."\n";
                print $out "local memcache_master=\"" . $nosql_info->{ip} ."\"\n"; 
                print $out "local memcache_port = " . $nosql_info->{port} ."\n";

                print $out "if not proxy.global.config.rwsplit then\n";
                print $out "proxy.global.config.rwsplit = {\n";
                print $out "       com_queries_ro   = 0,\n";
                print $out "       com_queries_rw   = 0,\n";
                if ($host_info->{lua_debug}  ) {
                   print $out "	is_debug = true \n"; 
                }   
                else 
                {
                  print $out "	is_debug = false  \n"; 
                }

                print $out "}\n";
                print $out "end\n";
            }
          }
        } 
        print $out $_;
    }
    close $out;
   # system("rm -f $file.old");
   # system("mv $file $file.old");
   # system("mv $file.new $file");
   # system("chmod 660 $file");

  } 
  return 0; 
}


sub write_write_alias($){
my $file= shift;
my $host_info ;
    my $err = "000000";
    
    open my $out, '>', "$file" or die "Can't write new file: $!";
    foreach my $host (keys(%{$config->{db}})) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        my $cmd = 
          "alias $host='"
          .  $SKYBASEDIR
          . "/mysql-client/bin/mysql "
          . " --user="
          . $host_info->{mysql_user}
          . " --password="
          . $host_info->{mysql_password}
          . " --host="
          . $host_info->{ip}
          . " --port="
          . $host_info->{mysql_port}
          ."'";
        print $out $cmd ."\n";
    }   
    foreach my $host (keys(%{$config->{lb}})) {
        
        $host_info = $config->{lb}->{$host};
        my $theip= $host_info->{ip};
        if ( $host_info->{mode} eq "keepalived" ) {
           $theip = $host_info->{vip};
        }
        
        my $cmd = 
          "alias $host='"
          .  $SKYBASEDIR
          . "/mysql-client/bin/mysql "
          . " --user="
          . $host_info->{mysql_user}
          . " --password="
          . $host_info->{mysql_password}
          . " --host="
          . $theip
          . " --port="
          . $host_info->{port}
          ."'";
        print $out $cmd ."\n";
    }   
    
    close $out;
      system("chmod 777 $file" );
return 0 ;
}


sub write_tarantool_config() {

 my $i=100;
 my $host_nosql;
 foreach my $nosql (keys(%{$config->{nosql}})) {
   my $dir=$SKYBASEDIR. "/ncc/etc/".$nosql; 
    mkdir($dir) unless(-d $dir) ; 
    
   $host_nosql = $config->{nosql}->{default};
   $host_nosql = $config->{nosql}->{$nosql}; 
   if  ($host_nosql->{mode} eq "tarantool") {
    open my $out, '>', "$dir/$nosql.cnf" or die "Can't write new file: $!";
    print $out "# Limit of memory used to store tuples to 100MB\n";
    print $out "# (0.1 GB)\n";
    print $out "# This effectively limits the memory, used by\n";
    print $out "# Tarantool. However, index and connection memory\n";
    print $out "# is stored outside the slab allocator, hence\n";
    print $out "# the effective memory usage can be higher (sometimes\n";
    print $out "# twice as high).\n";
    print $out "slab_alloc_arena = 0.05\n";

    print $out "#\n";
    print $out "# Store the pid in this file. Relative to\n";
    print $out "# startup dir.\n";
    print $out "pid_file = \"".$SKYDATADIR."/tmp/tarantool.". $nosql .".pid\"\n";

    print $out "\n";
    print $out "# Pipe the logs into the following process.\n";
    print $out "logger=\"cat - >> ". $SKYDATADIR . "/log/tarantool.". $nosql  .".log\"\n";

    print $out "#\n";
    print $out "# Read only and read-write port.\n";
    print $out "primary_port = ". $host_nosql->{port} ."\n";

    print $out "#\n";
    print $out "# Read-only port.\n";
    print $out "secondary_port = ". $host_nosql->{secondary_port} ."\n";

    print $out "#\n";
    print $out "# The port for administrative commands.\n";
    print $out "admin_port = ". $host_nosql->{admin_port} ."\n";

    print $out "#\n";
    print $out "# Each write ahead log contains this many rows.\n";
    print $out "# When the limit is reached, Tarantool closes\n";
    print $out "# the WAL and starts a new one.\n";
    print $out "rows_per_wal = ". $host_nosql->{admin_port} ."\n";


    print $out "# Define a simple space with 1 HASH-based\n";
    print $out "# primary key.\n";
    print $out "space[0].enabled = 1\n";
    print $out "space[0].index[0].type = \"HASH\"\n";
    print $out "space[0].index[0].unique = 1\n";
    print $out "space[0].index[0].key_field[0].fieldno = 0\n";
    print $out "space[0].index[0].key_field[0].type = \"NUM\"\n";

    print $out "# working directory (daemon will chdir(2) to it)\n";
    print $out "work_dir = \"". $SKYDATADIR . "/". $nosql  ."\"\n";


    
   } 
 }
}

sub write_cluster_config (){


$conf = <<END;

[TCP DEFAULT]
SendBufferMemory=256K
ReceiveBufferMemory=256K

[NDB_MGMD DEFAULT]
PortNumber=1186
Datadir=_DATADIR

[NDB_MGMD]
Id=1
Hostname=localhost

[NDBD DEFAULT]
NoOfReplicas=2
Datadir=_DATADIR
DataMemory=128MM
SharedGlobalMemory=64M
DiskPageBufferMemory=32M
IndexMemory=43M
LockPagesInMainMemory=0

MaxNoOfConcurrentOperations=32768

StringMemory=25
MaxNoOfTables=512
MaxNoOfOrderedIndexes=256
MaxNoOfUniqueHashIndexes=256
MaxNoOfAttributes=5000
DiskCheckpointSpeedInRestart=100M
FragmentLogFileSize=256M
NoOfFragmentLogFiles=3
RedoBuffer=8M

HeartbeatIntervalDbDb=15000
HeartbeatIntervalDbApi=15000
TimeBetweenLocalCheckpoints=20
TimeBetweenGlobalCheckpoints=1000
TimeBetweenEpochs=100

TimeBetweenEpochsTimeout=32000
MemReportFrequency=30
BackupReportFrequency=30
END

}
sub write_haproxy_config(){
 
 my $i=100;
 my $lb_info;
 my $cloud_name = Scramble::ClusterUtils::get_active_cloud_name($config);   
 foreach my $lb (keys(%{$config->{lb}})) {
    my $dir=$SKYBASEDIR. "/ncc/etc/".$lb; 
    mkdir($dir) unless(-d $dir) ;
    $lb_info = $config->{lb}->{default};
    $lb_info = $config->{lb}->{$lb};
    if  ( $lb_info->{mode} eq "haproxy" ){
    open my $out, '>', "$dir/$lb.cnf" or die "Can't write new file: $!";
    print $out "global\n";
    print $out "   log         127.0.0.1 local2\n";
    #print $out "   chroot      ".."/haproxy\n";
    #print $out "   pidfile     ".$SKYBASEDIR."/ncc/tmp/haproxy.pid\n";
    print $out "   maxconn     4000\n";
    #print $out "   user        ".."\n";
    #print $out "   group       haproxy\n";
    print $out "   daemon\n";
    print $out "   # turn on stats unix socket\n";
    print $out "   stats socket ".$SKYDATADIR."/tmp/stats\n";
    print $out "#---------------------------------------------------------------------\n";
    print $out "# common defaults that all the 'listen' and 'backend' sections will\n";
    print $out "# use if not designated in their block\n";
    print $out "#---------------------------------------------------------------------\n";
    print $out "defaults\n";
    print $out "   mode                    http\n";
    print $out "   log                     global\n";
    print $out "   option                  tcplog\n";
    print $out "   option                  dontlognull\n";
    print $out "   option                  redispatch\n";
    print $out "   retries                 3\n";
    print $out "   timeout http-request    10s\n";
    print $out "   timeout connect         10s\n";
    print $out "   timeout client          50000\n";
    print $out "   timeout server          50000\n";
    print $out "#    timeout http-keep-alive 10s\n";
    print $out "   timeout check           10s\n";
    print $out "   maxconn                 3000\n";

    print $out "frontend http-front\n";
    print $out "  mode tcp\n";
    print $out "  bind     ".$lb_info->{vip}.":80\n";
    print $out "  default_backend  http-back\n";

    print $out "frontend mysql-front\n";
    print $out "  mode tcp\n";
    print $out "  bind     ".$lb_info->{vip}.":".$lb_info->{port}."\n";
#    print $out "  bind     :".$lb_info->{port}."\n";
  
    print $out "  default_backend  mysql-back\n";
   
    # TO be tested  
    #tcp-smart-accept
    #option Ê Êtcp-smart-connect
    
    print $out "listen stats :8080\n";
    print $out "  mode http\n";
    print $out "  stats uri /stats\n";
    print $out "  stats enable\n";
    print $out "  stats refresh 5s\n";     
 
    print $out "#---------------------------------------------------------------------\n";
    print $out "# round robin balancing between the various mysql-proxy\n";
    print $out "#---------------------------------------------------------------------\n";
    print $out "backend mysql-back\n";
    print $out "  balance     roundrobin\n";
    print $out "  mode    tcp\n";
    my $host_info;
    foreach my $host (keys(%{$config->{proxy}})) {
        $host_info = $config->{proxy}->{$host};
       if ($host_info->{cloud} eq $cloud_name && $host_info->{cluster} eq $lb_info->{cluster}){  
    
        print $out "   server $host ". $host_info->{ip}.":". $host_info->{port} . "  check inter 3000\n";
      }
     }

     print $out "backend http-back\n";
    print $out "  balance     roundrobin\n";
    print $out "  mode    http\n";
    foreach my $host (keys(%{$config->{http}})) {
        $host_info = $config->{http}->{$host};
       if ($host_info->{cloud} eq $cloud_name && $host_info->{cluster} eq $lb_info->{cluster}){  
    
        print $out "   server $host ". $host_info->{ip}.":". $host_info->{port} . "  check\n";
      }
     }

      close $out;



      system("chmod 660 $dir/$lb.cnf " );
     $i++;

  }
 }
}

sub write_memcached_config(){
   
    my $lb= Scramble::ClusterUtils::get_active_lb($config);
    my $i=100;
    my $nosql;
    foreach my $host (keys(%{$config->{nosql}})) {
        my $dir=$SKYBASEDIR. "/ncc/etc/".$host; 
        mkdir($dir) unless(-d $dir) ;
        $nosql = $config->{nosql}->{default};
        $nosql = $config->{nosql}->{$host};
        if  ( $nosql->{mode} eq "memcache"){
        open my $out, '>', "$dir/$host.cnf" or die "Can't write new file: $!";
        print $out "-m ".$nosql->{mem}."\n";
        print $out "# default port\n";
        print $out "-p ".$nosql->{port}."\n";
        print $out "# user to run daemon nobody/apache/www-data\n";
        print $out "-u skysql\n";
        
        print $out "-l 0.0.0.0\n";
        print $out "-P ".$SKYDATADIR."/tmp/memcached.". $host . ".pid\n";

        close $out;
       $i++;
       system("chmod 660 $dir/$host.cnf" );
    }
  }

  # replace_config($file ,"proxy-read-only-backend-addresses","proxy-read-only-backend-addresses=". $slaves);
  # replace_config($file ,"proxy-backend-addresses","proxy-backend-addresses=".  $masters );
 }

sub get_memory_from_status($){
 my $host_info =shift;
 return 2000;
}

sub get_json_from_file($){
    my $file =shift;
    use JSON;

    my $json;
    {
      local $/; #Enable 'slurp' mode
      open my $fh, "<", $file;
      $json = <$fh>;
      close $fh;
    }
  return $json;
}


sub write_mysql_config(){
    my $host_info ;
    my $err = "000000";
    my @masters;
    my $cloud_name = Scramble::ClusterUtils::get_active_cloud_name($config);  
    foreach my $host (keys(%{$config->{db}})) {
        my $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        if ($host_info->{cloud} eq $cloud_name){  
        if (Scramble::ClusterUtils::is_ip_localhost($host_info->{ip}) ==1) {
          my $file = "$SKYDATADIR/".$host."/my.sandbox.cnf";
          my $param="ls -l $SKYDATADIR/$host| grep -v 'No such'  | wc -l";
      #    my $res=system( $param);  
      #    $res =~ s/^\s+//;
      #    $res =~ s/\s+$//;
          if (-e  $file)  {
          
            my $change_variables =get_mysql_variables_diff($host,$host_info); 
            foreach my $variable (keys(%{$change_variables})) {
               if ( $variable ne "scramble" ) {
                   replace_patern_infile("$SKYDATADIR/".$host."/my.sandbox.cnf" , $variable,$variable. "=" .$change_variables->{$variable} );  
               }
            }
            my $ram =get_memory_from_status($host_info);
            $log->log_debug("[worker_config] Info: mysql get_memory_from_status ". $ram ,1,"write_config");
            my $mem = $ram*$host_info->{mem_pct}/100;    
          
             $log->log_debug("[worker_config] Info: mysql Setting memory to ". $mem ,1,"write_config");
            replace_patern_infile($SKYDATADIR."/".$host."/my.sandbox.cnf", "innodb_buffer_pool_size","innodb_buffer_pool_size=" . $mem."M");        
            replace_patern_infile($SKYDATADIR."/".$host."/my.sandbox.cnf", "handlersocket_address","handlersocket_address=" . $host_info->{handlersocket_address} );        
            replace_patern_infile($SKYDATADIR."/".$host."/my.sandbox.cnf", "handlersocket_port=","handlersocket_port=" . $host_info->{handlersocket_port} );        
            replace_patern_infile($SKYDATADIR."/".$host."/my.sandbox.cnf", "handlersocket_port_wr=","handlersocket_port_wr=" . $host_info->{handlersocket_port_wr} );        
        
           }
  
        }
       }
    }
    return 0 ;
}

sub write_mysql_proxy_config(){


my $slaves =  Scramble::ClusterUtils::get_all_slaves($config) ;

my $i=100;
my $host_info;
foreach my $host (keys(%{$config->{proxy}})) {
    $host_info = $config->{proxy}->{default};
    $host_info = $config->{proxy}->{$host};
    my $masters = Scramble::ClusterUtils::get_all_masters_cluster($config,$host_info->{cluster});
    my $dir=$SKYBASEDIR. "/ncc/etc/".$host; 
    mkdir($dir) unless(-d $dir) ;
    open my $out, '>', "$dir/$host.cnf" or die "Can't write new file: $!";

    print $out "[mysql-proxy]\n";
    print $out "pid-file = $SKYDATADIR/tmp/mysql-proxy.$host.pid\n";
    print $out "log-file = $SKYDATADIR/log/mysql-proxy.".$host .".log\n";
    print $out "log-level = ". $host_info->{log_level} ."\n";
    print $out "event-threads = ". $host_info->{event_threads} ."\n";
    print $out "max-open-files=". $host_info->{max_open_files} ."\n";
    print $out "proxy-fix-bug-25371=". $host_info->{proxy_fix_bug_25371} ."\n";
    #print $out "plugin-dir=lib/mysql-proxy/plugins\n";
    #print $out "admin-username=". $host_info->{admin_user}." \n";
    #print $out "admin-password=". $host_info->{admin_password}." \n";
    #print $out "admin-lua-script=lib/mysql-proxy/lua/admin.lua\n";
    print $out "proxy-address =". $host_info->{ip}.":". $host_info->{port} ."\n";
    print $out "proxy-backend-addresses=".$masters."\n";
    #print $out "proxy-read-only-backend-addresses=". $slaves."\n";
    print $out "proxy-lua-script = $SKYBASEDIR/ncc/scripts/". $host .".lua\n";
    print $out "lua-path=$SKYBASEDIR/mysql-proxy/lib/mysql-proxy/lua/?.lua\n";
    print $out "lua-cpath=$SKYBASEDIR/mysql-proxy/lib/mysql-proxy/lua/?.so\n";
     close $out;
 $i++;
system("chmod 660 $dir/$host.cnf" );
}


# replace_patern_infile($file ,"proxy-read-only-backend-addresses","proxy-read-only-backend-addresses=". $slaves);
# replace_patern_infile($file ,"proxy-backend-addresses","proxy-backend-addresses=".  $masters );
}



sub write_mha_config($){
  my $file = shift;
  my $host_info ;
  my $err = "000000";
  my $i=1;
  my $cloud=Scramble::ClusterUtils::get_active_cloud($config);
  my $cloud_name = Scramble::ClusterUtils::get_active_cloud_name($config);
  open my $out, '>', "$file.new" or die "Can't write new file: $!";
  foreach my $host (keys(%{$config->{db}}) ) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        if (
                (  $host_info->{status} eq "master" 
                || $host_info->{status} eq "slave")
               &&  $host_info->{cloud} eq $cloud_name
               && ( $host_info->{mode} ne "spider" 
                 || $host_info->{mode} ne "ndbd"
                 || $host_info->{mode} ne "galera"
                 || $host_info->{mode} ne "leveldb"
                 || $host_info->{mode} ne "cassandra"
                 || $host_info->{mode} ne "hbase"

               )&&
               ( $host_info->{status} ne "standalone" 
                 || $host_info->{status} ne "discard"
               )
         ) {
            print $out "[server$i]\n";
            print $out "hostname=$host_info->{ip}\n";
            print $out "ip=$host_info->{ip}\n";
            print $out "port=$host_info->{mysql_port}\n";
            print $out "repl_user=$host_info->{mysql_user}\n";
            print $out "repl_password=$host_info->{mysql_password}\n";
            print $out "user=$host_info->{mysql_user}\n";
            print $out "password=$host_info->{mysql_password}\n";
            print $out "remote_workdir=$SKYDATADIR/mha\n";
            print $out "master_binlog_dir=$SKYDATADIR/$host/data\n";
            print $out "ssh_options=\"-i $SKYDATADIR/.ssh/". $cloud->{public_key} . "\"\n";
            print $out "basedir=$SKYBASEDIR\n";
            print $out "\n";
            $i++;
        }
    }
    close $out;
    system("rm -f $file.old");
    system("mv $file $file.old");
    system("mv $file.new $file");
    system("chmod 660 $file" );
    return  $err;
}

sub get_lua_connection_pool_server_id(){
    my @backend; 
    my $host_info ;
   
    my @replicas; 

    my $cloud_name = Scramble::ClusterUtils::get_active_cloud_name($config);
    print STDERR "cloud_name" . $cloud_name;
    foreach my $host (keys(%{$config->{db}})) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        if ($host_info->{status} eq "master"  && $host_info->{cloud} eq $cloud_name) {
              push(@backend  , $host_info->{mysql_port} );
         }
    }
    foreach my $host (keys(%{$config->{db}})) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        if ($host_info->{status} eq "slave" && $host_info->{cloud} eq $cloud_name) {
              push(@backend ,  $host_info->{mysql_port});
              push(@replicas, "{ port = " . $host_info->{mysql_port}
               . ", ip = '".   $host_info->{ip} ."'"
               . ", user = '".   $host_info->{mysql_user} ."'"
               . ", password  = '".   $host_info->{mysql_password} ."'"
               ."}");
         }
    }
    
    return "local backend_id_server = { " . join(',',@backend). "}\n local replication_dsn = {". join(',',@replicas). "}\n";
    
}





sub write_certificat(){
    my $my_home_user = $ ENV {HOME};
    my $cmd='string="`cat '.$SKYBASEDIR.'/ncc/etc/id_rsa.pub`"; sed -e "\|$string|h; \${x;s|$string||;{g;t};a\\" -e "$string" -e "}" $HOME/.ssh/authorized_keys > $HOME/.ssh/authorized_keys2 ;mv $HOME/.ssh/authorized_keys $HOME/.ssh/authorized_keys_old;mv $HOME/.ssh/authorized_keys2 $HOME/.ssh/authorized_keys';
    my $err = "000000";

    my @ips= Scramble::ClusterUtils::get_all_sercive_ips($config);
    foreach  (@ips) {
  #   $err = gearman_client($cmd, $_);
      system("scp -i ". $SKYBASEDIR."/ncc/etc/id_rsa " . $SKYBASEDIR. "/ncc/etc/cloud.cnf ". $_.":".$SKYBASEDIR."/ncc/etc");

   }

 #$cms="cat <<EOF_LO0 > /etc/sysconfig/network-scripts/ifcfg-lo:1
 #DEVICE=lo:1
 #IPADDR=192.168.0.10
 #NETMASK=255.255.255.255
 #NAME=loopback
 #ONBOOT=yes
 #EOF_LO0";

}




sub get_mysql_variables_diff($$){
 my $host=shift;
 my $host_info=shift;
 my $json = new JSON;
  

 $log->log_debug("[get_mysql_diff] Read variables diff for service ".$host,2,"write_config");

 my $command ="bash -c 'diff -B <($SKYBASEDIR/mariadb/bin/my_print_defaults  --defaults-file=$SKYBASEDIR/ncc/etc/".$host_info->{mysql_cnf}." mysqld | sort  | tr [:upper:] [:lower:] ) <( $SKYBASEDIR/mariadb/bin/my_print_defaults  --defaults-file=$SKYDATADIR/$host/my.sandbox.cnf mysqld | sort  | tr [:upper:] [:lower:] )' ";
 $command = $command . q% | grep '>' | grep -vE 'pid-file|datadir|basedir|port|server-id|tmpdir|tmpdir|socket|user' | sed 's/\> --//g' | awk -F'=' 'BEGIN { print "{"}  END { print "\"scramble\":\"\"}"}  {  print "\""$1"\":\""$2"\"," }'  | sed 's/""/"na"/g' | tr -d '\n' %;
 
 #my $result_json =Scramble::ClusterTransport::worker_node_command( $command, $host_info->{ip} ); 
 my  $result_json =  `$command`; 
  $result_json =~ s/\n//g; 
  
 #$log->log_json($mysql_variables,1);
    
 my $mysql_variables =  $json->allow_nonref->utf8->relaxed->escape_slash->loose
             ->allow_singlequote->allow_barekey->decode($result_json);
 
 return  $mysql_variables;
 
}

