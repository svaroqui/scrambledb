#!/usr/bin/env perl
use strict;
use Class::Struct;
use warnings FATAL => 'all';
use Common::Config;
use Sys::Hostname;
use Gearman::XS qw(:constants);
use Gearman::XS::Client;
use Gearman::XS::Worker;
use JSON;
use Cache::Memcached;
use DBI;
use Data::Dumper;

struct 'host' => {
    name			=> '$',
    ip                          => '$',
    port			=> '$',
    peer			=> '$',
    mode			=> '$',
    replication_user            => '$',
    replication_password	=> '$',
    mysql_port                  => '$',
    mysql_user          	=> '$',
    mysql_password		=> '$',
    mysql_version		=> '$',
    mysql_cnf                   => '$',
    datadir                     => '$'
};

struct 'nosql' => {
    name			=> '$',
    ip                          => '$',
    mode			=> '$',
    port                  => '$'
};


our  %ERRORMESSAGE = (
    "000000" => "OK",
    "ER0001" => "SQL command failure",
    "ER0002" => "Remote manager communication failure ",
    "ER0003" => "Database communication failure ",
    "ER0004" => "Remote manager command failure ",
    "ER0005" => "Memcache communication failure "
);

our $hashcolumn;
our $createtable = "";
our $like = "none";
our $database = "";
our $SKYBASEDIR = $ ENV {SKYBASEDIR};
our $SKYDATADIR = $ ENV {SKYDATADIR};
our $gearman_timeout = 2000;
our $mysql_connect_timeout=3;
our $console = "{result:{status:'00000'}}";
our $config = new SKY::Common::Config::;
my $conf="etc/cloud.cnf";
$config->read($conf);
$config->check('SANDBOX');
open my $LOG , q{>>},  $SKYDATADIR."/log/worker_write_config.log"
or die "can't create 'worker_write_config.log'\n";


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
    my $action;
    my $group;
    my $type;
    my $query;
    my $table = "";
    my $command = $job->workload();
    my $json = new JSON;
    my $json_text = $json->allow_nonref->utf8->relaxed->escape_slash->loose->allow_singlequote->allow_barekey->decode($command);
    my $peer_host_info = "";
    my $myhost = hostname;
    my $ddlallnode = 0;
    my $ddlallproxy = 0;
    my $ret = "true";
    $console= "";

    $config->read("etc/cloud.cnf");
    $config->check('SANDBOX');
    my $json2 = new JSON;
    my $json_config   = $json2->allow_blessed->convert_blessed->encode($config );
    $Data::Dumper::Terse = 1;        # don't output names where feasible
    $Data::Dumper::Quotekeys = 0;
    $Data::Dumper::Indent = 1;       # mild pretty print
    $Data::Dumper::Pair = " = ";
    $Data::Dumper::Indent = 2;
    $Data::Dumper::Useqq = 0;



  #  print  STDERR Dumper($config);
  #  print  STDERR $json_config;




    print STDERR "Receive command : $command\n";
    $action = $json_text->{command}->{action};
    $group = $json_text->{command}->{group};
    $type = $json_text->{command}->{type};
    $query = $json_text->{command}->{query};
    $database = $json_text->{command}->{database};
    write_mysql_proxy_config($SKYBASEDIR."/ncc/etc/mysql-proxy");
    print STDOUT "write mysql_proxy config\n"; 
    write_mha_config($SKYBASEDIR. "/ncc/etc/mha.cnf");
    print STDOUT "write mha config\n";  
    write_keepalived_config($SKYBASEDIR. "/ncc/etc/keepalived");
    print STDOUT "write keepalived config\n";  
    write_haproxy_config($SKYBASEDIR."/ncc/etc/haproxy");
    print STDOUT "write haproxy config\n";  
    write_memcached_config($SKYBASEDIR."/ncc/etc/memcached");
    print STDOUT "write memcached config\n"; 
    write_lua_variables_config($SKYBASEDIR."/ncc/etc/lua_variables_config");
    print STDOUT "write lua script include config\n"; 
    return  $console ;

}



sub replace_config($$$){
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



sub write_keepalived_config($){
my $file= shift;
my $i=100;
 my $lb_info;
foreach my $lb (keys(%{$config->{lb}})) {

    $lb_info = $config->{lb}->{default};
    $lb_info = $config->{lb}->{$lb};
    if  ( $lb_info->{mode} eq "keepalived"){
    open my $out, '>', "$file.$lb.cnf" or die "Can't write new file: $!";

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
    print $out "  # The default state, one should be master, the others should be set to SLAVE.\n";
    print $out "  state MASTER\n";
    print $out "\n";
    print $out " # This should be the same on all participating load balancers.\n";
    print $out "  virtual_router_id 1\n";
    print $out "\n";
    print $out "  priority ". $i."\n";
    print $out "\n";
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
     print $out " }\n";
     }
     close $out;
     $i++;

    }
    }

}
sub write_lua_variables_config($){
my $file= shift;
my $i=100;
my $nosql_info; 


foreach my $nosql (keys(%{$config->{nosql}})) {
    $nosql_info = $config->{nosql}->{default};
    $nosql_info = $config->{nosql}->{$nosql};
    if  ( $nosql_info->{status} eq "master"){
         open my $out, '>', "$file" or die "Can't write new file: $!";
         system("export MEMCACHE_HOST='". $nosql_info->{ip} ."' && export MEMCACHE_PORT=". $nosql_info->{port}   );
        print $out "local memcache_master=\"" . $nosql_info->{ip} ."\"\n"; 
        print $out "local memcache_port= " . $nosql_info->{port} ."\n"; 
         close $out;
         return 1; 
    }
   }
    


  return 0; 
}
sub write_haproxy_config($){
my $file= shift;
my $i=100;
 my $lb_info;
foreach my $lb (keys(%{$config->{lb}})) {

    $lb_info = $config->{lb}->{default};
    $lb_info = $config->{lb}->{$lb};
    if  ( $lb_info->{mode} eq "haproxy"){
    open my $out, '>', "$file.$lb.cnf" or die "Can't write new file: $!";
    print $out "global\n";
    print $out "   log         127.0.0.1 local2\n";
    #print $out "   chroot      ".."/haproxy\n";
    # print $out "   pidfile     ".$SKYBASEDIR."/ncc/tmp/haproxy.pid\n";
    print $out "   maxconn     4000\n";
    #print $out "   user        ".."\n";
    #print $out "   group       haproxy\n";
    print $out "   daemon\n";
    print $out "   # turn on stats unix socket\n";
    print $out "   stats socket ".$SKYBASEDIR."/ncc/tmp/stats\n";
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
    print $out "frontend mysql-front\n";
    print $out "mode tcp\n";
    print $out "bind     ".$lb_info->{vip}.":".$lb_info->{port}."\n";
    print $out "default_backend  mysql-back\n";
    #print $out "frontend web-front\n";
    #print $out "mode tcp\n";
    #print $out "bind   ".$lb_info->{vip}.":80\n";
    #print $out "default_backend  htp-back\n";
    print $out "#---------------------------------------------------------------------\n";
    print $out "# round robin balancing between the various backends\n";
    print $out "#---------------------------------------------------------------------\n";
    print $out "backend mysql-back\n";
    print $out "  balance     roundrobin\n";
    print $out "  mode    tcp\n";
    my $host_info;
    foreach my $host (keys(%{$config->{proxy}})) {
        $host_info = $config->{proxy}->{default};
        $host_info = $config->{proxy}->{$host};
        print $out "   server $host ". $host_info->{ip}.":". $host_info->{port} . "  check\n";

     }
      close $out;
      system("chmod 660 $file.$lb.cnf " );
     $i++;

  }
 }
}

sub write_memcached_config($){
    my $file = shift;
    my $masters = list_masters();
    my $slaves = list_slaves() ;

    my $i=100;
    my $nosql;
    foreach my $host (keys(%{$config->{nosql}})) {
        $nosql = $config->{nosql}->{default};
        $nosql = $config->{nosql}->{$host};
        if  ( $nosql->{mode} eq "memcache"){
        open my $out, '>', "$file.$host.cnf" or die "Can't write new file: $!";
        print $out "-m ".$nosql->{mem}."\n";
        print $out "# default port\n";
        print $out "-p ".$nosql->{port}."\n";
        print $out "# user to run daemon nobody/apache/www-data\n";
        print $out "-u skysql\n";
        print $out "-l ".$nosql->{ip}."\n";
        print $out "-P ".$SKYBASEDIR."/ncc/tmp/memcached.". $host . ".pid\n";

        close $out;
       $i++;
       system("chmod 660 $file.$host.cnf" );
    }
  }

  # replace_config($file ,"proxy-read-only-backend-addresses","proxy-read-only-backend-addresses=". $slaves);
  # replace_config($file ,"proxy-backend-addresses","proxy-backend-addresses=".  $masters );
 }

sub write_mysql_proxy_config($){
my $file = shift;
my $masters = list_masters();
my $slaves = list_slaves() ;

my $i=100;
my $host_info;
foreach my $host (keys(%{$config->{proxy}})) {
    $host_info = $config->{proxy}->{default};
    $host_info = $config->{proxy}->{$host};
    open my $out, '>', "$file.$host.cnf" or die "Can't write new file: $!";

    print $out "[mysql-proxy]\n";
    print $out "pid-file = mysql-proxy.pid\n";
    print $out "log-file = $SKYBASEDIR/ncc/log/mysql-proxy.log\n";
    print $out "log-level = debug\n";
   # print $out "plugin-dir=lib/mysql-proxy/plugins\n";
   # print $out "admin-username=". $host_info->{admin_user}." \n";
   # print $out "admin-password=". $host_info->{admin_password}." \n";
   # print $out "admin-lua-script=lib/mysql-proxy/lua/admin.lua\n";
    print $out "proxy-address =". $host_info->{ip}.":". $host_info->{port} ."\n";
    print $out "proxy-backend-addresses=".$masters."\n";
    print $out "proxy-read-only-backend-addresses=". $slaves."\n";
  # print $out "proxy-lua-script = ../ncc/scripts/interceptor.lua\n";
    print $out "proxy-lua-script = $SKYBASEDIR/ncc/scripts/". $host_info->{script} ."\n";
     close $out;
 $i++;
system("chmod 660 $file.$host.cnf" );
}


# replace_config($file ,"proxy-read-only-backend-addresses","proxy-read-only-backend-addresses=". $slaves);
# replace_config($file ,"proxy-backend-addresses","proxy-backend-addresses=".  $masters );
}

sub write_mha_config($){
  my $file = shift;
  my $host_info ;
  my $err = "000000";
  my $i=1;

  open my $out, '>', "$file.new" or die "Can't write new file: $!";
  foreach my $host (keys(%{$config->{db}})) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        if ($host_info->{mode} eq "master" || $host_info->{mode} eq "slave") {
            print $out "[server$i]\n";
            print $out "hostname=$host_info->{ip}\n";
            print $out "ip=$host_info->{ip}\n";
            print $out "port=$host_info->{mysql_port}\n";
            print $out "repl_user=$host_info->{mysql_user}\n";
            print $out "repl_password=$host_info->{mysql_password}\n";
            print $out "user=$host_info->{mysql_user}\n";
            print $out "password=$host_info->{mysql_password}\n";
            print $out "remote_workdir=$host_info->{datadir}/mha\n";
            print $out "master_binlog_dir=$host_info->{datadir}/sandboxes/$host/data\n";
            print $out "ssh_options=\"-i $SKYBASEDIR/ncc/etc/id_rsa\"\n";
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

sub list_slaves(){
  my $host_info ;
    my $err = "000000";
   my @slaves;
  foreach my $host (keys(%{$config->{db}})) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        if ($host_info->{mode} eq "slave") {
              push(@slaves  , $host_info->{ip}.":". $host_info->{mysql_port});
         }
    }

    return join(',',@slaves) ;
}
sub list_masters(){
  my $host_info ;
    my $err = "000000";
    my @masters;
    foreach my $host (keys(%{$config->{db}})) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        if ($host_info->{mode} eq "master") {
              push(@masters  , $host_info->{ip}.":". $host_info->{mysql_port});
         }
    }
    return join(',',@masters) ;
}

sub get_master_host(){
  my $host_info ;
    my $err = "000000";
    my @masters;
    foreach my $host (keys(%{$config->{db}})) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        if ($host_info->{mode} eq "master") {
            return $host_info;
        }
    }
    return 0 ;
}


sub list_memcaches(){
  my $host_info ;
  my $err = "000000";
  my @memcaches;
  foreach my $host (keys(%{$config->{db}})) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        if ($host_info->{mode} eq "memcache") {
              push(@memcaches  , $host_info->{ip}.":". $host_info->{mysql_port});
         }
    }
return join(',',@memcaches) ;
}




sub all_ips(){
  my $host_info ;
  my $err = "000000";
  my @ips;
  foreach my $host (keys(%{$config->{db}})) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        push(@ips  , $host_info->{ip});
    }
    return uniq(@ips) ;
}


sub bootstrap(){
    my $my_home_user = $ ENV {HOME};
    my $cmd='string="`cat '.$SKYBASEDIR.'/ncc/etc/id_rsa.pub`"; sed -e "\|$string|h; \${x;s|$string||;{g;t};a\\" -e "$string" -e "}" $HOME/.ssh/authorized_keys > $HOME/.ssh/authorized_keys2 ;mv $HOME/.ssh/authorized_keys $HOME/.ssh/authorized_keys_old;mv $HOME/.ssh/authorized_keys2 $HOME/.ssh/authorized_keys';
    my $err = "000000";

    my @ips= all_ips();
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







