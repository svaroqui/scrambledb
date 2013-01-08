#!../perl/bin/perl
use strict;
use Class::Struct;
use warnings FATAL => 'all';
use Sys::Hostname;
use Gearman::XS qw(:constants);
use Gearman::XS::Client;
use JSON;

our $SKYDATADIR = $ ENV {SKYDATADIR};

# perl -le 'BEGIN{use Linux::MemInfo} %mem=get_mem_info; print $mem{"MemTotal"}'



open my $LOG , q{>>}, $SKYDATADIR."/log/hearbeat.log"
    or die "can't create 'ncc.log'\n";



my $config_file = "etc/cloud.cnf";


my $TIME=10;
my $client = Gearman::XS::Client->new();
$client->add_servers("localhost");
 
sub gearman_client() {
# get the status from my point of view  
  my $command='{"level":"services","command":{"action":"status","group":"all","type":"all"}}';
  (my $ret,my $result_services_status) = $client->do('cluster_cmd', $command);   
  if ($ret != GEARMAN_SUCCESS) {
        printf(STDOUT "ERROR GEARMAN %s\n", $client->error());    
           return 0;
  }
 # system("system_profiler SPHardwareDataType | grep -i memory  | awk  '{print \$2*1024*1024*1024}'"); 

  $command='{"level":"instances","command":{"action":"status","group":"all","type":"all"}}';
  ($ret,my $result_instances_status) = $client->do('cluster_cmd', $command);   
  if ($ret != GEARMAN_SUCCESS) {
        printf(STDOUT "ERROR GEARMAN %s\n", $client->error());    
           return 0;
  }

  system("cat /proc/meminfo |  grep MemTotal | awk '{print \$2}'"); 
  my $ram =$? ;
    


   my $interface;
   my %IPs;
    foreach (qx{ (LC_ALL=C /sbin/ifconfig -a 2>&1) }) {
        $interface = $1 if /^(\S+?):?\s/;
        next unless defined $interface;
        $IPs{$interface}->{STATE} = uc($1) if /\b(up|down)\b/i;
        $IPs{$interface}->{STATE} =defined( $IPs{$interface}->{STATE}) ?  $IPs{$interface}->{STATE} : "na";
        $IPs{$interface}->{IP}    = $1     if /inet\D+(\d+\.\d+\.\d+\.\d+)/i; 
        $IPs{$interface}->{IP} =defined( $IPs{$interface}->{IP}) ?  $IPs{$interface}->{IP} : "na";
    }
  my $json       = new JSON ;
  my $json_interfaces = $json->allow_nonref->utf8->encode(\%IPs);
  
#my $json2 = encode_json \%IPs; 
# print    $json2;

$command='{"level":"services","command":{"action":"ping","group":"all","type":"db"},"host":{"ram":"'.$ram. '","interfaces":['. $json_interfaces .']},"services_status":'.$result_services_status.',"instances_status":'.$result_instances_status.'}';
  
( $ret,my  $result) = $client->do('cluster_cmd', $command);
    printf(STDOUT "%s\n", $command);
    if ($ret != GEARMAN_SUCCESS) {
        printf(STDOUT "%s\n", $client->error());
    }
    else {
        printf(STDOUT "%s\n",  $ret);
	printf(STDOUT "%s\n",  $result);
    }
}

while ( 1 )  
{
   sleep $TIME;
   gearman_client();
 }
 

