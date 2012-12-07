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
my $interface;
my %IPs;
foreach (qx{ (LC_ALL=C /sbin/ifconfig -a 2>&1) }) {
    $interface = $1 if /^(\S+?):?\s/;
    next unless defined $interface;
    $IPs{$interface}->{STATE} = uc($1) if /\b(up|down)\b/i;
    $IPs{$interface}->{IP}    = $1     if /inet\D+(\d+\.\d+\.\d+\.\d+)/i;
}


open my $LOG , q{>>}, $SKYDATADIR."/log/hearbeat.log"
    or die "can't create 'ncc.log'\n";



my $config_file = "etc/cloud.cnf";


my $TIME=10;
my $client = Gearman::XS::Client->new();
$client->add_servers("localhost");
 
sub gearman_client() {
# get the status from my point of view  
  my $command="{command:{action:'status',group:'all',type:'all'}}";
  (my $ret,my $result) = $client->do('cluster_cmd', $command);   
  if ($ret != GEARMAN_SUCCESS) {
        printf(STDOUT "%s\n", $client->error());    
           return 0;
  }
 system("system_profiler SPHardwareDataType | grep -i memory  | awk  '{print \$2*1024*1024*1024}'"); 
 # system(`cat /proc/meminfo |  grep "MemTotal" | awk '{print \$2}'`); 
  
my $ram =$? ;

$command="{command:{action:'ping',group:'all',type:'db'},data:{ram:'$ram'},hearbeat:$result}";
  
( $ret, $result) = $client->do('cluster_cmd', $command);
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
 

