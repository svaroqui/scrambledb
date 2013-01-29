#!../perl/bin/perl
use strict;
use Class::Struct;
use warnings FATAL => 'all';
use Sys::Hostname;
use Gearman::XS qw(:constants);
use Gearman::XS::Client;
use JSON;

use Scramble::Common::ClusterUtils;
use Scramble::Common::Config;


our $SKYBASEDIR            = $ENV{SKYBASEDIR};
our $SKYDATADIR            = $ENV{SKYDATADIR};
our $config                = new Scramble::Common::Config::;
$config->read($SKYBASEDIR."/ncc/etc/cloud.cnf");
$config->check('SANDBOX');

my $cloud = Scramble::Common::ClusterUtils::get_active_cloud($config);
my $sshkey = $SKYDATADIR . "/.ssh/" . $cloud->{public_key} ;



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
 




if ( Scramble::Common::ClusterUtils::is_ip_from_status_present($status,$self->{ip})==1) {
             print STDERR "Service Ip is found in status \n";   
             if ( Scramble::Common::ClusterUtils::is_ip_from_status_running($status,$self->{ip})==0) {
               my $status_instance='{"level":"instances","command":{"action":"status","group":"all","type":"all"},"cloud":'. $json_cloud_str. '}';
                print STDERR "debug cloud instance  : not running sending "  . $status_instance;
                
                my $cloud_status_json = worker_cloud_command($status_instance,$gearman_ip);
                print STDERR "debug cloud status : " .$cloud_status_json;
               
                my $cloud_status = $json_cloud->allow_nonref->utf8->relaxed->escape_slash->loose
            ->allow_singlequote->allow_barekey->decode($cloud_status_json);
                
                my $instance= Scramble::Common::ClusterUtils::get_instance_id_from_status_ip($cloud_status,$self->{ip});
               

               # my $instance= Scramble::Common::ClusterUtils::get_instance_id_from_status_ip($status,$self->{ip});
                
                # not running but present need to start 
              #  my $start_instance='{"level":"instances","command":{"action":"start","group":"'.$instance.'","type":"all"},"cloud":'. $json_cloud_str. '}';
              #  worker_cloud_command($start_instance,$gearman_ip);
                 
              }
         } else 
         {
              print STDERR "Service Ip is not found in status \n";
             #my $launch_instance='{"level":"instances","command":{"action":"launch","group":"ScrambleDB","type":"all","ip":"'.$self->{ip}.'"},"cloud":'. $json_cloud_str. '}';
             #worker_cloud_command($launch_instance,$gearman_ip);
                push  @{$todo->{actions}} , {
                    event_ip       => $self->{ip},
                    event_type     => "instances",
                    event_state    => "stopped" ,
                    do_level       => "services" ,
                    do_group       => $node,
                    do_action      => "start " 
                };     
              
                push  @{$todo->{actions}} , {
                    event_ip       => $self->{ip},
                    event_type     => "instances",
                    event_state    => "running" ,
                    do_level       => "services" ,
                    do_group       => $node,
                    do_action      => "bootstrap_ncc" 
                  };     
                   push  @{$todo->{actions}} , {
                    event_ip       => $self->{ip},
                    event_type     => "instances",
                    event_state    => "running" ,
                    do_level       => "services" ,
                    do_group       => $node,
                    do_action      => $cmd 
                  };       
                  
                  $json_todo =  $json_cloud->allow_blessed->convert_blessed->encode($todo);
                  print STDERR "Delayed actions :" . $json_todo ."\n";     
                  $memd->set( "actions",  $json_todo);
                  report_status( $self, $param,  "ER0017", $node );
                  return "ER0017";
            
         }