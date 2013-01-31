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

my $TIME=12;





sub gearman_client() {
  # get the status from my point of view  

  my $cloud = Scramble::Common::ClusterUtils::get_active_cloud($config);
  my $cloud_name = Scramble::Common::ClusterUtils::get_active_cloud_name($config);
  my $json_cloud       = new JSON ;
  my $json_cloud_str = $json_cloud->allow_nonref->utf8->encode($cloud);
         
  
  
  
  my $command='{"level":"instances","command":{"action":"actions","group":"all","type":"all"}}';
  my $cluster_actions_json= worker_cluster_command($command,"localhost"); 
  Scramble::Common::ClusterUtils::log_debug("Retriving delayed actions",1);
  Scramble::Common::ClusterUtils::log_json( $cluster_actions_json,1) ;
  if ('{"actions":[]}' eq $cluster_actions_json ){ return 0};
  my $cluster_actions = $json_cloud->allow_nonref->utf8->relaxed->escape_slash->loose
  ->allow_singlequote->allow_barekey->decode($cluster_actions_json);
  $command='{"level":"instances","command":{"action":"status","group":"all","type":"all"},"cloud":'. $json_cloud_str. '}';
  my $cloud_status_json = worker_cloud_command($command,"127.0.0.1:4731");
  $cloud_status_json= '{"instances_status":{"instances":'. $cloud_status_json ."}}";
  Scramble::Common::ClusterUtils::log_debug("Retriving cloud status",1);
  Scramble::Common::ClusterUtils::log_json( $cloud_status_json,1);
  my $cloud_status = $json_cloud->allow_nonref->utf8->relaxed->escape_slash->loose
  ->allow_singlequote->allow_barekey->decode($cloud_status_json);
  
   
  $command='{"level":"instances","command":{"action":"heartbeat","group":"all","type":"all"}}';
  my $cluster_status_json = worker_cluster_command($command,"localhost");    
  my $status= $json_cloud->allow_nonref->utf8->relaxed->escape_slash->loose
  ->allow_singlequote->allow_barekey->decode($cluster_status_json);
  
  foreach  my $action (  @{ $cluster_actions->{actions}} ) {
    if  ( $action->{do_action}   eq "start" ) {
        Scramble::Common::ClusterUtils::log_debug("Processing ".$action->{do_group} ,2); ;
        my $event_ip = Scramble::Common::ClusterUtils::get_service_ip_from_status_name($status,$action->{do_group});
        Scramble::Common::ClusterUtils::log_debug("Found event on ". $event_ip, 2);
        if ( Scramble::Common::ClusterUtils::is_ip_from_status_present($cloud_status,$event_ip)==1) {
              Scramble::Common::ClusterUtils::log_debug("Service IP is found in status of the cloud API ", 2);
                     
              if ( Scramble::Common::ClusterUtils::is_ip_from_status_running($status,$event_ip)==0) {
                 Scramble::Common::ClusterUtils::log_debug("Not running but instance exists ", 2);
                 my $state = Scramble::Common::ClusterUtils::get_instance_status_from_ip($cloud_status,$event_ip);
                 if( $state eq "stopped"){
                     Scramble::Common::ClusterUtils::log_debug("Instance is in state  stopped", 2); 
                   
                    my $instance= Scramble::Common::ClusterUtils::get_instance_id_from_status_ip($cloud_status,$event_ip);
                    
                   my $start_instance='{"level":"instances","command":{"action":"start","group":"'.$instance.'","type":"all"},"cloud":'. $json_cloud_str. '}';
                   worker_cloud_command($start_instance,"127.0.0.1:4731");
                 }  
                 else{
                   Scramble::Common::ClusterUtils::log_debug("Instance is not stopped may be starting", 1); 
                        
                 }
              }
          } else 
          {
              Scramble::Common::ClusterUtils::log_debug("Service Ip is not found in status launching instance", 1);
              my $launch_instance='{"level":"instances","command":{"action":"launch","group":"ScrambleDB","type":"all","ip":"'.$event_ip.'"},"cloud":'. $json_cloud_str. '}';
              worker_cloud_command($launch_instance,"127.0.0.1:4731");
              sleep 10;
          }      

      }              
  }
 
}


sub worker_cloud_command($$) {
    my $cmd    = shift;
    my $ip     = shift;
    my $client = Gearman::XS::Client->new();
    $client->add_servers($ip);
   
    Scramble::Common::ClusterUtils::log_debug( "[worker_cloud_command] ". $ip  ,1);
    Scramble::Common::ClusterUtils::log_json(  $cmd  ,1);
    
    $client->set_timeout(10000);
    #(my $ret,my $result) = $client->do_background('service_do_command', $cmd);
    ( my $ret, my $result ) = $client->do( 'cloud_cmd', $cmd );

    if ( $ret == GEARMAN_SUCCESS ) {
      if ( !defined $result ) {
        return "ER0006";
     } else { 
        return $result; 
     }
    
   }
   return "ER0006";
    
}


sub worker_cluster_command($$) {
    my $cmd    = shift;
    my $ip     = shift;
    my $client = Gearman::XS::Client->new();
    $client->add_servers($ip);
    Scramble::Common::ClusterUtils::log_debug( "[worker_cluster_command] ". $ip  ,1);
    Scramble::Common::ClusterUtils::log_json(  $cmd  ,1);
    #$client->set_timeout($gearman_timeout);
    #(my $ret,my $result) = $client->do_background('service_do_command', $cmd);
    ( my $ret, my $result ) = $client->do( 'cluster_cmd', $cmd );

    if ( $ret == GEARMAN_SUCCESS ) {
      if ( !defined $result ) {
        return "ER0006";
        Scramble::Common::ClusterUtils::log_debug( "[worker_cluster_command] ". "Print no result ",1);
     } else { 
        return $result; 
     }
    } 
    else
    {
      Scramble::Common::ClusterUtils::log_debug( "[worker_cluster_command] ".  "Call to worker failed ",1) ;
    }

   return "ER0006";
    
}


while ( 1 )  
{
   sleep $TIME;
   gearman_client();
 }
 


          
                