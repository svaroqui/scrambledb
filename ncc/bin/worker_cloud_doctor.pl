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
         
  
  my $status_instance='{"level":"instances","command":{"action":"status","group":"all","type":"all"},"cloud":'. $json_cloud_str. '}';
  print STDERR "debug cloud instance  : not running sending "  . $status_instance;

  my $cloud_status_json = worker_cloud_command($status_instance,"127.0.0.1:4731");
 
   $cloud_status_json= '{"instances_status":{"instances":'. $cloud_status_json ."}}";
 print STDERR "debug cloud status : " .$cloud_status_json;

  my $cloud_status = $json_cloud->allow_nonref->utf8->relaxed->escape_slash->loose->allow_singlequote->allow_barekey->decode($cloud_status_json);
                   

  
  my $command='{"level":"instances","command":{"action":"actions","group":"all","type":"all"}}';
  my $cluster_actions_json= worker_cluster_command($command,"localhost");    
  print $cluster_actions_json." \n\n";  
  my $cluster_actions = $json_cloud->allow_nonref->utf8->relaxed->escape_slash->loose
  ->allow_singlequote->allow_barekey->decode($cluster_actions_json);
  
   
  $command='{"level":"instances","command":{"action":"heartbeat","group":"all","type":"all"}}';
  my $cluster_status_json = worker_cluster_command($command,"localhost");    
  print $cluster_status_json." \n\n";

  my $status= $json_cloud->allow_nonref->utf8->relaxed->escape_slash->loose
  ->allow_singlequote->allow_barekey->decode($cluster_status_json);
   foreach  my $action (  @{ $cluster_actions->{actions}} ) {
    if  ( $action->{do_action}   eq "start" ) {
       print STDERR $action->{do_group}."\n";
       
        
       my $event_ip = Scramble::Common::ClusterUtils::get_service_ip_from_status_name($status,$action->{do_group});
        print STDERR $event_ip."\n";
        if ( Scramble::Common::ClusterUtils::is_ip_from_status_present($cloud_status,$event_ip)==1) {
              print STDERR "Service Ip is found in status \n";   
              if ( Scramble::Common::ClusterUtils::is_ip_from_status_running($status,$event_ip)==0) {
                 # not running but instance exist need to start 
                 my $status = Scramble::Common::ClusterUtils::get_instance_status_from_ip($cloud_status,$event_ip);
                 if( $status eq "stopped"){
                    my $instance= Scramble::Common::ClusterUtils::get_instance_id_from_status_ip($cloud_status,$event_ip);
                
                   my $start_instance='{"level":"instances","command":{"action":"start","group":"'.$instance.'","type":"all"},"cloud":'. $json_cloud_str. '}';
                   worker_cloud_command($start_instance,"127.0.0.1:4731");
                 }   
              }
          } else 
          {
              print STDERR "Service Ip is not found in status \n";
              my $launch_instance='{"level":"instances","command":{"action":"launch","group":"ScrambleDB","type":"all","ip":"'.$event_ip.'"},"cloud":'. $json_cloud_str. '}';
              worker_cloud_command($launch_instance,"127.0.0.1:4731");
          }      

      }              
  }
 
}


sub worker_cloud_command($$) {
    my $cmd    = shift;
    my $ip     = shift;
    my $client = Gearman::XS::Client->new();
    $client->add_servers($ip);
    print STDERR $ip . ' ' . $cmd . '\n';
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
    print STDERR $ip . ' ' . $cmd . '\n';
    #$client->set_timeout($gearman_timeout);
    #(my $ret,my $result) = $client->do_background('service_do_command', $cmd);
    ( my $ret, my $result ) = $client->do( 'cluster_cmd', $cmd );

    if ( $ret == GEARMAN_SUCCESS ) {
      if ( !defined $result ) {
        return "ER0006";
        print STDERR "Print no result ";
     } else { 
        return $result; 
     }
    } 
    else
    {
      print STDERR "Call to worker failed  ";
    }

   return "ER0006";
    
}


while ( 1 )  
{
   sleep $TIME;
   gearman_client();
 }
 


          
                