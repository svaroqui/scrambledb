#!../perl/bin/perl
# Copyright (c) 2013 Stephane VAROQUI http://skysql.com/
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish, dis-
# tribute, sublicense, and/or sell copies of the Software, and to permit
# persons to whom the Software is furnished to do so, subject to the fol-
# lowing conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABIL-
# ITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
# SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE.


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
my $ssh_error_retry=0;
my $max_ssh_error_retry=2;

sub cloud_create_tunnel(){
    Scramble::Common::ClusterUtils::log_debug("[cloud_create_tunnel] Info: Create tunnel to elastic ip",2);
    my $cloud = Scramble::Common::ClusterUtils::get_active_cloud($config);
    my $sshkey = $SKYDATADIR . "/.ssh/" . $cloud->{public_key} ;
    my $cmd= "ssh -f -o ConnectTimeout=2 -i ". $sshkey . " -L 4730:127.0.0.1:4730 ".  $cloud->{elastic_ip} . " -N < /dev/null > ".$SKYDATADIR. "/log/tunnel.log 2>&1 & ";

    system ($cmd);
    $cmd='ps ax|grep ssh|grep 4730|grep 4730 |awk "{ print  $1 }" | head -n1';
    my $pid=`$cmd`;
    system ("echo '". $pid ."' > ". $SKYDATADIR ."/tmp/tunnel.pid ");
}


sub cloud_switch_elastic_ip(){
    Scramble::Common::ClusterUtils::log_debug("[cloud_switch_elastic_ip] Info: Create tunnel to elastic ip",2);
    my $cloud = Scramble::Common::ClusterUtils::get_active_cloud($config);
    
}

sub cloud_is_scramble_running(){
    my $cloud = Scramble::Common::ClusterUtils::get_active_cloud($config);
    my $sshkey = $SKYDATADIR . "/.ssh/" . $cloud->{public_key} ;
    
    my $command="ssh -q -i "
    . $sshkey. " "
    . ' -o "BatchMode=yes"  -o "StrictHostKeyChecking=no" '
    . $cloud->{elastic_ip} 
    .' "ps -ef | grep gearmand | grep -vc grep "';
    my  $result = `$command`;
    Scramble::Common::ClusterUtils::log_debug("[cloud_is_scramble_running] Info: Checking ScrambleDB ". $cloud->{elastic_ip},2);
     
 
   $result =~ s/\n//g; 
     if ( $result eq 1){ 
         Scramble::Common::ClusterUtils::log_debug("[cloud_is_scramble_running] Info: ScrambleDB is running ". $cloud->{elastic_ip},2);
         return 1;
    }
    Scramble::Common::ClusterUtils::log_debug("[cloud_is_scramble_running] Info: ScrambleDB is not running ".$cloud->{elastic_ip},2);
   
    return 0;
}


sub cloud_is_eth1_up(){
    my $cloud = Scramble::Common::ClusterUtils::get_active_cloud($config);
    my $sshkey = $SKYDATADIR . "/.ssh/" . $cloud->{public_key} ;
    
    my $command="ssh -q -i "
    . $sshkey. " "
    . ' -o "BatchMode=yes"  -o "StrictHostKeyChecking=no" '
    . $cloud->{elastic_ip} 
    .' "ifconfig | grep eth1 | grep -vc grep "';
    my  $result = `$command`;
    Scramble::Common::ClusterUtils::log_debug("[cloud_is_eth1_up] Info: Checking eth1 ". $cloud->{elastic_ip},2);
     
 
   $result =~ s/\n//g; 
     if ( $result eq 1){ 
         Scramble::Common::ClusterUtils::log_debug("[cloud_is_eth1_up] Info: eth1 is ip ". $cloud->{elastic_ip},2);
         return 1;
    }
    Scramble::Common::ClusterUtils::log_debug("[cloud_is_scramble_running] Info: eth1 is not up ".$cloud->{elastic_ip},2);
   
    return 0;
}

sub cloud_start_scramble(){
    
    my $cloud = Scramble::Common::ClusterUtils::get_active_cloud($config);
    my $sshkey = $SKYDATADIR . "/.ssh/" . $cloud->{public_key} ;
   Scramble::Common::ClusterUtils::log_debug("[cloud_start_scramble] Info: starting ScrambleDB on ip : ".$cloud->{elastic_ip},2);
   
    my $command="ssh -q -i "
    . $sshkey. " "
    . ' -o "BatchMode=yes"  -o "StrictHostKeyChecking=no" '
    . $cloud->{elastic_ip} 
    .' "$SKYBASEDIR/ncc/init.d/clusterd start"';
    my  $result = `$command`;
 
    return 0;
}
sub cloud_check_ssh_tunnel(){
    my $command='ps ax |grep ssh |grep "4730" | grep -vc grep';
    my  $result = `$command`;
    Scramble::Common::ClusterUtils::log_debug("[cloud_check_ssh_tunnel] Info: Checking Tunnel ",2);
     
 
   $result =~ s/\n//g; 
     if ( $result eq 1){ 
         Scramble::Common::ClusterUtils::log_debug("[cloud_check_ssh_tunnel] Info: Tunnel is running ",2);
         return 1;
    }
    Scramble::Common::ClusterUtils::log_debug("[cloud_check_ssh_tunnel] Info: Tunnel is not running ",2);
    return 0;
}
sub cloud_check_ssh(){
    my $cloud = Scramble::Common::ClusterUtils::get_active_cloud($config);
    my $sshkey = $SKYDATADIR . "/.ssh/" . $cloud->{public_key} ;

    my $command="ssh -q -i "
    . $sshkey. " "
    . ' -o "BatchMode=yes"  -o "StrictHostKeyChecking=no" -o "ConnectTimeout=2" '
    . $cloud->{elastic_ip} 
    .' "echo 2>&1" && echo "OK" || echo "NOK"';
    Scramble::Common::ClusterUtils::log_debug("[cloud_check_ssh] Info: Checking  : ".$command,2);
     
 
    my  $result = `$command`;
     
 
   $result =~ s/\n//g; 
     if ( $result eq "OK"){ 
         Scramble::Common::ClusterUtils::log_debug("[cloud_check_ssh] Info: ssh ok on elastic ip : ". $cloud->{elastic_ip},2);
         return 1;
    }
    Scramble::Common::ClusterUtils::log_debug("[cloud_check_ssh] Info: ssh failed on elastic ip  : ". $cloud->{elastic_ip},2);
    return 0;
}
sub  cloud_is_memcache_running(){
# $command='{"level":"services","command":{"action":"start","group":"local","type":"memcache"}}';
# my $cluster_memcache_status_json = worker_cluster_command($command,"localhost");  
# if (  $cluster_memcache_status_json eq '{"return":"ER0006"}') { return 0 };
#  my $json      = new JSON;
#  my @perl_class = $json->allow_nonref->utf8->relaxed->escape_slash->loose->allow_singlequote->allow_barekey->decode($json_text);
 my $cloud = Scramble::Common::ClusterUtils::get_active_cloud($config);
    my $sshkey = $SKYDATADIR . "/.ssh/" . $cloud->{public_key} ;
    
    my $command="ssh -q -i "
    . $sshkey. " "
    . ' -o "BatchMode=yes"  -o "StrictHostKeyChecking=no" '
    . $cloud->{elastic_ip} 
    .' "ps -ef | grep -i memcached | grep -vc grep "';
    my  $result = `$command`;
    Scramble::Common::ClusterUtils::log_debug("[cloud_is_memcache_running] Info: Checking Memcache ". $cloud->{elastic_ip},2);
     
 
   $result =~ s/\n//g; 
     if ( $result eq 1){ 
         Scramble::Common::ClusterUtils::log_debug("[cloud_is_memcache_running] Info: Memcache is running ". $cloud->{elastic_ip},2);
         return 1;
    }
    Scramble::Common::ClusterUtils::log_debug("[cloud_is_memcache_running] Info: Memcache is not running ".$cloud->{elastic_ip},2);
   
    return 0;

}

sub cloud_have_actions($){
 my $actions_message=shift; 
  
 my $count= scalar(@{$actions_message->{actions} } );
  if (defined($actions_message->{return}->{code})) {
    
     if ( $actions_message->{return}->{code}  ne "000000") {
        Scramble::Common::ClusterUtils::log_debug("[cloud_have_actions] Info: Found Errors in  actions",1);        
               return 0;

      } 
  }  
  Scramble::Common::ClusterUtils::log_debug("[cloud_have_actions] Info: Found ".$count . " actions",1);   
    
 if ($count==0) {
    return 0; 
 } 
 return 1;   
}


sub cloud_check_return_error($){
 my $result=shift; 
 my $json      = new JSON;
 print STDERR $result;
 my $perl_class = $json->allow_nonref->utf8->relaxed->escape_slash->loose->allow_singlequote->allow_barekey->decode($result);
 if (defined($perl_class->{return}->{code})) {
      if ( $perl_class->{return}->{code}  eq "000000") {
          return 0;
      } 
      return 1;
 } else {
   return 0;
 }  
}

sub cloud_get_status_from_json_interfaces($$) {
    my $cloud_status_json=shift;
    my $ip=shift;
    my $json = new JSON;
    my $status = $json->allow_nonref->utf8->relaxed->escape_slash->loose
      ->allow_singlequote->allow_barekey->decode($cloud_status_json);
  
    
    my $name =shift;
     foreach  my $instance (  @{ $status->{interfaces_status}->{interfaces}} ) {
        foreach my $key (keys %$instance) {
        print STDERR $key;
        my $instance_info=$instance->{$key};
         if ( $instance_info->{ip} eq $ip)  { 
                  return $instance_info->{status};
          }    
       }
      }
     return 0; 

}

sub cloud_get_instance_id_from_json_interfaces($$) {
    my $cloud_status_json=shift;
    my $ip=shift;
    my $json = new JSON;
    my $status = $json->allow_nonref->utf8->relaxed->escape_slash->loose
      ->allow_singlequote->allow_barekey->decode($cloud_status_json);
  
    
    my $name =shift;
     foreach  my $instance (  @{ $status->{interfaces_status}->{interfaces}} ) {
        foreach my $key (keys %$instance) {
        print STDERR $key;
        my $instance_info=$instance->{$key};
         if ( $instance_info->{ip} eq $ip)  { 
                  return $instance_info->{instance_id};
          }    
       }
      }
     return 0; 

}


sub cloud_get_instance_id_from_eip($){
     my $cloud_eip_json=shift;
     my $vip=shift;
     
     Scramble::Common::ClusterUtils::log_debug("[cloud_get_instance_id_from_eip] getting elastic ip status",1); 
    Scramble::Common::ClusterUtils::log_json( $cloud_eip_json,1);  
    my $json = new JSON;
    my $status = $json->allow_nonref->utf8->relaxed->escape_slash->loose
      ->allow_singlequote->allow_barekey->decode($cloud_eip_json);
    foreach  my $instance (  @{ $status->{eips}} ) {
        foreach my $key (keys %$instance) {
        print STDERR $key;
        my $instance_info=$instance->{$key};
        return $instance_info->{instance_id} ;
                  
            
       }
      }
     return "na"; 
    
}
sub is_interface_attach_to_elastic_ip($$){
    my $cloud_eip_json=shift;
    my $instance_id= shift ;
    Scramble::Common::ClusterUtils::log_debug("[is_interface_attach_to_elastic_ip] getting elastic ip status",1); 
    Scramble::Common::ClusterUtils::log_json( $cloud_eip_json,1);  
     my $json = new JSON;
    my $status = $json->allow_nonref->utf8->relaxed->escape_slash->loose
      ->allow_singlequote->allow_barekey->decode($cloud_eip_json);
    
     foreach  my $instance (  @{ $status->{eips}} ) {
        foreach my $key (keys %$instance) {
        print STDERR $key;
        my $instance_info=$instance->{$key};
         if ( $instance_info->{instance_id} eq $instance_id)  { 
                  return 1;
          }    
       }
      }
     return 0; 

    
}

sub cloud_vip_fix_route($){
  my $json_cloud_str=shift;
  my $err   = "000000";  
  my $gateway= "10.0.0.1"; 
  my $lb = Scramble::Common::ClusterUtils::get_active_lb($config);
  Scramble::Common::ClusterUtils::log_debug("[cloud_vip_fix_route] Info: Fixing route to the vip",1); 
  
  Scramble::Common::ClusterUtils::log_debug("[cloud_vip_fix_route] Info:check status of interface",1);
 
  my $command='{"level":"instances","command":{"action":"status_vip","group":"all","type":"all"},"cloud":'. $json_cloud_str. '}';
  my $cloud_interfaces_json = worker_cloud_interface_status($command,"127.0.0.1:4731");
  Scramble::Common::ClusterUtils::log_json( $cloud_interfaces_json,1);  
  my $vip_status=cloud_get_status_from_json_interfaces($cloud_interfaces_json,$lb->{vip}) ; 
  my $vip_instance_id= cloud_get_instance_id_from_json_interfaces($cloud_interfaces_json,$lb->{vip}) ;   
 Scramble::Common::ClusterUtils::log_debug("[cloud_vip_fix_route] Info: Interface is atache to instance ". $vip_instance_id,1);
  
 $command='{"level":"instances","command":{"action":"status_eip","group":"all","type":"all"},"cloud":'. $json_cloud_str. '}';
 my $cloud_eip_json = worker_cloud_eip_status($command,"127.0.0.1:4731"); 
      


  if ($vip_status eq "in-use")  { 
       Scramble::Common::ClusterUtils::log_debug("[cloud_vip_fix_route] Info: Interface is already used",1);
       if (is_interface_attach_to_elastic_ip($cloud_eip_json,$vip_instance_id)==1){
           Scramble::Common::ClusterUtils::log_debug("[cloud_vip_fix_route] Info: Interface attached to elastic ip  ",1);
           Scramble::Common::ClusterUtils::log_debug("[cloud_vip_fix_route] Info: try to test the interface ",1);
           if ( cloud_is_eth1_up()==0){
            $command="ifup eth1";
            worker_node_command($command,"localhost"); 
           } 
        }        
        else
        {
           Scramble::Common::ClusterUtils::log_debug("[cloud_vip_fix_route] Info: Interface not  attached  to elastic ip  ",1);
           Scramble::Common::ClusterUtils::log_debug("[cloud_vip_fix_route] Info: detach the interface ",1);
           
        }
  } else {
    Scramble::Common::ClusterUtils::log_debug("[cloud_vip_fix_route] Info: Interface not attached try attaching",1); 
     my $eip_instance_id= cloud_get_instance_id_from_eip($cloud_eip_json);
     $command='{"level":"instances","command":{"action":"attach_vip","group":"'. $eip_instance_id.'","type":"all"},"cloud":'. $json_cloud_str. '}';
     my $ret = worker_cloud_command($command,"127.0.0.1:4731");
  }  
  my $cmd1="ip route add default via $gateway dev eth0 tab 1";
  worker_node_command($cmd1,"localhost");  
  
  $cmd1="ip route add default via $gateway dev eth1 tab 2";
  worker_node_command($cmd1,"localhost");  
  
  $cmd1="ip rule add from $lb->{vip} tab 1 priority 500";
  worker_node_command($cmd1,"localhost");  
  
  $cmd1="ip rule add from $lb->{vip} tab 2 priority 800";
  worker_node_command($cmd1,"localhost");  
  
  $cmd1="ip route flush cache" ; 
  worker_node_command($cmd1,"localhost");  
  
  $cmd1="echo 2 > /proc/sys/net/ipv4/conf/eth1/rp_filter";
  worker_node_command($cmd1,"localhost");  
   
  
  return  $err;
  

}

sub cloud_get_a_running_lb(){
  

  my $cloud = Scramble::Common::ClusterUtils::get_active_cloud($config);
  my $json_cloud       = new JSON ;
  my $json_cloud_str = $json_cloud->allow_nonref->utf8->encode($cloud);
   
  Scramble::Common::ClusterUtils::log_debug("[cloud_get_a_running_lb] Info: Try to found a running lb"  ,1);
  my $command='{"level":"instances","command":{"action":"status","group":"all","type":"all"},"cloud":'. $json_cloud_str. '}';
  my $cloud_status_json = worker_cloud_status($command,"127.0.0.1:4731");

  Scramble::Common::ClusterUtils::log_debug("[cloud_get_a_running_lb] Info: Retriving cloud status",1);
  Scramble::Common::ClusterUtils::log_json( $cloud_status_json,1);
  my $cloud_status = $json_cloud->allow_nonref->utf8->relaxed->escape_slash->loose
  ->allow_singlequote->allow_barekey->decode($cloud_status_json);
  my $host_info; 
    my $cloud_name = Scramble::Common::ClusterUtils::get_active_cloud_name($config);
    foreach my $lb ( keys( %{ $config->{lb} } ) ) {
       if ($lb ne "default" ) {
        $host_info = $config->{lb}->{default};
        $host_info = $config->{lb}->{$lb};
        if ( $host_info->{mode} eq "keepalived" && $host_info->{cloud} eq $cloud_name) {
          if ( Scramble::Common::ClusterUtils::is_ip_from_status_running($cloud_status,$host_info->{ip})==1) {
             
           my $instance= Scramble::Common::ClusterUtils::get_instance_id_from_status_ip($cloud_status,$host_info->{ip});
           Scramble::Common::ClusterUtils::log_debug("[cloud_get_a_running_lb] Info: Found a running instance : ".$instance ,1);
           # attach_network_interface(network_interface_id, instance_id, device_index)
           # detach_network_interface(attachement_id, force=False)
           #disassociate_address(public_ip=None, association_id=None)
            my $found_instance= Scramble::Common::ClusterUtils::get_instance_from_status_name($cloud_status,$instance);
            my $found_instance_json = $json_cloud->allow_nonref->utf8->encode($found_instance);
            Scramble::Common::ClusterUtils::log_json($found_instance_json ,1);
              
            $command='{"level":"instances","command":{"action":"disassociate","group":"'. $host_info->{ip}.'","type":"all"},"cloud":'. $json_cloud_str. '}';
            $cloud_status_json = worker_cloud_command($command,"127.0.0.1:4731");
            sleep(5);
            $command='{"level":"instances","command":{"action":"associate","group":"all","type":"all"},"cloud":'. $json_cloud_str. ',"instance":'.$found_instance_json.'}';
            $cloud_status_json = worker_cloud_command($command,"127.0.0.1:4731");
            return $instance; 
          }  
        }
       }
    }
    return 0 ;
}

sub gearman_client() {
  # get the status from my point of view  

  my $cloud = Scramble::Common::ClusterUtils::get_active_cloud($config);
  my $cloud_name = Scramble::Common::ClusterUtils::get_active_cloud_name($config);
  my $json_cloud       = new JSON ;
  my $json_cloud_str = $json_cloud->allow_nonref->utf8->encode($cloud);
 
  #my $test='{"level":"instances","command":{"action":"disassociate","group":"all","type":"all"},"cloud":'. $json_cloud_str. '}';
  #my $cloud_test = worker_cloud_command($test,"127.0.0.1:4731");
  #return 0;            
  if (cloud_check_ssh() ==0) {
    $ssh_error_retry=$ssh_error_retry+1;
    if ($ssh_error_retry==$max_ssh_error_retry) {
        $ssh_error_retry=0;
       my $instance = cloud_get_a_running_lb();
    }
    return 0;
  }
  if  (cloud_check_ssh_tunnel() ==0)  {
         cloud_create_tunnel();
  }
   
  my $command='{"level":"instances","command":{"action":"actions","group":"all","type":"all"}}';
  my $cluster_actions_json= worker_cluster_command($command,"127.0.0.1:4730"); 
  
  Scramble::Common::ClusterUtils::log_debug("[cloud_doctor] Info: Fetching delayed actions",1);
  Scramble::Common::ClusterUtils::log_json( $cluster_actions_json,1) ;
  if (  cloud_check_return_error( $cluster_actions_json )) {
    if ( cloud_is_scramble_running()==0){
       cloud_start_scramble();
    } 
    else
    {
        if ( cloud_is_memcache_running()==0){
        
          $command='{"level":"services","command":{"action":"start","group":"local","type":"memcache"}}';
          Scramble::Common::ClusterUtils::log_debug("[cloud_doctor] Info: Starting Memcache ", 2);
          my $cluster_memcache = worker_cluster_command($command,"127.0.0.1:4730");     
        } else {
         Scramble::Common::ClusterUtils::log_debug("[cloud_doctor] Info: Try to fixe route to VIP issue", 2);
         cloud_vip_fix_route($json_cloud_str); 
        } 

        return 0;
    }
    return 0;
  }  
    
  
  my $cluster_actions = $json_cloud->allow_nonref->utf8->relaxed->escape_slash->loose
  ->allow_singlequote->allow_barekey->decode($cluster_actions_json);
 if (cloud_have_actions( $cluster_actions) ==0){ return 0};

  $command='{"level":"instances","command":{"action":"status","group":"all","type":"all"},"cloud":'. $json_cloud_str. '}';
  my $cloud_status_json = worker_cloud_status($command,"127.0.0.1:4731");
 
  print STDERR $cloud_status_json;
  Scramble::Common::ClusterUtils::log_debug("[cloud_doctor] Info: Retriving cloud status",1);
  Scramble::Common::ClusterUtils::log_json( $cloud_status_json,1);
  my $cloud_status = $json_cloud->allow_nonref->utf8->relaxed->escape_slash->loose
  ->allow_singlequote->allow_barekey->decode($cloud_status_json);
  
   
  $command='{"level":"instances","command":{"action":"heartbeat","group":"all","type":"all"}}';
  my $cluster_status_json = worker_cluster_command($command,"127.0.0.1:4730");    
  my $status= $json_cloud->allow_nonref->utf8->relaxed->escape_slash->loose
  ->allow_singlequote->allow_barekey->decode($cluster_status_json);
   
 if ( cloud_is_eth1_up()==0){
      cloud_vip_fix_route($json_cloud_str); 
 }   
  my $no_services=1;

  foreach  my $action (  @{ $cluster_actions->{actions}} ) {
    if  ( $action->{event_type}   eq "cloud" ) {
        Scramble::Common::ClusterUtils::log_debug("Processing ".$action->{do_group} ,2); ;
        #my $event_ip = Scramble::Common::ClusterUtils::get_service_ip_from_status_name($status,$action->{do_group});
        my $event_ip = $action->{do_group};
        Scramble::Common::ClusterUtils::log_debug("[cloud_doctor] Info: Found event on ". $event_ip, 2);
        if( $action->{do_action} eq "world" ){
                   Scramble::Common::ClusterUtils::log_debug("[cloud_doctor] Info: New instance show up bootstrap config ", 1); 
                   $command='{"level":"services","command":{"action":"bootstrap_config","group":"all","type":"all"}}';
                   worker_cluster_command($command,"127.0.0.1:4730");   
                  
                   Scramble::Common::ClusterUtils::log_debug("[cloud_doctor] Info: New instance show up starting local services ", 1);
                   $command='{"level":"services","command":{"action":"start","group":"'.$action->{do_group} . '","type":"all"}}';
                   worker_cluster_command($command,"127.0.0.1:4730");  
                   my $command='{"level":"instances","command":{"action":"actions_init","group":"all","type":"all"}}';
                   my $cluster_actions_json= worker_cluster_command($command,"localhost"); 
                   return 0;
        }    

        if ( Scramble::Common::ClusterUtils::is_ip_from_status_present($cloud_status,$event_ip)==1) {
              Scramble::Common::ClusterUtils::log_debug("[cloud_doctor] Info: Service IP for action is found in status of the cloud API ", 2);
                     
              if ( Scramble::Common::ClusterUtils::is_ip_from_status_running($status,$event_ip)==0) {
                 Scramble::Common::ClusterUtils::log_debug("[cloud_doctor] Info: Ip for for action not running but instance exists in the cloud", 2);
                 my $state = Scramble::Common::ClusterUtils::get_instance_status_from_ip($cloud_status,$event_ip);
                 if( $state eq "stopped"){
                     Scramble::Common::ClusterUtils::log_debug("[cloud_doctor] Info: Instance is in state stopped", 2); 
                   
                    my $instance= Scramble::Common::ClusterUtils::get_instance_id_from_status_ip($cloud_status,$event_ip);
                    
                   my $cmd_instance='{"level":"instances","command":{"action":"'.$action->{do_action} .'","group":"'.$instance.'","type":"all"},"cloud":'. $json_cloud_str. '}';
                   worker_cloud_command($cmd_instance,"127.0.0.1:4731");
                 }  
                 else{
                   Scramble::Common::ClusterUtils::log_debug("[cloud_doctor] Info: Instance is not stopped may be starting", 1); 
                        
                 }
              }
              else  {
                my $instance= Scramble::Common::ClusterUtils::get_instance_id_from_status_ip($cloud_status,$event_ip);
                    
                
                if( $action->{do_action} eq "world" ){
                  
                } else {
                  Scramble::Common::ClusterUtils::log_debug("[cloud_doctor] Info: Instance exists and is running may we  send stop or terminate action", 2); 
                  my $cmd_instance_stop='{"level":"instances","command":{"action":"'.$action->{do_action} .'","group":"'.$instance.'","type":"all"},"cloud":'. $json_cloud_str. '}';
                  worker_cloud_command($cmd_instance_stop,"127.0.0.1:4731"); 
                 } 
              }
          } else 
          {
              Scramble::Common::ClusterUtils::log_debug("[cloud_doctor] Info: Service Ip is not found in cloud status launching instance", 1);
              my $launch_instance='{"level":"instances","command":{"action":"launch","group":"ScrambleDB","type":"all","ip":"'.$event_ip.'"},"cloud":'. $json_cloud_str. '}';
              worker_cloud_command($launch_instance,"127.0.0.1:4731");
              sleep 10;
             
          }      

      }  
       if  ( $action->{event_type}  ne "cloud" ) {
        $no_services=0;
       }
  }
   if  ($no_services==1 ) {
       my $command='{"level":"instances","command":{"action":"actions_init","group":"all","type":"all"}}';
       my $cluster_actions_json= worker_cluster_command($command,"localhost"); 
   }
}


sub worker_cluster_command($$) {
    my $cmd    = shift;
    my $ip     = shift;
    my $client = Gearman::XS::Client->new();
    $client->add_servers($ip);
   
    Scramble::Common::ClusterUtils::log_debug( "[worker_cluster_command] ". $ip  ,1);
    Scramble::Common::ClusterUtils::log_json(  $cmd  ,1);
    
    $client->set_timeout(8000);
    #(my $ret,my $result) = $client->do_background('service_do_command', $cmd);
    ( my $ret, my $result ) = $client->do( 'cluster_cmd', $cmd );

    if ( $ret == GEARMAN_SUCCESS ) {
      if ( !defined $result ) {
        Scramble::Common::ClusterUtils::log_debug( "[worker_cluster_command] Error: ". "Gearman no result ",1);
        return  '{"return":{"code":"ER0006","version":"1.0"},"question":'.$cmd.'}';
     } else { 
        return $result; 
     }
    
   }
   Scramble::Common::ClusterUtils::log_debug( "[worker_cluster_command] Error: ". "Gearman failed ",1);  
   return  '{"return":{"code":"ER0006","version":"1.0"},"question":'.$cmd.' }';
  
    
}

sub worker_cloud_eip_status($$) {
    my $cmd    = shift;
    my $ip     = shift;
    my $client = Gearman::XS::Client->new();
    $client->add_servers($ip);
   
    Scramble::Common::ClusterUtils::log_debug( "[worker_cloud_eip_status] Info: ". $ip  ,1);
    Scramble::Common::ClusterUtils::log_json(  $cmd  ,1);
    
    $client->set_timeout(10000);
    #(my $ret,my $result) = $client->do_background('service_do_command', $cmd);
    ( my $ret, my $result ) = $client->do( 'cloud_cmd', $cmd );

    if ( $ret == GEARMAN_SUCCESS ) {
      if ( !defined $result ) {
         Scramble::Common::ClusterUtils::log_debug( "[worker_cloud_eip_status] Error: ". "Gearman no result ",1);
        return  '{"eips":[] , "return":{"code":"ER0006","version":"1.0"},"question":'.$cmd.'}';
     } else { 
        print STDOUT $result;
         Scramble::Common::ClusterUtils::log_json(  $result   ,1);
           return  '{"eips":'.$result.', "return":{"code":"ER0006","version":"1.0"},"question":'.$cmd.'}';
     }
    
   }
   Scramble::Common::ClusterUtils::log_debug( "[worker_cloud_eip_status] Error: ". "Gearman failed ",1); 
    return  '{"eips":[], "return":{"code":"ER0006","version":"1.0"},"question":'.$cmd.'}';
    
}

sub worker_cloud_interface_status($$) {
    my $cmd    = shift;
    my $ip     = shift;
    my $client = Gearman::XS::Client->new();
    $client->add_servers($ip);
   
    Scramble::Common::ClusterUtils::log_debug( "[worker_cloud_interface_status] Info: ". $ip  ,1);
    Scramble::Common::ClusterUtils::log_json(  $cmd  ,1);
    
    $client->set_timeout(10000);
    #(my $ret,my $result) = $client->do_background('service_do_command', $cmd);
    ( my $ret, my $result ) = $client->do( 'cloud_cmd', $cmd );

    if ( $ret == GEARMAN_SUCCESS ) {
      if ( !defined $result ) {
         Scramble::Common::ClusterUtils::log_debug( "[worker_cloud_interface_status] Error: ". "Gearman no result ",1);
        return  '{"interfaces_status":{"interfaces":[], "return":{"code":"ER0006","version":"1.0"},"question":'.$cmd.'}}';
     } else { 
        print STDOUT $result;
         Scramble::Common::ClusterUtils::log_json(  $result   ,1);
         return  '{"interfaces_status":{"interfaces":' . $result . ',"return":{"code":"000000","version":"1.0"},"question":'.$cmd.'}}'; 
     }
    
   }
   Scramble::Common::ClusterUtils::log_debug( "[worker_cloud_command] Error: ". "Gearman failed ",1); 
   return  '{"interfaces_status":{"interfaces":[], "return":{"code":"ER0006","version":"1.0"},"question":'.$cmd.'}}';
  
    
}



sub worker_cloud_status($$) {
    my $cmd    = shift;
    my $ip     = shift;
    my $client = Gearman::XS::Client->new();
    $client->add_servers($ip);
   
    Scramble::Common::ClusterUtils::log_debug( "[worker_cloud_status] Info: ". $ip  ,1);
    Scramble::Common::ClusterUtils::log_json(  $cmd  ,1);
    
    $client->set_timeout(10000);
    #(my $ret,my $result) = $client->do_background('service_do_command', $cmd);
    ( my $ret, my $result ) = $client->do( 'cloud_cmd', $cmd );

    if ( $ret == GEARMAN_SUCCESS ) {
      if ( !defined $result ) {
         Scramble::Common::ClusterUtils::log_debug( "[worker_cloud_status] Error: ". "Gearman no result ",1);
        return  '{"instances_status":{"instances":[], "return":{"code":"ER0006","version":"1.0"},"question":'.$cmd.'}}';
     } else { 
        print STDOUT $result;
         Scramble::Common::ClusterUtils::log_json(  $result   ,1);
         return  '{"instances_status":{"instances":' . $result . ',"return":{"code":"000000","version":"1.0"},"question":'.$cmd.'}}'; 
     }
    
   }
   Scramble::Common::ClusterUtils::log_debug( "[worker_cloud_status] Error: ". "Gearman failed ",1); 
   return  '{"instances_status":{"instances":[], "return":{"code":"ER0006","version":"1.0"},"question":'.$cmd.'}}';
  
    
}

sub worker_cloud_command($$) {
    my $cmd    = shift;
    my $ip     = shift;
    my $client = Gearman::XS::Client->new();
    $client->add_servers($ip);
   
    Scramble::Common::ClusterUtils::log_debug( "[worker_cloud_command] Info: ". $ip  ,1);
    Scramble::Common::ClusterUtils::log_json(  $cmd  ,1);
    
    $client->set_timeout(10000);
    #(my $ret,my $result) = $client->do_background('service_do_command', $cmd);
    ( my $ret, my $result ) = $client->do( 'cloud_cmd', $cmd );

    if ( $ret == GEARMAN_SUCCESS ) {
      if ( !defined $result ) {
         Scramble::Common::ClusterUtils::log_debug( "[worker_cloud_command] Error: ". "Gearman no result ",1);
        return  '{ "result":"failed", "return":{"code":"ER0006","version":"1.0"},"question":'.$cmd.'}';
     } else { 
        print STDOUT $result;
        return  '"result":"' . $result . '","return":{"code":"000000","version":"1.0"},"question":'.$cmd.'}'; 
     }
    
   }
   Scramble::Common::ClusterUtils::log_debug( "[worker_cloud_command] Error: ". "Gearman failed ",1); 
   return   return  '{ "result":"failed", "return":{"code":"ER0006","version":"1.0"},"question":'.$cmd.'}';
  
    
}

sub worker_node_command($$) {
    my $cmd    = shift;
    my $ip     = shift;
    my $client = Gearman::XS::Client->new();
    $client->add_servers($ip);
    Scramble::Common::ClusterUtils::log_debug( "[worker_node_command] Info: ". $ip  ,1);
    Scramble::Common::ClusterUtils::log_debug( "[worker_node_command] Info: ". $cmd  ,1);
    
    #$client->set_timeout($gearman_timeout);
    #(my $ret,my $result) = $client->do_background('service_do_command', $cmd);
    ( my $ret, my $result ) = $client->do( 'node_cmd', $cmd );

    if ( $ret == GEARMAN_SUCCESS ) {
      if ( !defined $result ) {
        return  '{"return":{"code":"ER0006","version":"1.0"},"question":'.$cmd.'}';
        Scramble::Common::ClusterUtils::log_debug( "[worker_node_command] Error: ". "Gearman no result ",1);
     } else { 
        return $result; 
     }
    } 
    else
    {
      Scramble::Common::ClusterUtils::log_debug( "[worker_node_command] Error: ".  "Gearman failed ",1) ;
    }

   return  '{"return":{"code":"ER0006","version":"1.0"},"question":'.$cmd.'}';
    
}


while ( 1 )  
{
   sleep $TIME;
   gearman_client();
 }
 


          
                