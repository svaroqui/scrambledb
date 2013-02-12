#!../perl/bin/perl
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

use strict;
use Class::Struct;
use warnings FATAL => 'all';
use Sys::Hostname;
use Gearman::XS qw(:constants);
use Gearman::XS::Client;
use Scramble::ClusterLog;
use Scramble::ClusterConfig;
use JSON;

our $SKYDATADIR = $ ENV {SKYDATADIR};
our $log = new Scramble::ClusterLog;
our $config = new Scramble::ClusterConfig;

$config->read($SKYBASEDIR."/ncc/etc/cloud.cnf");
$log->set_logs($config);


my $TIME=$config->{"scramble"}->{"cluster_heartbeat_time"};
my $client = Gearman::XS::Client->new();
$client->add_servers("localhost");
 
sub gearman_client() {
  # get the status from my point of view  
  my $command='{"level":"services", "command":{"action":"status","group":"all","type":"all"}}';
  (my $ret,my $result_services_status) = $client->do('cluster_cmd', $command);   
  if ($ret != GEARMAN_SUCCESS) {
      $log->log_debug("[gearman_client] Error: ".$client->error(),1,"heartbeat");  
      return 0;
  }
  # system("system_profiler SPHardwareDataType | grep -i memory  | awk  '{print \$2*1024*1024*1024}'"); 

  $command='{"level":"instances","command":{"action":"status","group":"all","type":"all"}}';
  ($ret,my $result_instances_status) = $client->do('cluster_cmd', $command);   
  if ($ret != GEARMAN_SUCCESS) {
      $log->log_debug("[gearman_client] Error: ".$client->error(),1,"heartbeat");      
      return 0;
  }
  $command="cat /proc/meminfo |  grep MemTotal | awk '{print \$2}'";
  my  $ram = `$command`;  
    
  $ram =~ s/\n//g; 

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

$command='{"level":"services","version":"1.0","command":{"action":"ping","group":"all","type":"db"},"host":{"ram":"'.$ram. '","interfaces":['. $json_interfaces .']},"services_status":'.$result_services_status.',"instances_status":'.$result_instances_status.'}';
  
( $ret,my  $result) = $client->do('cluster_cmd', $command);
    printf(STDOUT "%s\n", $command);
    if ($ret != GEARMAN_SUCCESS) {
        $log->log_debug("[gearman_client] Error: ".$client->error(),1,"heartbeat");      
      
     
    }
    else {
          $log->log_debug("[gearman_client] Return: ".$ret,1,"heartbeat");      
          $log->log_debug("[gearman_client] Result: ".$result,1,"heartbeat");      
    }
}

while ( 1 )  
{
   sleep $TIME;
   gearman_client();
 }
 

