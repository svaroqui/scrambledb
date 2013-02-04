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
use Scramble::Common::Config;
use Gearman::XS qw(:constants);
use Gearman::XS::Client;
use Gearman::XS::Worker;
use JSON;
use Cache::Memcached;
use DBI;
use Data::Dumper;
use Scramble::Common::ClusterUtils;

our $SKYBASEDIR = $ ENV {SKYBASEDIR};
our $SKYDATADIR = $ ENV {SKYDATADIR};
our $gearman_timeout = 2000;
our $gearman_ip            ="localhost";
our $console = "{result:{status:'00000'}}";
our $config = new Scramble::Common::Config::;
my $conf="etc/cloud.cnf";
$config->read($conf);
$config->check('SANDBOX');


my $worker = new Gearman::XS::Worker;
my $ret = $worker->add_server($gearman_ip,0);
if ($ret != GEARMAN_SUCCESS) {
    Scramble::Common::ClusterUtils::log_debug("[cluster_doctor] Error:  $worker->error()",1); 
    exit(1);
}

$ret = $worker->add_function("consult_cmd", 0, \&consult_cmd, 0);
if ($ret != GEARMAN_SUCCESS) {
     Scramble::Common::ClusterUtils::log_debug("[cluster_doctor] Error:  $worker->error()",1); 
}

while (1) {

    my $ret = $worker->work();
    if ($ret != GEARMAN_SUCCESS) {
           Scramble::Common::ClusterUtils::log_debug("[cluster_doctor] Error:  $worker->error()",1); 
    }
}


 
sub consult_cmd() {
    my ($job, $options) = @_;
    my $command = $job->workload();
    Scramble::Common::ClusterUtils::log_debug("[cluster_doctor] Info: Receive command : $command",1);
    my $json = new JSON;
    my $diff_status = $json->allow_nonref->utf8->relaxed->escape_slash->loose->allow_singlequote->allow_barekey->decode($command);
    $console= "";
    $config->read("etc/cloud.cnf");
    $config->check('SANDBOX');
    my $mem_info=Scramble::Common::ClusterUtils::get_active_memcache($config);
    Scramble::Common::ClusterUtils::log_debug("[cluster_doctor] Info: ". "Get the actions in memcache: ". $mem_info->{ip} . ":" . $mem_info->{port},1);
   
        
    my $memd = new Cache::Memcached {
               'servers' => [ $mem_info->{ip} . ":" . $mem_info->{port} ],
               'debug'   => 0,
               'compress_threshold' => 10_000,
     };
      my $json_todo = $memd->get("actions");
      
        if (!$json_todo )
        {
            Scramble::Common::ClusterUtils::log_debug("[cluster_doctor] Info: No actions in memcache",1);
            return "ER0015";  
        }
        my $json_status = $memd->get("status");
        if (!$json_status )
        {
           Scramble::Common::ClusterUtils::log_debug("[cluster_doctor] Info: No status in memcache",1);
           return "ER0015";  
        }

        my $status = $json->allow_nonref->utf8->relaxed->escape_slash->loose
         ->allow_singlequote->allow_barekey->decode($json_status);

        my $todo =  $json->allow_nonref->utf8->relaxed->escape_slash->loose
         ->allow_singlequote->allow_barekey->decode($json_todo);
       foreach  my $action (  @{ $todo->{actions}} ) {
        foreach  my $trigger(  @{ $diff_status->{events}} ) {
            Scramble::Common::ClusterUtils::log_debug("[consult_cmd] Info: testing action ip:$action->{event_ip} with trigger $trigger->{ip} ",2);
            Scramble::Common::ClusterUtils::log_debug("[consult_cmd] Info: testing action type:$action->{event_type} with trigger $trigger->{type} ",2);
            Scramble::Common::ClusterUtils::log_debug("[consult_cmd] Info: testing action state:$action->{event_state} with trigger $trigger->{state} ",2);
            
          
            if  ($action->{event_ip} eq $trigger->{ip} && 
                 $action->{event_type} eq $trigger->{type} &&
                 $action->{event_state} eq $trigger->{state}
                ){
                 
                  my $command= '{"level":"'.$action->{do_level}. '","command":{"action":"'.$action->{do_action}.'","group":"'.$action->{do_group}.'","type":"all"} } ';
                  Scramble::Common::ClusterUtils::log_debug("[consult_cmd] Info: Test pass do action  ",1);
                  Scramble::Common::ClusterUtils::log_json($command,1);
                  worker_cluster_command($command,$gearman_ip);   
                  Scramble::Common::ClusterUtils::log_debug("[consult_cmd] Info: Set empty actions",1);
                  
                  $memd->set( "actions", '{"return":{"code":"000000","version":"1.0"},"actions":[]}' );
                  sleep 1;
            }   

        }
       }
}

sub worker_cluster_command($$) {
    my $cmd    = shift;
    my $ip     = shift;
    my $client = Gearman::XS::Client->new();
    $client->add_servers($ip);
    Scramble::Common::ClusterUtils::log_debug("[worker_cluster_command] Info: Send to ip :". $ip ,1);
    Scramble::Common::ClusterUtils::log_json($cmd,2);
   
   
    #$client->set_timeout($gearman_timeout);
    #(my $ret,my $result) = $client->do_background('service_do_command', $cmd);
    ( my $ret, my $result ) = $client->do( 'cluster_cmd', $cmd );

    if ( $ret == GEARMAN_SUCCESS ) {
      if ( !defined $result ) {
        return "ER0006";
        Scramble::Common::ClusterUtils::log_debug("[worker_cluster_command] Error : No result",1);
        
     } else { 
        return $result; 
     }
    } 
    else
    {
      Scramble::Common::ClusterUtils::log_debug("[worker_cluster_command] Error : Gearman call failed",1);
    }

   return "ER0006";
    
}
    



