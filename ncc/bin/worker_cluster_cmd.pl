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


use Scramble::ClusterConfig;
use Scramble::ClusterLog;
use Scramble::ClusterUtils;
use Scramble::ClusterTransport;
use Gearman::XS qw(:constants);
use Gearman::XS::Worker;


use Cache::Memcached;
use Sys::Hostname;
use Data::Dumper;
use strict;
use Class::Struct;
use warnings FATAL => 'all';
use JSON;
use DBI;

our $SKYBASEDIR            = $ENV{SKYBASEDIR};
our $SKYDATADIR            = $ENV{SKYDATADIR};
our $config                = new Scramble::ClusterConfig::;
our $log                = new Scramble::ClusterLog;
$config->read($SKYBASEDIR."/ncc/etc/cloud.cnf");
$config->check('SANDBOX');


our $gearman_ip            ="localhost";
our $hashcolumn;
our $createtable           = "";
our $like                  = "none";
our $database              = "";

our $mysql_connect_timeout = 1;



my $sshkey;



my $worker = new Gearman::XS::Worker;
my $ret = $worker->add_server( '', 0 );

if ( $ret != GEARMAN_SUCCESS ) {
    $log->log_debug("[cluster_cmd] Error: $worker->error()",1);
    exit(1);
}

$ret = $worker->add_function( "cluster_cmd", 0, \&cluster_cmd, 0 );
if ( $ret != GEARMAN_SUCCESS ) {
    $log->log_debug("[cluster_cmd] Error: $worker->error()",1);
}

while (1) {

    my $ret = $worker->work();
    if ( $ret != GEARMAN_SUCCESS ) {
        $log->log_debug("[cluster_cmd] Error: $worker->error()",1);
        
    }
}

sub cluster_cmd {
    my ( $job, $options ) = @_;
  
    my $table   = "";
    my $command = $job->workload();
    my $json    = new JSON;
    my $json_text =
    $json->allow_nonref
    ->utf8
    ->relaxed
    ->escape_slash
    ->loose
    ->allow_singlequote
    ->allow_barekey
    ->decode($command);
    my $peer_host_info = "";
    my $myhost         = hostname;
    my $ddlallnode     = 0;
    my $ddlallproxy    = 0;
    my $ret            = "true";
    
    
    $config->read($SKYBASEDIR ."/ncc/etc/cloud.cnf");
    $config->check('SANDBOX');
   
    $log->init_console();    
    
   
    my $cloud =Scramble::ClusterUtils::get_active_cloud($config);
   
    my $cloud_name = Scramble::ClusterUtils::get_active_cloud_name($config);
    $log->log_debug($cloud_name,1);
    $sshkey ="/.ssh/" . $cloud->{public_key} ;

    my $json2       = new JSON;
    my $json_config = $json2->allow_blessed->convert_blessed->encode($config);
    $Data::Dumper::Terse     = 1;       # don't output names where feasible
    $Data::Dumper::Quotekeys = 0;
    $Data::Dumper::Indent    = 1;       # mild pretty print
    $Data::Dumper::Pair      = " = ";
    $Data::Dumper::Indent    = 2;
    $Data::Dumper::Useqq     = 0;

    #  print  STDERR Dumper($config);
    #  print  STDERR $json_config;

    my $retjson="";
    my $action   = $json_text->{command}->{action};
    if ($action ne "ping" )  {$log->log_debug("[cluster_cmd] Info: Receive command : $command ",1);}
    my $group    = $json_text->{command}->{group};
    my $type     = $json_text->{command}->{type};
    my $query    = $json_text->{command}->{query};
    my $database = $json_text->{command}->{database};
    my $level    = $json_text->{level};
    if ( $level eq "instances"  ){
      my $json_cmd       = new JSON ;
       my $json_cmd_str = $json_cmd->allow_nonref->utf8->encode($json_text->{command});
       my $mem_info= Scramble::ClusterUtils::get_active_memcache($config);
       my $memd = new Cache::Memcached {
               'servers' => [ $mem_info->{ip} . ":" . $mem_info->{port} ],
               'debug'   => 0,
               'compress_threshold' => 10_000,
       };     
       if ($action eq "actions" ) {
            my $json_todo = $memd->get("actions");
           if (!$json_todo )
           {
                  $log->log_debug("[cluster_cmd] Info: No actions in memcache ",1);
                  $log->report_action( "localhost", "fetch_event",  "ER0015","memd->get('actions')");
                  my $json_action      = new JSON;
                  my @console =$log->get_actions();
                  return  '{"return":{"code":"ER0015"},"console":'. $json_action->allow_nonref->utf8->encode(\@console) .' , "version":"1.0","question":'.$json_cmd_str.',"actions":[]}';
           }
           print STDERR $json_todo ."\n";
           return   $json_todo ;
       }
       elsif ($action eq "heartbeat" ) {
           my $json_status = $memd->get("status");
           if (!$json_status )
           {
                 $log->log_debug("[cluster_cmd] Info: No heartbeat in memcache ",1);
                 $log->report_action( "localhost", "fetch_heartbeat",  "ER0015","memd->get('status')" );
                 my $json_action      = new JSON;
                 my @console =$log->get_actions();
                
                 return  '{"return":{"code":"ER0015"},"console":'. $json_action->allow_nonref->utf8->encode(\@console) .' , "version":"1.0","question":'.$json_cmd_str.',"instances":[]}';
           }
           print STDERR $json_status ."\n";
           return   $json_status ;
       }
       elsif ( $action eq "status") {
        $ret= get_local_instances_status($config);
        return  '{"return":{"code":"000000"},"instances":'. $ret .'}' ;
       } 
       elsif ( $action eq "actions_init") {
          $log->log_debug("[cluster_cmd] Info: Set empty actions",1);
          $memd->set( "actions", '{"return":{"code":"000000"},"version":"1.0","question":'.$json_cmd_str.',"actions":[]}' );
       } 
       elsif ( $action eq "start" || $action eq "launch" || $action eq "stop" || $action eq "terminate" || $action eq "world") {
        my $json_todo = $memd->get("actions");
        if (!$json_todo )
        {
               $log->log_debug("[cluster_cmd] Info: No actions in memcache",1); 
   
               $log->report_action( "localhost", "fetch_event",  "ER0015","memd->get('actions')");
               my $json_action      = new JSON;
               my @console =$log->get_actions();
               return  '{"return":{"code":"ER0015"},"console":'. $json_action->allow_nonref->utf8->encode(@console) .' , "version":"1.0","question":'.$json_cmd_str.',"actions":[]}';
               
        }
        my $json      = new JSON ;
        my $todo =  $json->allow_nonref->utf8->relaxed->escape_slash->loose
        ->allow_singlequote->allow_barekey->decode($json_todo);
        if ($action eq "world"){
           $group=Scramble::ClusterUtils::get_source_ip_from_command($json_text,$config);
        }
        push  @{$todo->{actions}} , {
                 event_ip       => "na",
                 event_type     => "cloud",
                 event_state    => "stopped" ,
                 do_level       => "instances" ,
                 do_group       =>  $group,
                 do_action      =>  $action 
        };  
        
        
        $json_todo =  $json->allow_blessed->convert_blessed->encode($todo);
        $log->log_debug("[cluster_cmd] Info: Delayed actions",1);
        $log->log_json($json_todo,1);
       
         
        $memd->set( "actions",  $json_todo);
        $log->report_action( "localhost", "delayed_action",  "ER0017","memd->set('actions',?)");
        return  '{"return":{"code":"000000"},"version":"1.0","question":'.$json_cmd_str.',"actions":'. $json->allow_nonref->utf8->encode($log->get_actions())."}";
      }
      
       
    }  
    if ( $level eq "services" ){
        if ( $action eq "sql" ) {
               $like       = "";
               $ddlallnode = spider_is_ddl_all_node($query);
               if ( $like ne "" ) { $query = spider_rewrite_query_like(); }
        }
        elsif ( $action eq "start" ) {
            $ret = bootstrap_config();
        }
        elsif ( $action eq "bootstrap_binaries" ) {
            $ret = bootstrap_binaries();
        }
        elsif ( $action eq "bootstrap_ncc" ) {
            $ret = bootstrap_ncc();
        }
        elsif ( $action eq "bootstrap_config" ) {
            $ret = bootstrap_config();
        }
        elsif ( $action eq "rolling_restart" ) {
           $ret = database_rolling_restart();
        }
        elsif ( $action eq "ping" ) {
          $ret = instance_heartbeat_collector($json_text,$command);
        }
        elsif ( $action eq "switch_vip"){
          $ret = service_switch_vip();
        }
        elsif ( $action eq "check_vip"){
          $ret = service_check_vip();
        }
        foreach my $host ( sort( keys( %{ $config->{db} } ) ) ) {
           my $host_info = $config->{db}->{default};
           $host_info = $config->{db}->{$host};
           if ( $host_info->{mode} ne "spider" ) {
               $peer_host_info = $config->{db}->{ $host_info->{peer}[0] };
           }
          
           if ( is_filter_service($group,$type, $host_info, $host, $cloud_name) == 1 ) {
               $log->log_debug("[cluster_cmd] Info: Processing $host on $myhost",1);  
               if ( $action eq "install" ) {
                   $ret = service_do_command( $host_info, $host, $action );
               }
               if ( $action eq "stop" ) {
                   $ret = service_do_command( $host_info, $host, $action );
               }
               if ( $action eq "sync" ) {
                   $ret = service_sync_database( $host_info, $host, $peer_host_info );
               }
               if ( $action eq "start" ) {
                   $ret = service_do_command( $host_info, $host, $action, $query );
               }
               if ( $action eq "remove" ) {
                   $ret = service_do_command( $host_info, $host, $action, $query );
               }
               if ( $action eq "restart" ) {
                   $ret = service_do_command( $host_info, $host, $action );
               }
               if ( $action eq "status" ) {
                   $ret = service_do_command( $host_info, $host, $action );
               }
               if ( $action eq "join" ) {
                   $ret = spider_node_join( $host_info, $host ); 
               }
               if ( $action eq "switch" ) { 
                   $ret = service_switch_database($host); 
                  }
               if ( $action eq "sql" ) {
                   $ret = spider_node_sql( $host_info, $host, $action, $query, $database,
                       $ddlallnode );
               }
           }
       }
       if ( $action eq "sql" && ( $ddlallnode == 0 || $ddlallnode == 2 ) ) {
            spider_create_table_info( $query, $ddlallnode );
       }
       foreach my $nosql ( sort( keys( %{ $config->{nosql} } ) ) ) {
            my $host_info = $config->{nosql}->{default};
            $host_info = $config->{nosql}->{$nosql};
            if (is_filter_service($group,$type, $host_info, $nosql, $cloud_name) == 1   ) {
                my $le_localtime = localtime;
                if ( $action eq "stop" ) {
                    $ret = service_do_command( $host_info, $nosql, $action );
                }
                if ( $action eq "start" ) {
                    $ret = service_do_command( $host_info, $nosql, $action, $query );
                }
                if ( $action eq "restart" ) {
                    $ret = service_do_command( $host_info, $nosql, $action );
                }
                if ( $action eq "status" ) {
                    $ret = service_do_command( $host_info, $nosql, $action );
                }
            }
        }

        foreach my $host ( sort( keys( %{ $config->{proxy} } ) ) ) {
            my $host_info = $config->{proxy}->{default};
            $host_info = $config->{proxy}->{$host};
            if ( is_filter_service($group,$type, $host_info, $host, $cloud_name)  == 1 ) {
                my $le_localtime = localtime;
                if ( $action eq "stop" ) {
                    $ret = service_do_command( $host_info, $host, $action );
                }
                if ( $action eq "start" ) {
                    $ret = service_do_command( $host_info, $host, $action, $query );
                }
                if ( $action eq "restart" ) {
                    $ret = service_do_command( $host_info, $host, $action );
                }
                if ( $action eq "status" ) {
                    $ret = service_do_command( $host_info, $host, $action );
                }
            }

        }
        foreach my $host ( sort( keys( %{ $config->{lb} } ) ) ) {
            my $host_info = $config->{lb}->{default};
            $host_info = $config->{lb}->{$host};
           
            if (is_filter_service($group,$type, $host_info, $host, $cloud_name) == 1 ) {
                my $le_localtime = localtime;
                if ( $action eq "stop" ) {
                    $ret = service_do_command( $host_info, $host, $action );
                }
                if ( $action eq "start" ) {
                    $ret = service_do_command( $host_info, $host, $action, $query );
                }
                if ( $action eq "restart" ) {
                    $ret = service_do_command( $host_info, $host, $action );
                }
                if ( $action eq "status" ) {
                    $ret = service_do_command( $host_info, $host, $action );
                }
            }

        }
        
        foreach my $host ( sort( keys( %{ $config->{bench} } ) ) ) {
            my $host_info = $config->{bench}->{default};
            $host_info = $config->{bench}->{$host};
          
            if ( is_filter_service($group,$type, $host_info, $host, $cloud_name) == 1 ) {
                my $le_localtime = localtime;
                if ( $action eq "install" ) {
                    $ret = service_install_bench( $host_info, $host, $action );
                }
                if ( $action eq "stop" ) {
                    $ret = service_stop_bench( $host_info, $host, $action );
                }
                if ( $action eq "start" ) {
                    $retjson = service_start_bench( $host_info, $host, $action, $query );
                }
                

            }
        }
        foreach my $host ( sort( keys( %{ $config->{monitor} } ) ) ) {
          my $host_info = $config->{monitor}->{default};
          $host_info = $config->{monitor}->{$host};
         
          if ( is_filter_service($group,$type, $host_info, $host, $cloud_name) == 1 ) {
              my $le_localtime = localtime;

              if ( $action eq "stop" ) {
                  $ret = service_do_command( $host_info, $host, $action );
              }
              if ( $action eq "start" ) {
                  $ret = service_do_command( $host_info, $host, $action, $query );
              }
              if ( $action eq "restart" ) {
                  $ret = service_do_command( $host_info, $host, $action );
              }

          }

        }
   }
   my $json_action      = new JSON; 
   my @actions = $log->get_actions();
  
   my @console = $log->get_console() ;
   return '{"'.$level.'":[' . join(',' , @console) .']'. $retjson .',"console":'. $json_action->allow_nonref->utf8->encode(\@actions).' }';

}

sub is_filter_service($$$$$) {
    my $action_group = shift;
    my $action_type = shift; 
    my $service_host = shift;
    my $host_name =  shift; 
    my $action_cloud_name = shift;
    
    $log->log_debug("[is_filter_service] Info: Start",2); 
    my $pass = 0;
    
    if ( $action_group eq $host_name 
         || $action_group eq "all" 
         || ($action_group eq "local" 
             && Scramble::ClusterUtils::is_ip_localhost($service_host->{ip})==1 )
         || $action_group eq $service_host->{ip}  
       )     
     { 
        
        if ( $action_type eq $service_host->{mode} 
             || $action_type eq "all" 
            ) { 
              if ( $action_cloud_name  eq $service_host->{cloud} ){
                 $pass = 1;
               }     
         } 
    
      } 
    
    
    return  $pass;        
}

sub get_local_instances_status($) {
    my $config =shift;
    my @ips=Scramble::ClusterUtils::get_all_sercive_ips($config);
    my @interfaces;
    my $i=0;
    my $host_info;
    my $state ;
    $log->log_debug("[get_local_instances_status] Info: Start",2); 
    foreach my $ip ( @ips)  {
        if (instance_check_ssh($ip) ==0 )  {
           $state ="stopped";
        }   else {  
            $state ="running"; 
        }     
        push @interfaces, {"instance". $i=>  {
            id     => "instance". $i,
            ip       => $ip,
            state     => $state 
        }  };

     $i++;    
    }
    my $json       = new JSON;
    my $json_instances_status =  $json->allow_blessed->convert_blessed->encode(\@interfaces);
   
    return $json_instances_status ; 
}

sub get_status_diff($$) {
  my $previous_status =shift;
  my $status_r =shift;
  
  my $i=0;
  my @diff;
   
   
  foreach  my $service (  @{ ${$status_r}->{services_status}->{services}} ) {
   foreach my $key (keys %$service) {
      # service name may change or service removed  
      if (  defined($previous_status->{services_status}->{services}[$i]->{$key}->{state}) )
      { 
           
      $log->log_debug("[get_status_diff] Info:search diff services $key",2);    
      if ( $service->{$key}->{state} ne $previous_status->{services_status}->{services}[$i]->{$key}->{state}
            || $service->{$key}->{code} ne $previous_status->{services_status}->{services}[$i]->{$key}->{code}
         )    
      {
         $log->log_debug("[get_status_diff] Info: Found diff services",2); 
         push @diff, {
         name     => $key ,
         type     => "services",
         ip    => $service->{$key}->{ip} ,
         state    => $service->{$key}->{state} ,
         code     => $service->{$key}->{code} , 
         previous_state =>$previous_status->{services_status}->{services}[$i]->{$key}->{state},
         previous_code =>$previous_status->{services_status}->{services}[$i]->{$key}->{code}
        };  
        
       }
       
     }
     }
      
   $i++;
  }
   $i=0;
   foreach  my $instance (  @{ ${$status_r}->{instances_status}->{instances}} ) {
     foreach my $attr (keys %$instance) {
       my $skip=0; 
       $log->log_debug("[get_status_diff] Info: Testing ssh instances  $attr",2);    
     
       if ( (defined ($instance->{$attr}->{state}) ? $instance->{$attr}->{state}:"") ne 
          (defined ($previous_status->{instances_status}->{instances}[$i]->{$attr}->{state}) ? $previous_status->{instances_status}->{instances}[$i]->{$attr}->{state} :"")

        )    
       {
           
            if ((defined($instance->{$attr}->{state}) ? $instance->{$attr}->{state} : "" ) eq "running"){
             if( instance_check_ssh($instance->{$attr}->{ip}) ==0 ) {
                  $log->log_debug("[get_status_diff] Info: Skipping because ssh failed ",1);   
                  ${$status_r}->{instances_status}->{instances}[$i]->{$attr}->{state}="pending";
                     $skip=1;  
               
          } 

         } 
         if ($skip==0 )   {
           $log->log_debug("[get_status_diff] Info: Found diff instances",2);  
         
           push @diff, {
            name     => $instance->{$attr}->{id} ,
            type     => "instances",
            ip       => defined($instance->{$attr}->{ip}) ?  $instance->{$attr}->{ip} : "",
            state    => defined($instance->{$attr}->{state} ) ? $instance->{$attr}->{state} : "" ,
            code     => "0" , 
            previous_state =>$previous_status->{instances_status}->{instances}[$i]->{$attr}->{state},
            previous_code =>0
          };  
        }

       }
       }
       $i++;
     }  
     my $json       = new JSON;
     my $json_status_diff = '{"events":' . $json->allow_blessed->convert_blessed->encode(\@diff).'}';
     $log->log_json($json_status_diff,2);  
     return $json_status_diff;
}

sub gttid_reinit(){
  $log->log_debug("[gttid_reinit] Info: Start ",2);  
  my $sql ="replace into mysql.TBLGTID select CRC32(concat(table_schema, table_name)), 0,1  from information_schema.tables;";
  my $master_host= Scramble::ClusterUtils::get_active_db($config);
  mysql_do_command($master_host,$sql); 
}


sub dbt2_parse_mix($) {
    
    my $filename = shift;
    $log->log_debug("[dbt2_parse_mix] Info: Start ",1);  
    my $self ;
    my $current_time;
    my $previous_time;
    my $elapsed_time = 1;
    my $total_transaction_count = 0;
    my %transaction_count;
    my %error_count;
    my %rollback_count;
    my %transaction_response_time;

    my @delivery_response_time = ();
    my @new_order_response_time = ();
    my @order_status_response_time = ();
    my @payement_response_time = ();
    my @stock_level_response_time = ();
    #
    # Zero out the data.
    #
    $rollback_count{ 'd' } = 0;
    $rollback_count{ 'n' } = 0;
    $rollback_count{ 'o' } = 0;
    $rollback_count{ 'p' } = 0;
    $rollback_count{ 's' } = 0;
    #
    # Transaction counts for the steady state portion of the test.
    #
    $transaction_count{ 'd' } = 0;
    $transaction_count{ 'n' } = 0;
    $transaction_count{ 'o' } = 0;
    $transaction_count{ 'p' } = 0;
    $transaction_count{ 's' } = 0;

    $self->{data}->{errors} = 0;
    $self->{data}->{steady_state_start_time} = undef;
    $self->{data}->{start_time} = undef;

    open(FILE, "< $filename");
    while (defined(my $line = <FILE>)) {
        chomp $line;
        my @word = split /,/, $line;

        if (scalar(@word) == 4) {
            $current_time = $word[0];
            my $transaction = $word[1];
            my $response_time = $word[2];
            my $tid = $word[3];

            #
            # Transform mix.log into XML data.
            #
            push @{$self->{data}->{mix}->{data}},
                    {ctime => $current_time, transaction => $transaction,
                    response_time => $response_time, thread_id => $tid};

            unless ($self->{data}->{start_time}) {
                $self->{data}->{start_time} = $previous_time = $current_time;
            }
            #
            # Count transactions per second based on transaction type only
            # during the steady state phase.
            #
            if ($self->{data}->{steady_state_start_time}) {
                if ($transaction eq 'd') {
                    ++$transaction_count{$transaction};
                    $transaction_response_time{$transaction} += $response_time;
                    push @delivery_response_time, $response_time;
                } elsif ($transaction eq 'n') {
                    ++$transaction_count{$transaction};
                    $transaction_response_time{$transaction} += $response_time;
                    push @new_order_response_time, $response_time;
                } elsif ($transaction eq 'o') {
                    ++$transaction_count{$transaction};
                    $transaction_response_time{$transaction} += $response_time;
                    push @order_status_response_time, $response_time;
                } elsif ($transaction eq 'p') {
                    ++$transaction_count{$transaction};
                    $transaction_response_time{$transaction} += $response_time;
                    push @payement_response_time, $response_time;
                } elsif ($transaction eq 's') {
                    ++$transaction_count{$transaction};
                    $transaction_response_time{$transaction} += $response_time;
                    push @stock_level_response_time, $response_time;
                } elsif ($transaction eq 'D') {
                    ++$rollback_count{'d'};
                } elsif ($transaction eq 'N') {
                    ++$rollback_count{'n'};
                } elsif ($transaction eq 'O') {
                    ++$rollback_count{'o'};
                } elsif ($transaction eq 'P') {
                    ++$rollback_count{'p'};
                } elsif ($transaction eq 'S') {
                    ++$rollback_count{'s'};
                } elsif ($transaction eq 'E') {
                    ++$self->{data}->{errors};
                    ++$error_count{$transaction};
                } else {
                    print "error with mix.log format\n";
                    exit(1);
                }
                ++$total_transaction_count;
            }
        } elsif (scalar(@word) == 2) {
            #
            # Look for that 'START' marker to determine the end of the rampup
            # time and to calculate the average throughput from that point to
            # the end of the test.
            #
            $self->{data}->{steady_state_start_time} = $word[0];
        }
    }
    close(FILE);
    #
    # Calculated the number of New Order transactions per second.
    #
    my $tps = $transaction_count{'n'} /
            ($current_time - $self->{data}->{steady_state_start_time});
    $self->{data}->{metric} = $tps * 60.0;
    $self->{data}->{duration} =
            ($current_time - $self->{data}->{steady_state_start_time}) / 60.0;
    $self->{data}->{rampup} = $self->{data}->{steady_state_start_time} -
            $self->{data}->{start_time};
    #
    # Other transaction statistics.
    #
    my %transaction;
    $transaction{'d'} = "Delivery";
    $transaction{'n'} = "New Order";
    $transaction{'o'} = "Order Status";
    $transaction{'p'} = "Payment";
    $transaction{'s'} = "Stock Level";
    #
    # Resort numerically, default is by ascii..
    #
    @delivery_response_time = sort { $a <=> $b } @delivery_response_time;
    @new_order_response_time = sort{ $a <=> $b }  @new_order_response_time;
    @order_status_response_time =
        sort { $a <=> $b } @order_status_response_time;
    @payement_response_time = sort { $a <=> $b } @payement_response_time;
    @stock_level_response_time = sort { $a <=> $b } @stock_level_response_time;
    #
    # Get the index for the 90th percentile response time index for each
    # transaction.
    #
    my $delivery90index = $transaction_count{'d'} * 0.90;
    my $new_order90index = $transaction_count{'n'} * 0.90;
    my $order_status90index = $transaction_count{'o'} * 0.90;
    my $payment90index = $transaction_count{'p'} * 0.90;
    my $stock_level90index = $transaction_count{'s'} * 0.90;

    my %response90th;

    #
    # 90th percentile for Delivery transactions.
    #
    $response90th{'d'} =  Scramble::ClusterUtils::get_90th_per($delivery90index,
            @delivery_response_time);
    $response90th{'n'} =  Scramble::ClusterUtils::get_90th_per($new_order90index,
            @new_order_response_time);
    $response90th{'o'} =  Scramble::ClusterUtils::get_90th_per($order_status90index,
            @order_status_response_time);
    $response90th{'p'} =  Scramble::ClusterUtils::get_90th_per($payment90index,
            @payement_response_time);
    $response90th{'s'} =  Scramble::ClusterUtils::get_90th_per($stock_level90index,
            @stock_level_response_time);
    #
    # Summarize the transaction statistics into the hash structure for XML.
    #
    $self->{data}->{transactions}->{transaction} = [];
    foreach my $idx ('d', 'n', 'o', 'p', 's') {
        my $mix = ($transaction_count{$idx} + $rollback_count{$idx}) /
                $total_transaction_count * 100.0;
        my $rt_avg = 0;
        if ($transaction_count{$idx} != 0) {
            $rt_avg = $transaction_response_time{$idx} /
                    $transaction_count{$idx};
        }
        my $txn_total = $transaction_count{$idx} + $rollback_count{$idx};
        my $rollback_per = $rollback_count{$idx} / $txn_total * 100.0;
        push @{$self->{data}->{transactions}->{transaction}},
                {mix => $mix,
                rt_avg => $rt_avg,
                rt_90th => $response90th{$idx},
                total => $txn_total,
                rollbacks => $rollback_count{$idx},
                rollback_per => $rollback_per,
                name => $transaction{$idx}};
    }
    my $json       = new JSON;
    my $json_result = ',"results":' . $json->allow_blessed->convert_blessed->encode($self);
    $log->log_debug("[dbt2_parse_mix] Info: End ",1);  
   return $json_result;
}


sub mysql_do_command($$) {
  my $host_info=shift;
  my $sql =shift;
        my $dsn =
            "DBI:mysql:host="
          . $host_info->{ip}
          . ";port="
          . $host_info->{mysql_port}
          . ";mysql_connect_timeout="
          . $mysql_connect_timeout;

        my $dbh = DBI->connect(
            $dsn,
            $host_info->{mysql_user},
            $host_info->{mysql_password},
            {RaiseError=>1, mysql_no_autocommit_cmd => 1}
        );
        use Error qw(:try);
        try { 
            $log->log_debug("[mysql_do_command] Info: $dsn $sql ",1); 
            my $sth = $dbh->do($sql);           
        }
        catch Error with {
            $log->log_debug("[mysql_do_command] Error: $dsn $sql ",1); 
            return 0;
        };
        $dbh->disconnect;
        return 1;  
}

sub spider_is_ddl_all_node($) {
    my $lquery = shift;
     $log->log_debug("[spider_is_ddl_all_node] Info: start  ",1);  
    my @tokens = tokenize_sql($lquery);
    my $create = 99;
    my $next_i = 1;
    for my $token (@tokens) {
        if ( lc $token eq "create" ) {
            if ( lc $tokens[$next_i] eq "table" ) {
                $create      = 1;
                $createtable = $tokens[ $next_i + 1 ];
                if ( scalar(@tokens) >= $next_i + 2 ) {
                    if ( $tokens[ $next_i + 2 ] eq "." ) {
                        $createtable = $tokens[ $next_i + 3 ];
                        # $database = $tokens[$next_i+1];
                        # need to be unquited but useless as the table in token is comming with the db
                    }
                }
            }
        }
        if ( lc $token eq "like" && $create == 1 ) {
            print STDERR "token:" . $token . " ct" . scalar(@tokens);
            $like = $tokens[$next_i];
            print STDERR " createtablelike: " . $like;

            if ( scalar(@tokens) >= $next_i + 2 ) {
                if ( $tokens[ $next_i + 1 ] eq "." ) {
                    print STDERR $like;
                    $like = $tokens[ $next_i + 2 ];
                }
            }
        }
        $next_i++;
    }
    if ( $create == 1 ) { return 0; }
    if (   lc $tokens[1] eq "trigger"
        || lc $tokens[1] eq "view"
        || lc $tokens[1] eq "procedure"
        || lc $tokens[1] eq "function" )
    {
        print STDERR "create statement to proxy node only \n";
        return 2;
    }
    return 1;
}

sub spider_rewrite_query_like() {

    my $host_info;
    $log->log_debug("[spider_rewrite_query_like] start  ",1);  
    foreach my $host ( keys( %{ $config->{db} } ) ) {
        if ( $config->{db}->{$host}->{status} eq "master" ) {
            $host_info = $config->{db}->{default};
            $host_info = $config->{db}->{$host};
            my $dsn =
                "DBI:mysql:database="
              . $database
              . ";host="
              . $host_info->{ip}
              . ";port="
              . $host_info->{mysql_port}
              . ";mysql_connect_timeout="
              . $mysql_connect_timeout;
            my $dbh = DBI->connect(
                $dsn,
                $host_info->{mysql_user},
                $host_info->{mysql_password}
            );
            my $sql = "show create table " . $like;

            my $sth2 = $dbh->prepare($sql);
            $sth2->execute();
            print STDERR $sql;
            while ( my @row2 = $sth2->fetchrow_array() ) {
                my $createsql = $row2[1];
                print STDERR $createsql;

                return spider_swap_table_name($createsql);
            }

        }
    }

}

sub spider_swap_table_name($) {
    my $oriq   = shift;
    my $destq  = "";
    my @tokens = tokenize_sql($oriq);
    my $next_i = 1;
   $log->log_debug("[spider_swap_table_name] start  ",1);    
    for my $token (@tokens) {
        print STDERR $token . "\n";
        if ( lc $token eq "create" ) {
            if ( lc $tokens[$next_i] eq "table" ) {
                if ( $tokens[ $next_i + 2 ] eq "." ) {
                    $tokens[ $next_i + 3 ] = $createtable;
                }
                else {
                    $tokens[ $next_i + 1 ] = $createtable;
                }
            }
        }
        $destq = $destq . $token . " ";
        $next_i++;
    }
    return $destq;
}

sub spider_node_join($$) {
    my $host_joined     = shift;
    my $host_joined_key = shift;
    my $host_info;
    my $host_router;
    my $loop_host;

    my $dsn =
        "DBI:mysql:database="
      . $database
      . ";host="
      . $host_info->{ip}
      . ";port="
      . $host_info->{mysql_port}
      . ";mysql_connect_timeout="
      . $mysql_connect_timeout;

    my $database = "";
    foreach my $host ( keys( %{ $config->{db} } ) ) {
        if (   $host_joined_key ne $host
            && $config->{db}->{$host}->{status} eq "master" )
        {
            $host_info = $config->{db}->{default};
            $host_info = $config->{db}->{$host};
        }
        if ( $config->{db}->{$host}->{mode} eq "router" ) {
            $host_router = $config->{db}->{default};
            $host_router = $config->{db}->{$host};
        }
    }
    $dsn =
        "DBI:mysql:database="
      . $database
      . ";host="
      . $host_info->{ip}
      . ";port="
      . $host_info->{mysql_port}
      . ";mysql_connect_timeout="
      . $mysql_connect_timeout;
    my $dbh = DBI->connect(
        $dsn,
        $host_info->{mysql_user},
        $host_info->{mysql_password}
    );
    my $sql =
   "select table_schema, table_name from information_schema.tables where TABLE_SCHEMA<>'mysql' AND TABLE_SCHEMA<>'information_schema'";
    $log->log_debug("[spider_node_join] $dsn: $sql  ",1);     
    
    my $result = $dbh->selectall_arrayref( $sql, { Slice => {} } );
    my $createsql = '';
    my $sth2;

    for my $i ( 0 .. $#$result ) {
        my $onlinealter =
            $SKYBASEDIR
          . "/ncc/bin/pt_online_schema_change h="
          . $host_router->{ip} . ",P="
          . $host_router->{mysql_port} . ",u="
          . $host_router->{mysql_user} . ",p="
          . $host_router->{mysql_password} . ",D="
          . $result->[$i]{table_schema} . ",t="
          . $result->[$i]{table_name}
          . " --execute";
        $log->log_debug("[spider_node_join] $onlinealter  ",1);     
        Scramble::ClusterTransport::worker_node_command( $onlinealter, $host_router->{ip} ,$log);
    }
    $dbh->disconnect;
}

sub spider_node_sql($$$$$$$) {
    my $self       = shift;
    my $node       = shift;
    my $cmd        = shift;
    my $query      = shift;
    my $database   = shift;
    my $ddlallnode = shift;
    my $param  = "";
    my $err    = "000000";
    my $result = "";

    # do not produce per node action if only DDL on the proxy (tiggers,views,procedures,functions)
    if ( $ddlallnode == 2 ) {
        $log->log_debug("[spider_node_sql] Skip sql action for service $node  ",1);   
        return 0;
    }
    if (
        $self->{mode} ne "router"
        && (   $self->{status} eq "master"
            || $self->{status} eq "slave"
            || $ddlallnode == 1 )
      )
    {

        try {
            my $dsn =
                "DBI:mysql:database="
              . $database
              . ";host="
              . $self->{ip}
              . ";port="
              . $self->{mysql_port}
              . ";mysql_connect_timeout="
              . $mysql_connect_timeout;
            my $dbh =
              DBI->connect( $dsn, $self->{mysql_user},
                $self->{mysql_password} );
            print STDERR $node . ": spider_node_sql : " . $query . "\n";
            $dbh->do($query);

            if ( $ddlallnode == 0 ) {

                my $unquote_tbl = $createtable;
                $unquote_tbl =~ s/`//g;
                my $requete =
                "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE table_name='$unquote_tbl' and table_schema='$database' and COLUMN_KEY='PRI' LIMIT 1 ";
                $log->log_debug("[spider_node_sql] $dsn: $requete ",2);    
                my $sth = $dbh->prepare($requete);
                $sth->execute();
                while ( my @row = $sth->fetchrow_array ) {
                    $hashcolumn = $row['0'];
                }
            }
            else {

                #	my $sth = $dbh->prepare($query);
                #	$sth->execute();
                #	$sth->finish;
            }
            $dbh->disconnect;
        }
        catch Error with {
            $err = "ER0003";
        }
    }
}

sub spider_create_table_info($$) {
    my $query      = shift;
    my $ddlallnode = shift;
    my $err        = "000000";
    my $host_info;
    my $engine     = "";
    my $monitoring = "5012";

    if ( $ddlallnode == 0 ) {
        $log->log_debug("[spider_create_table_info] Create spider table on all proxy node ",2);
        my $cptpart = 0;
        foreach my $host ( keys( %{ $config->{db} } ) ) {
            $host_info = $config->{db}->{default};
            $host_info = $config->{db}->{$host};
            if ( $host_info->{status} eq "master" ) {
                if ( $cptpart ne 0 ) { $engine = $engine . ",\n"; }
                my $peer_host_info = $host_info->{peer}[0];
                $createtable =~ s/`//g;
                $engine =
                    $engine
                  . "\n partition pt"
                  . $cptpart
                  . " values in ("
                  . $cptpart
                  . ") comment ' table \""
                  . $createtable
                  . "\", host \""
                  . $host_info->{ip} . " "
                  . $config->{db}->{$peer_host_info}->{ip}
                  . "\", port \""
                  . $host_info->{mysql_port} . " "
                  . $config->{db}->{$peer_host_info}->{mysql_port} . "\"  '";

# $engine=$engine . "\n partition pt" . $cptpart ." values in (".$cptpart.") comment ' table \"".  $createtable ."\", host \"".$host_info->{ip} ." " .  $config->{db}->{$peer_host_info}->{ip} ."\", port \"".$host_info->{mysql_port} ." " .  $config->{db}->{$peer_host_info}->{mysql_port} .  "\" ,mbk \"2\", mkd \"2\", msi \"" . $monitoring  . "\" '";
                $cptpart++;
            }
        }
        $engine =
            " engine=Spider Connection 'user \""
          . $host_info->{mysql_user}
          . "\",password \""
          . $host_info->{mysql_password}
          . "\" '\n partition by list(mod("
          . $hashcolumn . ","
          . $cptpart . "))("
          . $engine . ")";
        foreach my $host ( keys( %{ $config->{db} } ) ) {
            $host_info = $config->{db}->{default};
            $host_info = $config->{db}->{$host};
            if (   $host_info->{mode} eq "spider"
                || $host_info->{mode} eq "monitor" )
            {
                my $dsn =
                    "DBI:mysql:database="
                  . $database
                  . ";host="
                  . $host_info->{ip}
                  . ";port="
                  . $host_info->{mysql_port}
                  . ";mysql_connect_timeout="
                  . $mysql_connect_timeout;
                my $dbh = DBI->connect(
                    $dsn,
                    $host_info->{mysql_user},
                    $host_info->{mysql_password}
                );
                my $requete = $query . " \n" . $engine . "\n";
                $log->log_debug("[spider_create_table_info] $dsn: $requete ",2); 
               
                my $sth = $dbh->do($requete);
            }
        }
    }
    if ( $ddlallnode == 2 ) {
        service_sql_database($query);
    }
    return $err;
}



sub service_sql_database($) {
    my $query = shift;
    my $host_info;
    my $err = "000000";
    $log->log_debug("[service_sql_database] start ",2); 
    foreach my $host ( keys( %{ $config->{db} } ) ) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        if ( $host_info->{mode} eq "spider" ) {
            use Error qw(:try);
            my $dsn =
                "DBI:mysql:database="
              . $database
              . ";host="
              . $host_info->{ip}
              . ";port="
              . $host_info->{mysql_port}
              . ";mysql_connect_timeout="
              . $mysql_connect_timeout;
            try {
                my $TRIG =
                    "$SKYDATADIR/$host/my sql -h"
                  . $host_info->{ip} . " -P "
                  . $host_info->{mysql_port} . " -u"
                  . $host_info->{mysql_user}
                  . "  -p$host_info->{mysql_password} -e\""
                  . $query . "\"";
                $log->log_debug("[service_sql_database] $TRIG: $query ",2);   
                Scramble::ClusterTransport::worker_node_command( $TRIG, $host_info->{ip} ,$log);
            
            }
            catch Error with {
                $log->log_debug("[service_sql_database] Failed ",2);   
                $err = "ER0003";
            }

        }
    }
    return $err;
}

sub service_remove_database($$) {
  my $self = shift;
  my $node = shift;
  my $err = "000000";
  my $param ="$SKYDATADIR/$node/stop";
  $log->log_debug("[service_remove_database] $self->{ip}: $param ",2);   
  $err   = Scramble::ClusterTransport::worker_node_command( $param, $self->{ip} ,$log);
  $param = "rm -rf $SKYDATADIR/$node";
  $err   = Scramble::ClusterTransport::worker_node_command( $param, $self->{ip},$log );
  $log->log_debug("[service_remove_database] $self->{ip}: $param ",2); 
  return $err;
}

sub service_status_mycheckpoint($$$) {
  my $host_vip=shift;
  my $host_info=shift;
  my $host=shift;  
  my $mon= Scramble::ClusterUtils::get_active_monitor($config);
  my $err; 
  my $cmd =
            $SKYBASEDIR
          . "/mycheckpoint/bin/mycheckpoint -o  --database=mon_"
          . $host
          . " --monitored-user="
          . $host_info->{mysql_user}
          . " --monitored-password="
          . $host_info->{mysql_password}
          . " --monitored-host="
          . $host_info->{ip}
          . " --monitored-port="
          . $host_info->{mysql_port}
          . " --user="
          . $host_vip->{mysql_user}
          . " --password="
          . $host_vip->{mysql_password}
          . " --host="
          . $host_vip->{ip}
          . " --port="
          . $host_vip->{mysql_port}
          . " --disable-bin-log"
          . " --purge-days=" 
          . $mon->{purge_days};
        $log->log_debug("[service_status_mycheckpoint] $host_info->{ip}: $cmd ",2);    
        $err = Scramble::ClusterTransport::worker_node_command( $cmd, $host_info->{ip} ,$log);
        if(  $err eq "00000" ){
            return 1;
        }    
        else {
         return 0;
        } 
}

sub service_status_memcache_fromdb($) {
        
      my $host_info=shift;
      my  $dsn2 =
            "DBI:mysql:host="
          . $host_info->{ip}
          . ";port="
          . $host_info->{mysql_port}
          . ";mysql_connect_timeout="
          . $mysql_connect_timeout;
       $log->log_debug("[service_status_memcache_fromdb] start : $dsn2",2);   
        
       my  $dbh2 = DBI->connect(
            $dsn2,
            $host_info->{mysql_user},
            $host_info->{mysql_password},
           {RaiseError=>0,PrintError=>1}
        );
        if (!$dbh2)  { 
          $log->log_debug("[service_status_memcache_fromdb] Database connection failed : $dsn2 ",2);   
          return 0;
        }
        my $sql="SELECT memc_set('test','test',0)";
        my $res = 0;
       
        my $sth2 = $dbh2->do($sql);
        if (!$sth2)  {    
              $log->log_debug("[service_status_memcache_fromdb] memc_set failed ",2); 
            
        }
        $sql="SELECT memc_get('test') as c1";
          
        if ( my $stm = $dbh2->prepare($sql)){
                $stm->execute; 
                my($result) = $stm->fetchrow_array();
                $result= defined ($result) ? $result : "no";
                 
                if  ( $result eq "test" )   {
                   $log->log_debug("[service_status_memcache_fromdb] memc_get success ",2); 
                   $res=1;
                } 
                $stm->finish(); 
           }  
         else {
             $dbh2->disconnect;
             $log->log_debug("[service_status_memcache_fromdb] memc_get failed ",2);
             return 0;    
          }
           
          
        if ($res == 0 ) {
             
            my $mem_info= Scramble::ClusterUtils::get_active_memcache($config);
            $sql="SELECT memc_servers_set('". $mem_info->{ip} .":". $mem_info->{port}."')";
            $log->log_debug("[service_status_memcache_fromdb] ". $mem_info->{ip} .":". $mem_info->{port}.": memc_servers_set ",2);      
            try {
             my $sth = $dbh2->do($sql);
            }
            catch Error with {
                 $log->log_debug("[service_status_memcache_fromdb] ". $mem_info->{ip} .":". $mem_info->{port}.": failed memc_servers_set ",2);
                 print STDERR "Mon connect memc_servers_set\n";
                 $dbh2->disconnect;
                return 0;

            };  
        }
        $dbh2->disconnect;
        return 1;
}

sub service_status_database($$) {
  my $self = shift;
  my $node = shift;
  my $err = "000000";
  my $port = "3306";
    if (   $self->{mode} eq "mysql-proxy"
        || $self->{mode} eq "keepalived"
        || $self->{mode} eq "haproxy" )
    {
        $port = $self->{port};
    }
    else {
        $port = $self->{mysql_port};
    }
    my $theip = $self->{ip};
    if ( $self->{mode} eq "keepalived" ) {
        $theip = $self->{vip};
    }
    my $dsn =
        "DBI:mysql:database=mysql;host="
      . $theip
      . ";port="
      . $port
      . ";mysql_connect_timeout="
      . $mysql_connect_timeout;
    $log->log_debug("[service_status_database] $theip: dsn ",2);
    use Error qw(:try);
    try {

        my $dbh =
          DBI->connect( $dsn, $self->{mysql_user},
            $self->{mysql_password} ,{ mysql_no_autocommit_cmd => 1});
        my $param = "SELECT 1";
        $dbh->do($param);
    }
    catch Error with {
        $err = "ER0003";
        $log->log_debug("[service_status_database] $theip: Failed connecting to db ",2);
    
    };
 return $err;
}

sub service_status_memcache($$) {
  my $self = shift;
  my $node = shift;
  my $err = "000000";
  use Error qw(:try);
  $log->log_debug("[service_status_memcache] $self->{ip}: Connecting to memcache "  ,2);  

  try {

    my $memd = new Cache::Memcached {
        'servers' => [ $self->{ip} . ":" . $self->{port} ],
        'debug'   => 0,
        'compress_threshold' => 10_000,
    };

    $memd->set( "key_test", "test" );
    my $val = $memd->get("key_test");
    $err = "ER0005";
    if ( $val eq "test" ) { $err = "000000"; 
        $log->log_debug("[service_status_memcache] $self->{ip}: Success to memcache "  ,1); 
    }
    
  }  catch Error with {
    $err = "ER0005";
    $log->log_debug("[service_status_memcache] $self->{ip}: Error to memcache "  ,1); 
  };
  return $err;
}

sub service_start_memcache($$) {
  my $self = shift;
  my $node = shift;
  my $err = "ER0019";

    # delete_pid_if_exists($SKYBASEDIR."/ncc/tmp/memcached.". $node .".pid",$self->{ip});
  my $param =
        $SKYBASEDIR
      . "/ncc/init.d/start-memcached "
      . $SKYBASEDIR
      . "/ncc/etc/memcached."
      . $node . ".cnf";
  $log->log_debug("[service_start_memcache] on $self->{ip}: ". $param  ,1);     
  my $res = Scramble::ClusterTransport::worker_node_command( $param, $self->{ip} ,$log);
  if ( $res ne "000000")   { $err=$res;}  
    
  return $err;
}

sub service_start_tarantool($$) {
  my $self = shift;
  my $node = shift;
  my $err = "ER0019";

   
  my $param =
        $SKYBASEDIR
      . "/tarantool/bin/tarantool_box -B -c "
      . $SKYBASEDIR
      . "/ncc/etc/tarantool."
      . $node . ".cnf";
  $log->log_debug("[service_start_tarantool] on $self->{ip}: ". $param  ,1);  
  my $res = Scramble::ClusterTransport::worker_node_command( $param, $self->{ip},$log );
  if ( $res ne "000000")   { $err=$res;}  
  return $err;
}



sub service_start_database($$) {
    my $self = shift;
    my $node = shift;   
    my $err = "ER0019";
   
    my $param = "$SKYDATADIR/$node/send_kill";
    my $res = Scramble::ClusterTransport::worker_node_command( $param, $self->{ip},$log );
    $log->log_debug("[service_start_database] on $self->{ip}: ". $param  ,1);        
    if ( $res ne "000000")   { $err=$res;}  

    $param = "$SKYDATADIR/$node/start";
    $res = Scramble::ClusterTransport::worker_node_command( $param, $self->{ip},$log );
    $log->log_debug("[service_start_database] on $self->{ip}: ". $param  ,1);        
    if ( $res ne "000000")   { $err=$res;}
 
    my $memcaches =  Scramble::ClusterUtils::get_all_memcaches($config);
    $param =
    "$SKYDATADIR/$node/my sql -uroot -p$self->{mysql_password} -e\""
          . "SELECT memc_servers_set('"
          . $memcaches . "');\"";
          
    $log->log_debug("[service_start_database] on $self->{ip}: ". $param  ,1);        
    $res = Scramble::ClusterTransport::worker_node_command( $param, $self->{ip} ,$log);
    if ( $res ne "000000")   { $err=$res;}
    return $err;
}

sub service_start_mycheckpoint($$) {
  my $self = shift;
  my $node = shift;
  my $host=get_active_master_db_name($config);
  my $host_info = $config->{db}->{$host};  
  my $err = "000000";
  
  my $cmd =
            $SKYBASEDIR
          . "/mycheckpoint/bin/mycheckpoint http "
          . " --user="
          . $host_info->{mysql_user}
          . " --password="
          . $host_info->{mysql_password}
          . " --host="
          . $host_info->{ip}
          . " --port="
          . $host_info->{mysql_port}
          . " --database=mon_"
          . $host
          . " --http-port=80 &";
   $log->log_debug("[service_start_mycheckpoint] on $self->{ip}: ". $cmd  ,1);           
   my $res = Scramble::ClusterTransport::worker_node_command( $cmd , $self->{ip} ,$log);
   if ( $res ne "000000")   { $err=$res;}  
   return $err;
}

sub service_start_mysqlproxy($$) {
  my $self = shift;
  my $node = shift;
  my $err = "000000";  
  my $param =
            $SKYBASEDIR
          . "/mysql-proxy/bin/mysql-proxy --daemon --defaults-file=$SKYBASEDIR/ncc/etc/mysql-proxy."
          . $node
          . ".cnf --pid-file="
          . $SKYDATADIR
          . "/tmp/mysql-proxy."
          . $node
          . ".pid --log-file="
          . $SKYDATADIR
          . "/log/mysql-proxy."
          . $node . ".log  1>&2 ";
  $log->log_debug("[service_start_mysqlproxy] on $self->{ip}: ". $param  ,1);    
  my $res = Scramble::ClusterTransport::worker_node_command( $param, $self->{ip} ,$log);
  if ( $res ne "000000")   { $err=$res;}    
  return $err;
}

sub service_start_keepalived($$) {
  my $self = shift;
  my $node = shift;
  my $err = "ER0019";  
  my $param =
            $SKYBASEDIR
          . "/keepalived/sbin/keepalived -f  "
          . $SKYBASEDIR
          . "/ncc/etc/keepalived."
          . $node . ".cnf";
  $log->log_debug("[service_start_keepalived] on $self->{ip}: ". $param  ,1);                
  my $res = Scramble::ClusterTransport::worker_node_command( $param, $self->{ip},$log );
  if ( $res ne "000000")   { $err=$res;}  
  return $err;
}

sub service_start_haproxy($$) {
  my $self = shift;
  my $node = shift;
  my $err = "ER0019";  
  my $param =
            $SKYBASEDIR
          . "/haproxy/sbin/haproxy -f "
          . $SKYBASEDIR
          . "/ncc/etc/haproxy."
          . $node
          . ".cnf -p "
          . $SKYDATADIR
          . "/tmp/haproxy."
          . $node . ".pid ";
  $log->log_debug("[service_start_haproxy] on $self->{ip}: ". $param  ,1);        
  my $res = Scramble::ClusterTransport::worker_node_command( $param, $self->{ip},$log );
  if ( $res ne "000000")   { $err=$res;}  
  return $err;
}

sub service_start_bench($$$$) {
    my $self = shift;
    my $node = shift;
    my $type = shift;
    my $bench_info= Scramble::ClusterUtils::get_active_lb($config);
    my $err = "ER0019";
   
 #  @abs_top_srcdir@/bin/client -u skysql -h 192.168.0.10 -a skyvodka -f -c 10 -s 10 -d dbt2 -l 3306  -o @abs_top_srcdir@/scripts/output/10/client
     
  # my $cmd=$SKYBASEDIR."/dbt2/bin/client  -c ".$self->{concurrency}." -d ".$self->{duration}." -n -w ".$self->{warehouse}." -s 10 -u ".$bench_info->{mysql_user}." -x ".$bench_info->{mysql_password} ." -H". $bench_info->{vip};
    
    my $cmd =
      $SKYBASEDIR
      . "/dbt2/bin/client -u "
      . $bench_info->{mysql_user} . " -h "
      . $bench_info->{vip} . " -a "
      . $bench_info->{mysql_password}
      . " -f -c "
      . $self->{concurrency}."  -s "
      . " 2 -d dbt2 -l "
      . $bench_info->{port} . "  -o  "
      . $SKYDATADIR
      ."/" 
      . $node
      ."/client &";
        $log->log_debug("[service_start_bench] on $self->{ip}: ". $cmd  ,1);
    $err = Scramble::ClusterTransport::worker_node_command( $cmd, $self->{ip},$log );
     #   @abs_top_srcdir@/src/driver -d localhost -l 100 -wmin 1 -wmax 10 -w 10 -sleep 10 -outdir @abs_top_srcdir@/scripts/output/10/driver -tpw 10 -ktd 0 -ktn 0 -kto 0 -ktp 0 -kts 0 -ttd 0 -ttn 0 -tto 0 -ttp 0 -tts 0
    $cmd =
      $SKYBASEDIR
      . "/dbt2/src/driver  "
      . "-d 127.0.0.1 -l "
      . $self->{duration} 
      ." -wmin 1 -wmax "
      . $self->{warehouse} 
      ."  -w "
      . $self->{warehouse}  
      . " -sleep 1 -outdir "
      . $SKYDATADIR
      ."/" 
      . $node
      ."/driver"; 
      $log->log_debug("[service_start_bench] on $self->{ip}: ". $cmd  ,1); 
     $err = Scramble::ClusterTransport::worker_node_command( $cmd, $self->{ip} ,$log); 
     $cmd="killall client";
     $log->log_debug("[service_start_bench] on $self->{ip}: ". $cmd  ,1);
     $err = Scramble::ClusterTransport::worker_node_command( $cmd, $self->{ip},$log ); 
     my $outfile =$SKYDATADIR. "/" .$node."/driver/mix.log";
     my $json_res = dbt2_parse_mix( $outfile );
     return $json_res;
}

sub service_stop_database($$) {
    my $self = shift;
    my $node = shift;
    my $err = "ER0020";

    my $param = "$SKYDATADIR/$node/send_kill";
    $log->log_debug("[service_stop_database] on $self->{ip}: ". $param  ,1);
    $err = Scramble::ClusterTransport::worker_node_command( $param, $self->{ip},$log );
    return $err;
}
sub service_stop_mysqlproxy($$) {
  my $self = shift;
  my $name = shift;
  my $err = "ER0020";
    
  my $param =
            "kill -9 `cat "
          . $SKYDATADIR
          . "/tmp/mysql-proxy."
          . $name . ".pid`";
  $log->log_debug("[service_stop_mysqlproxy] on $self->{ip}: ". $param  ,1);   
  $err = Scramble::ClusterTransport::worker_node_command( $param, $self->{ip},$log );
  return $err;
}

sub service_stop_memcache($$) {
  my $self = shift;
  my $name = shift;
  my $err = "ER0020";
  my $param =
            "kill -9 `cat "
          . $SKYDATADIR
          . "/tmp/memcached."
          . $name . ".pid`";
  $log->log_debug("[service_stop_memcache] on $self->{ip}: ". $param  ,1);         
  $err = Scramble::ClusterTransport::worker_node_command( $param, $self->{ip} ,$log);
  return $err;
}

sub service_stop_keepalived($$) {
  my $self = shift;
  my $name = shift;
  my $err = "ER0020";
  my $param = "killall keepalived ";
  $log->log_debug("[service_stop_keepalived] on $self->{ip}: ". $param  ,1);  
  $err = Scramble::ClusterTransport::worker_node_command( $param, $self->{ip} ,$log);
  return $err;
}

sub service_stop_haproxy($$) {
  my $self = shift;
  my $name = shift;
  my $err = "ER0020";
  my $param =
          "kill -9 `cat " . $SKYDATADIR . "/tmp/haproxy." . $name . ".pid`";
  $log->log_debug("[service_stop_haproxy] on $self->{ip}: ". $param  ,1);           
  $err = Scramble::ClusterTransport::worker_node_command( $param, $self->{ip},$log );
  return $err;
}

sub service_stop_mycheckpoint($$) {
  my $self = shift;
  my $name = shift;
  my $err = "ER0020";
  my $param =
            "kill -9 `cat "
          . $SKYDATADIR
          . "/tmp/mycheckpoint."
          . $name . ".pid`";
  $log->log_debug("[service_stop_mycheckpoint] on $self->{ip}: ". $param  ,1);         
  $err = Scramble::ClusterTransport::worker_node_command( $param, $self->{ip} ,$log);
   
  return $err;
}

sub service_install_bench($$) {
 my $self=shift;
 my $name = shift;
 my $cmd="mkdir ". $SKYDATADIR ."/".$name;
 $log->log_debug("[service_install_bench]  ". $cmd  ,1);      
 
 Scramble::ClusterTransport::worker_node_command( $cmd, $self->{ip} ,$log);
 $cmd="mkdir ". $SKYDATADIR ."/".$name."/client";
 $log->log_debug("[service_install_bench]  ". $cmd  ,1);      
 Scramble::ClusterTransport::worker_node_command( $cmd, $self->{ip},$log );
 $cmd="mkdir ". $SKYDATADIR ."/".$name."/driver";
 $log->log_debug("[service_install_bench]  ". $cmd  ,1);      
 
 Scramble::ClusterTransport::worker_node_command( $cmd, $self->{ip},$log );
 
 
 $cmd=$SKYBASEDIR
      . "/dbt2/bin/datagen  -w "
      .  $self->{warehouse}
      . " -d "
      . $SKYDATADIR 
      ."/".$name
      ." --mysql";
 $log->log_debug("[service_install_bench]  ". $cmd  ,1);      
 
 Scramble::ClusterTransport::worker_node_command( $cmd, $self->{ip} ,$log);
 my $master=  Scramble::ClusterUtils::get_active_db($config);
 mysql_do_command($master,"DROP DATABASE IF EXISTS dbt2");
 $log->log_debug("[service_install_bench] DROP DATABASE IF EXISTS dbt2 on master "  ,1);      
 

 $cmd=$SKYBASEDIR
      . "/dbt2/scripts/mysql/build_db.sh -w "
      .  $self->{warehouse}
      . " -d dbt2 -f"
      . $SKYDATADIR 
      ."/".$name
      ." -s /tmp/mysql_sandbox" 
      .$master->{mysql_port}
      .".sock -u " 
      .$master->{mysql_user}
      ." -h ".$master->{ip}
      ." -P ".$master->{mysql_port}
      ." -p".$master->{mysql_password}
      ." -l -e INNODB"; 
 $log->log_debug("[service_install_bench]  ". $cmd  ,1);      
 Scramble::ClusterTransport::worker_node_command( $cmd, $self->{ip},$log );
 
# "/usr/local/skysql/dbt2/bin/datagen -w 3 -d /var/lib/skysql/dbt2 --mysql"
# /usr/local/skysql/dbt2/scripts/mysql/build_db.sh -w 3 -d dbt2 -f /var/lib/skysql/dbt2/ -s /tmp/mysql_sandbox5010.sock -u skysql  -pskyvodka -e INNODB
}

sub service_install_tarantool($$) {
  my $self = shift;
  my $node = shift;
  my $err = "000000";
  my $param =
        "mkdir "
      .  $SKYDATADIR 
      . "/"
      . $node ;
  $log->log_debug("[service_install_tarantool]  ". $param  ." on: ". $self->{ip},1);      
  $err = Scramble::ClusterTransport::worker_node_command( $param, $self->{ip},$log );
  $param =
        "chown skysql:skysql "
      . $SKYDATADIR 
      . "/"
      . $node;
  $err = Scramble::ClusterTransport::worker_node_command( $param, $self->{ip},$log);
  return $err;
}

sub service_install_mycheckpoint($$) {
  my $host_info=shift;
  my $db_name =shift;
  my $sql = "CREATE DATABASE if not exists ".$db_name ;
  my $err=  mysql_do_command($host_info,$sql);
  return $err;  
}

sub service_install_database($$$) {
    my $self = shift;
    my $node = shift;
    my $type = shift;
    my $err  = "000000";
    
    my $SANDBOX =
        "low_level_make_sandbox --force --no_show" . "  "
      . " -c server-id=$self->{mysql_port} "
      . " --db_user=$self->{mysql_user} "
      . " --db_password=$self->{mysql_password} "
      . " -d $node "
      . " --sandbox_port=$self->{mysql_port} "
      . " --upper_directory=$SKYDATADIR "
      . " --install_version=5.5 --no_ver_after_name "
      . " --basedir=$SKYBASEDIR/$self->{mysql_version} "
      . " --my_file=$SKYBASEDIR/ncc/etc/$self->{mysql_cnf}";

    
    $log->log_debug("[service_install_database] $SANDBOX : ".$self->{ip},1);
    my $result = Scramble::ClusterTransport::worker_node_command( $SANDBOX, $self->{ip} ,$log);
    
    my $GRANT =
    "$SKYDATADIR/$node/my sql -uroot -p$self->{mysql_password} -e\""
      . "GRANT replication slave ON *.* to \'$self->{replication_user}\'@\'%\' IDENTIFIED BY \'$self->{replication_password}\';"
      . "GRANT ALL ON *.* to \'$self->{mysql_user}\'@\'%\' IDENTIFIED BY \'$self->{mysql_password}\';"
      . "\"";
    $log->log_debug("[service_install_database] Grant: $GRANT : ".$self->{ip},1);  
    Scramble::ClusterTransport::worker_node_command( $GRANT, $self->{ip},$log );

    if ( $type eq 'spider' || $type eq 'monitor' ) {
    
        my $SPIDER =
        "$SKYDATADIR/$node/my sql -uroot -p$self->{mysql_password} < $SKYBASEDIR/ncc/scripts/install_spider.sql";
        $log->log_debug("[service_install_database] Spider : $SPIDER : ".$self->{ip},1);  
        $err = Scramble::ClusterTransport::worker_node_command( $SPIDER, $self->{ip} ,$log);
        

    }
    else {
        my $HANDLERSOCKET =
    "$SKYDATADIR/$node/my sql -uroot -p$self->{mysql_password} -e\"INSTALL PLUGIN handlersocket SONAME \'handlersocket.so\';\"";
        $log->log_debug("[service_install_database] HandlerSocket : $HANDLERSOCKET : ".$self->{ip},1);  
       
        $err = Scramble::ClusterTransport::worker_node_command( $HANDLERSOCKET, $self->{ip},$log );
    }
    my $GEARMANUDF =
    "$SKYDATADIR/$node/my sql -uroot -p$self->{mysql_password} < $SKYBASEDIR/ncc/scripts/gearmanudf.sql";
    $log->log_debug("[service_install_database] HandlerSocket : $GEARMANUDF : ".$self->{ip},1);  
       
    $err = Scramble::ClusterTransport::worker_node_command( $GEARMANUDF, $self->{ip},$log );

    my $param =
    "$SKYDATADIR/$node/my sql -uroot -p$self->{mysql_password} -e\""
      . "SELECT gman_servers_set('127.0.0.1');\"";
    $log->log_debug("[service_install_database] Gearman : $param : ".$self->{ip},1);  
    $err = Scramble::ClusterTransport::worker_node_command( $param, $self->{ip} ,$log);

    my $MEMCACHEUDF =
    "$SKYDATADIR/$node/my sql -uroot -p$self->{mysql_password} < $SKYBASEDIR/ncc/scripts/install_memcacheudf.sql";
    $log->log_debug("[service_install_database] Memcache : $MEMCACHEUDF : ".$self->{ip},1);  
    
    $err = Scramble::ClusterTransport::worker_node_command( $MEMCACHEUDF, $self->{ip},$log );

    my $GTTID =
   "$SKYDATADIR/$node/my sql -uroot -p$self->{mysql_password} < $SKYBASEDIR/ncc/scripts/gttid.sql";
    $log->log_debug("[service_install_database] Table Global Unique Id : $GTTID : ".$self->{ip},1);  
    
    $err = Scramble::ClusterTransport::worker_node_command( $GTTID, $self->{ip},$log );
    my $memcaches =  Scramble::ClusterUtils::get_all_memcaches($config);

    $param =
    "$SKYDATADIR/$node/my sql -uroot -p$self->{mysql_password} -e\""
      . "SELECT memc_servers_set('"
      . $memcaches . "');\"";
    $log->log_debug("[service_install_database] test memcache set : $param: ".$self->{ip},1);  
      
    $err = Scramble::ClusterTransport::worker_node_command( $param, $self->{ip} ,$log);

   # $log->report_status( $self, "Install node", $err, $node );
    return $err;
}

sub service_sync_database($$$) {
   my $self   = shift;
   my $node   = shift;
   my $master = shift;
   $log->log_debug("[service_sync_database] on master ip : ". $master->{ip},1);
   my $param =
    "mysql -h$self->{ip} ".
    "-P$self->{mysql_port} ".
    "-u $self->{mysql_user} ".
    "-p$self->{mysql_password} ".
    "-e \'stop slave;".
    "change master to master_host=\"$master->{ip}\",".
    "master_user=\"$master->{replication_user}\",".
    "master_password=\"$master->{replication_password}\",".
    "master_port=$master->{mysql_port};\';".
    "mysqldump -u $self->{mysql_user} ".
    "-p$self->{mysql_password} ".
    "-h $master->{ip} -P$master->{mysql_port} ".
    "--single-transaction --master-data --all-databases ".
    "| mysql ".
    "-u $self->{mysql_user} ".
    "-p$self->{mysql_password} ".
    "-h$self->{ip} ".
    "-P$self->{mysql_port};".
    "mysql -h$self->{ip} ".
    "-P$self->{mysql_port} ".
    "-u $self->{mysql_user} ".
    "-p$self->{mysql_password} ".
    "-e \'start slave;\'";

    Scramble::ClusterTransport::worker_node_command( $param, $self->{ip} ,$log);
}


sub instance_check_ssh($){
    my $ip =shift;
    my $command="ssh -q -i "
    . $SKYDATADIR
    . $sshkey. " "
    . ' -o "BatchMode=yes"  -o "StrictHostKeyChecking=no" '
    . $ip
    .' "echo 2>&1" && echo "OK" || echo "NOK"';
    my  $result = `$command`;
    $log->log_debug("[instance_check_ssh] check ip : ". $ip,2);
     
 
   $result =~ s/\n//g; 
     if ( $result eq "OK"){ 
         $log->log_debug("[instance_check_ssh] ssh ok on ip : ". $ip,2);
         return 1;
    }
    $log->log_debug("[instance_check_ssh] ssh failed on ip : ". $ip,2);
 
    return 0;
}

sub instance_check_scrambledb($){
    my $ip =shift;
    $log->log_debug("[instance_check_scrambledb] check ip : ". $ip,2);
   
    my $command=
    ' "echo 2>&1" && echo "OK" || echo "NOK"';
    my  $result = Scramble::ClusterTransport::worker_node_command($command,$ip,$log);
  
     if ( $result eq "000000"){ 
       $log->log_debug("[instance_check_scrambledb] is running on ip : ". $ip,2);
   
       return 1;
    }
    $log->log_debug("[instance_check_scrambledb] is not running on ip : ". $ip,2);
   
  return 0;
}

sub instance_heartbeat_collector($$) {
    my $status =shift;
    my $json_status = shift ;
    my $json    = new JSON;
    my $err = "000000";
    my $host_info;
    my $host_vip =  Scramble::ClusterUtils::get_active_db($config);
    $log->log_debug("[Instance_hearbeat_collector] Heartbeat fetch active database: ". $host_vip,2);
   
    my $source_ip = Scramble::ClusterUtils::get_source_ip_from_status($status) ;
    $log->log_debug("[Instance_hearbeat_collector] Heartbeat receive from ip: ". $source_ip,2);
           
   
    my $mem_info= Scramble::ClusterUtils::get_active_memcache($config);
    $log->log_debug("[Instance_hearbeat_collector] Process the ping with memcache: ". $mem_info->{ip} . ":" . $mem_info->{port},2);
            
    
      my $memd = new Cache::Memcached {
           'servers' => [ $mem_info->{ip} . ":" . $mem_info->{port} ],
           'debug'   => 0,
           'compress_threshold' => 10_000,
        };
        
   #     use Error qw(:try);
   #     try {
   
          my $previous_json_status = $memd->get( "status".  $source_ip);
        
       
        if ($previous_json_status )
       { 
           $log->log_debug("[Instance_hearbeat_collector] Previous_json_status ",2);
           $log->log_json($previous_json_status,2);
           my $previous_status = $json->allow_nonref->utf8->relaxed->escape_slash->loose
           ->allow_singlequote->allow_barekey->decode($previous_json_status);
           
           my $json_triggers =  get_status_diff($previous_status,\$status);
           Scramble::ClusterTransport::worker_doctor_command($json_triggers,$gearman_ip);
           $json_status= $json->allow_blessed->convert_blessed->encode($status);
           $log->log_debug("[Instance_hearbeat_collector] After diff status ",2);
           $log->log_json($json_status,2);
           
       }     
       
    #}  catch Error with {
    #     print STDERR "\nbogus get bogus\n";
    #    $err = "ER0011";
    #};

    try {
        $log->log_debug("[Instance_hearbeat_collector] Storing hearbeat to memcache ",2);
        $memd->set( "status".   $source_ip, $json_status );
        $memd->set( "status", $json_status );
        
        
       }  catch Error with {
       $log->log_debug("[Instance_hearbeat_collector] Can't set status in memcache ",2);
       $err = "ER00012";
    };
    my $json_todo = $memd->get("actions");
    if (!$json_todo )
    {
              $log->log_debug("[Instance_hearbeat_collector] No actions in memcache heartbeat setting empty json ",2);
              $memd->set( "actions", '{"return":{"error":"000000","version":"1.0"},"actions":[]}' );
    }
    my $cloud_name = Scramble::ClusterUtils::get_active_cloud_name($config);
    foreach my $host ( keys( %{ $config->{db} } ) ) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
       if ($host_info->{cloud} eq $cloud_name){ 
        
        # Check memcache_udf
        service_status_memcache_fromdb($host_info);
        service_install_mycheckpoint( $host_vip , "mon_" . $host);
        service_status_mycheckpoint($host_vip,$host_info,$host);
       } 
    }
    gttid_reinit();
    return $err;
    
}


sub database_rolling_restart(){ 
 my $host_info;
 my $host_slave;
 my $cloud_name = get_active_cloud_name($config);
 $log->log_debug("[database_rolling_restart] start ",1);   
 foreach my $host ( keys( %{ $config->{db} } ) ) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        if  ( $host_info->{mode} eq "slave" && $host_info->{cloud} eq $cloud_name ){
                $ret = service_do_command( $host_info, $host, "stop" );
                $host_slave=$host_info;
        }         
       
  }


 foreach my $host ( keys( %{ $config->{db} } ) ) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        if  ( $host_info->{status} eq "master" && $host_info->{cloud} eq $cloud_name){
           service_switch_database($host_slave); 
           $ret = service_do_command( $host_info, $host, "stop" );
       }         
       
  }
 
}

sub config_switch_status($$){
    my $switchhost = shift;
    my $status = shift;
    $log->log_debug("[config_switch_status] start ",1);
    
    my $file = "$SKYBASEDIR/ncc/etc/cloud.cnf";
    open my $in,  '<', $file       or die "Can't read old file: $!";
    open my $out, '>', "$file.new" or die "Can't write new file: $!";
    my $line = "";
    my $drap = 0;
    
    while ( $line = <$in> ) {

        # print STDERR $line . " ". $drap . "\n";
        if ( $line =~ /^\s*<\s*(\w+)\s+([\w\-_]+)\s*>\s*$/ ) {
            my $type = $1;
            my $name = $2;

            # print STDERR $name . $switchhost. "\n" ;
            if ( $name eq $switchhost ) {
                $drap = 1;
            }
            else {
                $drap = 0;
            }
        }

        if ( $drap == 1  && $status eq "master" ) {
            $line =~ s/^(.*)status(.*)slave(.*)$/\tstatus\t\t\t\tmaster/gi;

        }
        if( $drap == 1  && $status eq "slave" ) {
            $line =~ s/^(.*)status(.*)(slave|master)(.*)$/\tstatus\t\t\t\tslave/gi;
        }
        print $out $line;
        next;
    }
    system("rm -f $file.old");
    system("mv $file $file.old");
    system("mv $file.new $file");
    system("chmod 660 $file");
 }

sub service_switch_database($) {
    my $switchhost = shift;
    my $host_info;
    my $err = "000000";
    $log->log_debug("[service_switch_database] start ",1);
    my $cloud_name = get_active_cloud_name($config);
    my $oldhost =  Scramble::ClusterUtils::get_active_master_db_name($config);
    
    config_switch_status($oldhost,"slave");
    config_switch_status($switchhost,"master");
     
   
    stop_all_proxy();
    bootstrap_config();
    foreach my $host ( keys( %{ $config->{db} } ) ) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        if ( $host eq $switchhost && $host_info->{cloud} eq $cloud_name) {
            my $cmd =
                $SKYBASEDIR
              . "/perl/bin/perl "
              . $SKYBASEDIR
              . "/mha4mysql/bin/masterha_master_switch --master_state=alive --orig_master_is_new_slave  --conf="
              . $SKYBASEDIR
              . "/ncc/etc/mha.cnf --interactive=0 --new_master_host="
              . $host_info->{ip}
              . " --new_master_port="
              . $host_info->{mysql_port};
            system($cmd);
        }
    }
    start_all_proxy();
    return $err;
}
sub service_check_vip(){
  
  return 1;
  my $err   = "000000";
  my $ip= Scramble::ClusterUtils::get_my_ip_from_config($config);
  my $lb= Scramble::ClusterUtils::get_active_lb($config);  
  if (Scramble::ClusterUtils::is_ip_localhost($lb->{ip})==0)  {
    my $cmd2 ="/sbin/ifconfig lo:0 "
    . $lb->{vip} 
    ." broadcast"
    ." $lb->{vip}" 
    ." netmask 255.255.255.255 up";
    my $cmd1 ="/sbin/ifconfig lo:0 down";  
    Scramble::ClusterTransport::worker_node_command($cmd1,"localhost",$log);
    Scramble::ClusterTransport::worker_node_command($cmd2,"localhost",$log);

  }
   return  $err;
}



sub service_switch_vip(){
  my $err   = "000000";
  my $ip= Scramble::ClusterUtils::get_my_ip_from_config($config);
  my $newlb= Scramble::ClusterUtils::get_lb_name_from_ip($config,$ip);  
  my $oldlb= Scramble::ClusterUtils::get_lb_peer_name_from_ip($config,$ip);   
  $log->log_debug("[service_witch_vip] Current lb from config: ".$newlb ,1);
  $log->log_debug("[service_witch_vip] Passive lb from config: ".$oldlb ,1);
  


  
  $config->read($SKYBASEDIR."/ncc/etc/cloud.cnf");
  #if (Scramble::ClusterUtils::is_ip_localhost($ip)==1)  {
  #  print STDERR "nothing do do lb in the conf already master\n";
  # 
  #} else   {
  #  config_switch_status($oldlb,"master");
  #  config_switch_status($newlb,"slave");
  #  bootstrap_config();
  #}
  my $lb = Scramble::ClusterUtils::get_active_lb($config);
 

  my $cmd1 ="/sbin/ifconfig lo:0 down";
  print STDERR  $cmd1."\n";
  my $cmd2 ="/sbin/ifconfig lo:0 "
  . $lb->{vip} 
  ." broadcast"
  ." $lb->{vip}" 
  ." netmask 255.255.255.255 up";
  $log->log_debug("[service_witch_vip] ".$cmd2 ,1);
 
   my @ips =  Scramble::ClusterUtils::get_all_sercive_ips($config);
   foreach (@ips) {  
        #$log->log_debug("removing loopback on ". $_ ,1);  
        #Scramble::ClusterTransport::worker_node_command($cmd1 , $_);
        if (Scramble::ClusterUtils::is_ip_localhost($_)==0) {
            #print STDERR  "add loopback on ". $_ . "\n";  
            #Scramble::ClusterTransport::worker_node_command($cmd2 , $_);
        } else {
        
        }
   } 
   return $err; 
}

sub service_do_command($$$) {
    my $self = shift;
    my $node = shift;
    my $cmd  = shift;
    my $param = "";
    my $err   = "000000";
    $log->log_debug("[service_do_command] start " ,1);
      
    if ( $cmd eq "start" && Scramble::ClusterUtils::is_ip_localhost($self->{ip})==0) {
        # get the instances status from memcache 
        my $mem_info= Scramble::ClusterUtils::get_active_memcache($config);
        $log->log_debug("[service_do_command] Get the status in memcache: ". $mem_info->{ip} . ":" . $mem_info->{port}  ,1);
        my $memd = new Cache::Memcached {
               'servers' => [ $mem_info->{ip} . ":" . $mem_info->{port} ],
               'debug'   => 0,
               'compress_threshold' => 10_000,
        };

        my $cloud =  Scramble::ClusterUtils::get_active_cloud($config);
        my $json_cloud       = new JSON ;
        my $json_cloud_str = $json_cloud->allow_nonref->utf8->encode($cloud);

        my $json_status = $memd->get("status");
        if (!$json_status )
        {
           $log->log_debug("[service_do_command] No status in memcache :"  ,1);
           $log->report_status( $self, $param,  "ER0014", $node );
           return "ER0014";  
        }
        my $json_todo = $memd->get("actions");
        if (!$json_todo )
        {
           $log->log_debug("[service_do_command] No actions in memcache "  ,1);
           $log->report_status( $self, $param,  "ER0015", $node );
           return "ER0015";  
        }

        my $status = $json_cloud->allow_nonref->utf8->relaxed->escape_slash->loose
         ->allow_singlequote->allow_barekey->decode($json_status);

        my $todo =  $json_cloud->allow_nonref->utf8->relaxed->escape_slash->loose
         ->allow_singlequote->allow_barekey->decode($json_todo);

        $log->log_debug("[service_do_command] Local status :"  ,1);
        $log->log_json($json_status  ,1); 


        if ( Scramble::ClusterUtils::is_ip_from_status_running($status,$self->{ip})==0) {

        #use Cache::Memcached::Queue;

        #my $q = Cache::Memcached::Queue->new( name => 'actions_queue', 
        #      max_enq => 100, 
        #      servers => [{address => $mem_info->{ip} . ":" . $mem_info->{port}}, 
        #      id => 1,
        #             id_prefix => 'MYQUEUE',
        #)->init;

        my $my_event = {
                 event_ip       => "na",
                 event_type     => "cloud",
                 event_state    => "stopped" ,
                 do_level       => "instances" ,
                 do_group       =>  $self->{ip},
                 do_action      => "start" 
        };
        #$q->enq({value => $json_cloud->allow_blessed->convert_blessed->encode($my_event)});
        push  @{$todo->{actions}} , $my_event;  
        $my_event ={
                 event_ip       => $self->{ip},
                 event_type     => "instances",
                 event_state    => "running" ,
                 do_level       => "services" ,
                 do_group       => $node,
                 do_action      => "bootstrap_ncc" 
        };
        push  @{$todo->{actions}}, $my_event ; 
        $my_event ={
                 event_ip       => $self->{ip},
                 event_type     => "instances",
                 event_state    => "running" ,
                 do_level       => "services" ,
                 do_group       => $node,
                 do_action      => $cmd 
        };    
        push  @{$todo->{actions}} , $my_event;       

        $json_todo =  $json_cloud->allow_blessed->convert_blessed->encode($todo);
        
        $log->log_debug("[service_do_command] Delayed actions :" . $json_todo ,1);
    
        $memd->set( "actions",  $json_todo);
        $log->report_status( $self, $param,  "ER0016", $node );
        return "ER0016";
      }
    } 
    if ( $cmd eq "install" ) {
         if (   $self->{mode} eq "mariadb"
            || $self->{mode} eq "mysql"
            || $self->{mode} eq "spider")
            {
                $ret = service_install_database( $self, $node, $self->{mode} );
            } elsif ( $self->{mode} eq "dbt2" ) {
                $ret = service_install_bench( $self, $node)
            
            } elsif ( $self->{mode} eq "tarantool" ) {
                $ret = service_install_tarantool( $self, $node)
            }
    }
    elsif ( $cmd eq "remove" ) {
         if (   $self->{mode} eq "mariadb"
            || $self->{mode} eq "mysql"
            || $self->{mode} eq "spider")
        {
            $err=service_remove_database($self,$node);
        }
    }
    elsif ( $cmd eq "status" ) {

        if (   $self->{mode} eq "mariadb"
            || $self->{mode} eq "mysql"
            || $self->{mode} eq "spider"
            || $self->{mode} eq "mysql-proxy"
            || $self->{mode} eq "keepalived"
            || $self->{mode} eq "haproxy" )
        {
           $err=service_status_database($self,$node);
          
        }
        elsif ( $self->{mode} eq "memcache" ) {
           $err= service_status_memcache($self,$node);
        }
    }
    elsif (
        $cmd eq "start"
        && (   $self->{mode} eq "mariadb"
            || $self->{mode} eq "mysql"
            || $self->{mode} eq "spider"
           )
          ) {
        $err=service_start_database($self,$node);
    }
    elsif (
        $cmd eq "stop"
        && (   $self->{mode} eq "mariadb"
            || $self->{mode} eq "mysql"
            || $self->{mode} eq "spider"
           )
          ) {
        service_stop_database($self,$node);
    }                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
    elsif ( $cmd eq "start" && $self->{mode} eq "mysql-proxy" ) {
      $err=service_start_mysqlproxy($self,$node);   
    }
    elsif ( $cmd eq "stop" && $self->{mode} eq "mysql-proxy" ) {
      $err=service_stop_mysqlproxy($self,$node);  
    }
    elsif ( $cmd eq "start" && $self->{mode} eq "keepalived" ) {
      $err=service_start_keepalived($self,$node); 
    }
    elsif ( $cmd eq "stop" && $self->{mode} eq "keepalived" ) {
      $err=service_stop_keepalived($self,$node); 
    }
    elsif ( $cmd eq "start" && $self->{mode} eq "haproxy" ) {
      $err=service_start_haproxy($self,$node);     
    }
    elsif ( $cmd eq "stop" && $self->{mode} eq "haproxy" ) {
      $err=service_stop_haproxy($self,$node);         
    }
    elsif ( $cmd eq "start" && $self->{mode} eq "memcache" ) {
        $err=service_start_memcache($self,$node);   
    }
    elsif ( $cmd eq "stop" && $self->{mode} eq "memcache" ) {
        $err = service_stop_memcache($self,$node);
    }
    elsif ( $cmd eq "start" && $self->{mode} eq "mycheckpoint" ) {
        $err = service_start_mycheckpoint($self,$node);
    }
    elsif ( $cmd eq "stop" && $self->{mode} eq "mycheckpoint" ) {
        $err = service_stop_mycheckpoint($self,$node); 
    }
    elsif ( $cmd eq "start" && $self->{mode} eq "tarantool" ) {
        $err = service_start_tarantool($self,$node); 
    }
    elsif ( $cmd eq "stop" && $self->{mode} eq "tarantool" ) {
        $err = service_stop_tarantool($self,$node); 
    }
    elsif ( $cmd eq "start" && $self->{mode} eq "sphinx" ) {
        $err = service_stop_sphinx($self,$node); 
    }
    elsif ( $cmd eq "stop" && $self->{mode} eq "sphinx" ) {
        $err = service_stop_sphinx($self,$node); 
    }
    
    $log->report_status( $self, $param, $err, $node );
    return $err;
}



sub bootstrap_config() {
    $log->log_debug("[bootstrap_config] Start bootstrap_config" ,1);
    my $my_home_user = $ENV{HOME};
    my $cmd =
        'string="`cat '
      . $SKYBASEDIR
      . '/ncc/etc/id_rsa.pub`"; sed -e "\|$string|h; \${x;s|$string||;{g;t};a\\" -e "$string" -e "}" $HOME/.ssh/authorized_keys > $HOME/.ssh/authorized_keys2 ;mv $HOME/.ssh/authorized_keys $HOME/.ssh/authorized_keys_old;mv $HOME/.ssh/authorized_keys2 $HOME/.ssh/authorized_keys';
    my $err = "000000";

    my @ips =  Scramble::ClusterUtils::get_all_sercive_ips($config);
    foreach (@ips) {
        my $command =
          "{command:{action:'write_config',group:'localhost',type:'all'}}";
         my $action =  
        "scp -i "
              . $SKYDATADIR
              . $sshkey." "
              . $SKYBASEDIR
              . "/ncc/etc/cloud.cnf "
              . $_ . ":"
              . $SKYBASEDIR
              . "/ncc/etc" ;  
        $log->log_debug("[bootstrap_config] Calling system: " .$action ,1);
    
        system( $action);
        
         
         $action =  
        "scp -i "
              . $SKYDATADIR
              . $sshkey." "
              . $SKYDATADIR
              . $sshkey." "
              . $_ . ":"
              . $SKYDATADIR
              . "/.ssh" ;  
        $log->log_debug("[bootstrap_config] Calling system: " .$action ,1);
    
        system( $action);
          
        
        $err = Scramble::ClusterTransport::worker_config_command( $command, $_ ,$log);
        
    }
    $log->log_debug("[bootstrap_config] End bootstrap_config" ,1);
    return $err;
}

sub bootstrap_binaries() {
    my $my_home_user = $ENV{HOME};
    my $command =
        "tar cz -C "
      . $SKYBASEDIR
      . "/.. -f "
      . $SKYDATADIR
      . "/skystack.tar.gz  ./skysql";
    system($command);
    my $err = "000000";
    my @ips =  Scramble::ClusterUtils::get_all_sercive_ips($config);
    foreach (@ips) {
        if ( Scramble::ClusterUtils::is_ip_localhost($_) == 0 ) {
            
            $command =
                "scp -q -i  "
              . $SKYDATADIR
              . $sshkey ." "
              . $SKYDATADIR
              . "/skystack.tar.gz "
              . $_ . ":/tmp";
            system($command);
            my @dest= split('/', $SKYBASEDIR);
            pop(@dest);
              $command =
                "ssh -q -i  "
              . $SKYDATADIR
              . $sshkey. " "
              . $_
              . " \"tar -xvz -f /tmp/skystack.tar.gz -C "
              . join('/',@dest)
              . " && "
              . $SKYBASEDIR
              . "/ncc/init.d/clusterd stop && "
              . $SKYBASEDIR
              . "/ncc/init.d/clusterd start  \"";
            system($command);
        }
    }
    return $err;
}

sub bootstrap_ncc() {
    my $command =
      "tar czv -C " . $SKYBASEDIR . " -f " . $SKYDATADIR . "/ncc.tar.gz  ./ncc";

    system($command);
    my $my_home_user = $ENV{HOME};
    my $err          = "000000";


    my @ips =  Scramble::ClusterUtils::get_all_sercive_ips($config);
    foreach (@ips) {
        if ( Scramble::ClusterUtils::is_ip_localhost($_) ==0) {

            $command =
                "scp -q -i "
              . $SKYDATADIR
              . $sshkey. " "
              . $SKYDATADIR
              . "/ncc.tar.gz "
              . $_ . ":/tmp";
            system($command);
            $command =
                "ssh -q -i "
              . $SKYDATADIR
              . $sshkey. " "
              . $_
              . " \"tar -xzv -f /tmp/ncc.tar.gz -C "
              . $SKYBASEDIR . " ; "
              . $SKYBASEDIR
              . "/ncc/init.d/clusterd stop && "
              . $SKYBASEDIR
              . "/ncc/init.d/clusterd start  \"";
            system($command);
        }
    }
    return $err;
}

sub stop_all_proxy() {

    my $host_info;
    my $err = "000000";
    foreach my $host ( keys( %{ $config->{proxy} } ) ) {
        $host_info = $config->{proxy}->{default};
        $host_info = $config->{proxy}->{$host};
        if ( $host_info->{mode} eq "mysql-proxy" ) {
            $log->log_debug("[stop_all_proxy] service_do_command stop on ". $host ,2 );
            service_do_command( $host_info, $host, "stop" );
        }
    }

}

sub start_all_proxy() {
    $log->log_debug( "[start_all_proxy] entering" ,2 );
    my $host_info;
    my $err = "000000";
    foreach my $host ( keys( %{ $config->{proxy} } ) ) {
        $host_info = $config->{proxy}->{default};
        $host_info = $config->{proxy}->{$host};
        if ( $host_info->{mode} eq "mysql-proxy" ) {
           $log->log_debug( "[start_all_proxy] service_do_command start on ". $host ,2 );
           service_do_command( $host_info, $host, "start" );
        }
    }
}


