#!/usr/bin/env perl
#  Copyright (C) 2012 SkySQL AB.,Ltd.
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
use Common::Config;
use Sys::Hostname;
use Gearman::XS qw(:constants);
use Gearman::XS::Client;
use Gearman::XS::Worker;
use Cache::Memcached;
use Data::Dumper;
use JSON;
use DBI;

struct 'host' => {
    name                 => '$',
    ip                   => '$',
    port                 => '$',
    peer                 => '$',
    mode                 => '$',
    replication_user     => '$',
    replication_password => '$',
    mysql_port           => '$',
    mysql_user           => '$',
    mysql_password       => '$',
    mysql_version        => '$',
    mysql_cnf            => '$'
  
};

struct 'nosql' => {
    name => '$',
    ip   => '$',
    mode => '$',
    port => '$'
};

our %ERRORMESSAGE = (
    "000000" => "running",
    "ER0001" => "SQL command failure",
    "ER0002" => "Remote manager communication failure",
    "ER0003" => "Database communication failure",
    "ER0004" => "Remote manager command failure",
    "ER0005" => "Memcache communication failure",
    "ER0006" => "Remote write configuration failure",
    "ER0007" => "Remote monitoring execution failure",
    "ER0008" => "Can't create remote monitoring db",
    "ER0009" => "Database error in memc_set()",
    "ER0010" => "Database error in memc_servers_set()",
    "ER0011" => "No Memcached for heartbeat",
    "ER0014" => "No Memcached status for action",
    "ER0015" => "No Memcached actions ",
    "ER0016" => "Delayed start until instance start",
    "ER0017" => "Delayed start until instance creation"

);

our $SKYBASEDIR            = $ENV{SKYBASEDIR};
our $SKYDATADIR            = $ENV{SKYDATADIR};
our $hashcolumn;
our $createtable           = "";
our $like                  = "none";
our $database              = "";
our $gearman_timeout       = 2000;
our $gearman_ip            ="localhost";

our $mysql_connect_timeout = 1;
our $config                = new SKY::Common::Config::;
$config->read("etc/cloud.cnf");
$config->check('SANDBOX');

my %ServiceIPs;
my @console;
my @actions;
my $cloud;
my $sshkey;


open my $LOG, q{>>}, $SKYDATADIR . "/log/worker_cluster_cmd.log"
  or die "can't c   reate 'worker_cluster_cmd.log'\n";

my $worker = new Gearman::XS::Worker;
my $ret = $worker->add_server( '', 0 );

if ( $ret != GEARMAN_SUCCESS ) {
    printf( STDERR "%s\n", $worker->error() );
    exit(1);
}

$ret = $worker->add_function( "cluster_cmd", 0, \&cluster_cmd, 0 );
if ( $ret != GEARMAN_SUCCESS ) {
    printf( STDERR "%s\n", $worker->error() );
}

while (1) {

    my $ret = $worker->work();
    if ( $ret != GEARMAN_SUCCESS ) {
        printf( STDERR "%s\n", $worker->error() );
    }
}



sub is_ip_from_status_present($$) {
  my $status =shift;
  my $ip =shift;
  foreach  my $instance (  @{ $status->{instances_status}->{instances}} ) {
    foreach my $key (keys %$instance) {
      if((defined ($instance->{$key}->{ip}) ? $instance->{$key}->{ip}:"" ) eq $ip) {
        return 1;
      }
    }
   }
  return 0; 
}

sub is_ip_from_status_running($$) {
  my $status =shift;
  my $ip =shift;
  foreach  my $instance (  @{ $status->{instances_status}->{instances}} ) {
    foreach my $key (keys %$instance) {
       if((defined ($instance->{$key}->{ip}) ? $instance->{$key}->{ip}:"" ) eq $ip) {
      
        if($instance->{$key}->{state} eq "running" ) {
            return 1;
        }
      }    
    }
   }
  return 0; 
}



sub is_ip_localhost($) {
    
    my $testIP = shift;
    
    my %IPs;
    my $interface;

    foreach (qx{ (LC_ALL=C /sbin/ifconfig -a 2>&1) }) {
        $interface = $1 if /^(\S+?):?\s/;
        next unless defined $interface;
        $IPs{$interface}->{STATE} = uc($1) if /\b(up|down)\b/i;
        $IPs{$interface}->{STATE} =defined( $IPs{$interface}->{STATE}) ?  $IPs{$interface}->{STATE} : "na";
        $IPs{$interface}->{IP}    = $1     if /inet\D+(\d+\.\d+\.\d+\.\d+)/i; 
        $IPs{$interface}->{IP} =defined( $IPs{$interface}->{IP}) ?  $IPs{$interface}->{IP} : "na";
    }
   

    foreach my $key ( sort keys %IPs ) {
        if ( $IPs{$key}->{IP} eq $testIP ) {
            return 1;
        }
    }
    return 0;
}

sub add_ip_to_list($) {
    my $testIP = shift;
    $ServiceIPs{$testIP}=1;
    return 1;
}






sub cluster_cmd {
    my ( $job, $options ) = @_;
  
    my $table   = "";
    my $command = $job->workload();
    my $json    = new JSON;
    my $json_text =
      $json->allow_nonref->utf8->relaxed->escape_slash->loose
      ->allow_singlequote->allow_barekey->decode($command);
    my $peer_host_info = "";
    my $myhost         = hostname;
    my $ddlallnode     = 0;
    my $ddlallproxy    = 0;
    my $ret            = "true";
    my @cmd_console;
    my @cmd_action;
    @console=@cmd_console;
    @actions = @cmd_action;
    $config->read("etc/cloud.cnf");
    $config->check('SANDBOX');
    $cloud = get_active_cloud();
    $sshkey ="/ncc/etc/" . $cloud->{public_key} ;

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
    if ($action ne "ping" )  {print STDERR "Receive command : $command\n";}
    my $group    = $json_text->{command}->{group};
    my $type     = $json_text->{command}->{type};
    my $query    = $json_text->{command}->{query};
    my $database = $json_text->{command}->{database};
    my $level    = $json_text->{level};
    if ( $level eq "instances"  ){
       my $json_cloud       = new JSON ;
       my $json_cloud_str = $json_cloud->allow_nonref->utf8->encode($cloud);
       my $json_cmd       = new JSON ;
       my $json_cmd_str = $json_cmd->allow_nonref->utf8->encode($json_text->{command});

       $json_cloud_str =' {"command":'.$json_cmd_str.',"cloud":'.$json_cloud_str.'}'; 
      
         print  STDERR $json_cloud_str;
       if ($cloud->{driver} ne "LOCAL") {
            $ret= worker_cloud_command($json_cloud_str,$gearman_ip);
       } 
       else {
          $ret= get_local_instances_status();
       }   
        return '{"return":'.$json_cloud_str.',"instances":' .  $ret .'}';
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

        foreach my $host ( sort( keys( %{ $config->{db} } ) ) ) {
           my $host_info = $config->{db}->{default};
           $host_info = $config->{db}->{$host};
           add_ip_to_list($host_info->{ip});
           if ( $host_info->{mode} ne "spider" ) {
               $peer_host_info = $config->{db}->{ $host_info->{peer}[0] };
           }
           my $pass = 1;
           print STDOUT $group . " vs " . $host;
           if ( $group ne $host ) { $pass = 0 }

           if ( $pass == 0 && $group eq "local" && $myhost eq $host_info->{ip} ) {
               $pass = 1;
           }
           if ( $pass == 0 && $group eq "all" ) { $pass = 1 }
           if ( $type ne $host_info->{mode} && $type ne 'all' ) { $pass = 0 }
           if ( $pass == 1 ) {
               my $le_localtime = localtime;
               print $LOG $le_localtime . " processing " . $host . " on $myhost\n";
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
            add_ip_to_list($host_info->{ip});
            my $pass = 1;
            #print STDOUT $group . " vs " . $nosql;
            if ( $group ne $nosql ) { $pass = 0 }

            if ( $pass == 0 && $group eq "local" && $myhost eq $host_info->{ip} ) {
                $pass = 1;
            }
            if ( $pass == 0 && $group eq "all" ) { $pass = 1 }
            if ( $type ne $host_info->{mode} && $type ne 'all' ) { $pass = 0 }
            if ( $pass == 1 ) {
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
            add_ip_to_list($host_info->{ip});
            my $pass = 1;
            #print STDOUT $group . " vs " . $host;
            if ( $group ne $host ) { $pass = 0 }

            if ( $pass == 0 && $group eq "local" && $myhost eq $host_info->{ip} ) {
                $pass = 1;
            }
            if ( $pass == 0 && $group eq "all" ) { $pass = 1 }
            if ( $type ne $host_info->{mode} && $type ne 'all' ) { $pass = 0 }
            if ( $pass == 1 ) {
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
            add_ip_to_list($host_info->{ip});
            my $pass = 1;
            #print STDOUT $group . " vs " . $host;
            if ( $group ne $host ) { $pass = 0 }

            if ( $pass == 0 && $group eq "local" && $myhost eq $host_info->{ip} ) {
                $pass = 1;
            }
            if ( $pass == 0 && $group eq "all" ) { $pass = 1 }
            if ( $type ne $host_info->{mode} && $type ne 'all' ) { $pass = 0 }
            if ( $pass == 1 ) {
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
            my $pass = 1;
            #print STDOUT $group . " vs " . $host;
            if ( $group ne $host ) { $pass = 0 }

            if ( $pass == 0 && $group eq "local" && $myhost eq $host_info->{ip} ) {
                $pass = 1;
            }
            if ( $pass == 0 && $group eq "all" ) { $pass = 1 }
            if ( $type ne $host_info->{mode} && $type ne 'all' ) { $pass = 0 }
            if ( $pass == 1 ) {
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
          my $pass = 1;
          #print STDOUT $group . " vs " . $host;
          if ( $group ne $host ) { $pass = 0 }

          if ( $pass == 0 && $group eq "local" && $myhost eq $host_info->{ip} ) {
              $pass = 1;
          }
          if ( $pass == 0 && $group eq "all" ) { $pass = 1 }
          if ( $type ne $host_info->{mode} && $type ne 'all' ) { $pass = 0 }
          if ( $pass == 1 ) {
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
        foreach my $key ( sort keys %ServiceIPs ) {
          print $key;
        }
   }
   my $json_action      = new JSON; 
   print  $json_action->allow_nonref->utf8->encode(\@actions);
   
   return '{"'.$level.'":[' . join(',' , @console) .']'. $retjson .' }';

}

sub gttid_reinit(){

  my $sql ="replace into mysql.TBLGTID select CRC32(concat(table_schema, table_name)), 0,1  from information_schema.tables;";
  my $master_host=get_active_master();
  mysql_do_command($master_host,$sql); 
}

 
sub lookup_table_name($) {
    my $lquery = shift;
    my @tokens = tokenize_sql($lquery);
    my $next_i = 1;
    for my $token (@tokens) {
        if ( lc $token eq "table" ) {
            return $tokens[$next_i];
        }
        $next_i++;
    }
}

sub dbt2_parse_mix($) {
    
    my $filename = shift;
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
    $response90th{'d'} = get_90th_per($delivery90index,
            @delivery_response_time);
    $response90th{'n'} = get_90th_per($new_order90index,
            @new_order_response_time);
    $response90th{'o'} = get_90th_per($order_status90index,
            @order_status_response_time);
    $response90th{'p'} = get_90th_per($payment90index,
            @payement_response_time);
    $response90th{'s'} = get_90th_per($stock_level90index,
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
     print STDERR $json_result    . "\n";
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
            {RaiseError=>1}
        );
        try {      
            my $sth = $dbh->do($sql);           
        }
        catch Error with {
            print STDERR "Error in mysql_do_command :".$sql." to host:".$host_info->{ip} . "\n";
            return 0;
        };
        $dbh->disconnect;
        return 1;  
}
sub spider_is_ddl_all_node($) {
    my $lquery = shift;
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
" select table_schema, table_name from information_schema.tables where TABLE_SCHEMA<>'mysql' AND TABLE_SCHEMA<>'information_schema'";
    print STDERR $sql;
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

        print STDERR $onlinealter;
        worker_node_command( $onlinealter, $host_router->{ip} );
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
        print STDERR "Skip sql action for data node " . $node . "\n";
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
                print STDERR $requete . "\n";
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
        print STDERR "Create spider table on all proxy node \n";
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
                print STDOUT $requete;
                my $sth = $dbh->do($requete);
            }
        }
    }
    if ( $ddlallnode == 2 ) {
        service_sql_database($query);
    }
    return $err;
}

sub get_instance_id_from_status_ip($$){
my $status =shift;
 my $ip =shift;
  foreach  my $instance (  @{ $status->{instances_status}->{instances}} ) {
    foreach my $key (keys %$instance) {
     if((defined ($instance->{$key}->{ip}) ? $instance->{$key}->{ip}:"" ) eq $ip) {
            return $instance->{$key}->{id};
       }     
    }
   }
  return 0; 
}

sub get_all_slaves() {
    my $host_info;
    my $err = "000000";
    my @slaves;
    foreach my $host ( keys( %{ $config->{db} } ) ) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        if ( $host_info->{mode} eq "slave" ) {
            push( @slaves, $host_info->{ip} . ":" . $host_info->{mysql_port} );
        }
    }

    return join( ',', @slaves );
}

sub get_all_masters() {
    my $host_info;
    my $err = "000000";
    my @masters;
    foreach my $host ( keys( %{ $config->{db} } ) ) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        if ( $host_info->{status} eq "master" ) {
            push( @masters, $host_info->{ip} . ":" . $host_info->{mysql_port} );
        }
    }
    return join( ',', @masters );
}

sub get_all_memcaches() {
    my $host_info;
    my $err = "000000";
    my @memcaches;
    foreach my $host ( keys( %{ $config->{db} } ) ) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        if ( $host_info->{mode} eq "memcache" ) {
            push( @memcaches,
                $host_info->{ip} . ":" . $host_info->{mysql_port} );
        }
    }
    return join( ',', @memcaches );
}

sub get_all_sercive_ips() {
    my $host_info;
    my $err = "000000";
    my @ips;
    foreach my $host ( keys( %{ $config->{db} } ) ) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        push( @ips, $host_info->{ip} );
    }
    foreach my $host ( keys( %{ $config->{nosql} } ) ) {
        $host_info = $config->{nosql}->{default};
        $host_info = $config->{nosql}->{$host};
        push( @ips, $host_info->{ip} );
    }
    foreach my $host ( keys( %{ $config->{proxy} } ) ) {
        $host_info = $config->{proxy}->{default};
        $host_info = $config->{proxy}->{$host};
        push( @ips, $host_info->{ip} );
    }
     foreach my $host ( keys( %{ $config->{lb} } ) ) {
        $host_info = $config->{lb}->{default};
        $host_info = $config->{lb}->{$host};
        push( @ips, $host_info->{ip} );
    }

    return uniq(@ips);
}




sub get_local_instances_status() {
    
    my @ips=get_all_sercive_ips();
    my @interfaces;
    my $i=0;
    my $host_info;
    
    foreach my $ip ( @ips)  {
        push @interfaces, {"instance". $i=>  {
            id     => "instance". $i,
            ip       => $ip,
            state       => "running"
           }     
        };

     $i++;    
    }
    my $json       = new JSON;
     my $json_instances_status =  $json->allow_blessed->convert_blessed->encode(\@interfaces);
   
   return $json_instances_status ; 
}


sub get_active_cloud() {
    my $host_info;
    foreach my $host ( keys( %{ $config->{cloud} } ) ) {
        $host_info = $config->{cloud}->{default};
        $host_info = $config->{cloud}->{$host};
        if ( $host_info->{status} eq "master" ) {
            return $host_info;
        }
    }
   return 0; 
}

sub get_active_monitor() {
    my $host_info;
    foreach my $host ( keys( %{ $config->{monitor} } ) ) {
        $host_info = $config->{monitor}->{default};
        $host_info = $config->{monitor}->{$host};
      #  if ( $host_info->{status} eq "master" ) {
            return $host_info;
      #  }
    }
   return 0; 
}

sub get_active_master() {
    my $host_info;
    foreach my $host ( keys( %{ $config->{db} } ) ) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        if ( $host_info->{status} eq "master" ) {
            return $host_info;
        }
    }
   return 0; 
}

sub get_active_memcache() {
    my $nosql_info; 
    foreach my $nosql (keys(%{$config->{nosql}})) {
        $nosql_info = $config->{nosql}->{default};
        $nosql_info = $config->{nosql}->{$nosql};
        if  ( $nosql_info->{status} eq "master" &&  $nosql_info->{mode} eq "memcache" ){
           return $nosql_info;
        }
    }    
    return 0;
}

sub get_active_lb() {
    my $host_info; 
    foreach my $bench ( keys( %{ $config->{lb} } ) ) {

       $host_info = $config->{lb}->{default};
        $host_info = $config->{lb}->{$bench};
        if ( $host_info->{mode} eq "keepalived" ) {
            return $host_info;
        }
    }
    return 0;
}

sub get_active_master_hash() {
    my $host_info;
    foreach my $host ( keys( %{ $config->{db} } ) ) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        if ( $host_info->{status} eq "master" ) {
            return $host;
        }
    }
   return 0; 
}

sub get_all_ip_from_status($) {
  my $status =shift;
   my @serviceips;  
  foreach  my $service (  @{ $status->{services_status}->{services}} ) {
    foreach my $key (keys %$service) {
     push (@serviceips,$service->{$key}->{ip} );
   }
   }

  return @serviceips; 
}

sub get_source_ip_from_status($) {
  my $status =shift;
   my @serviceips;  
   foreach  my $interface (  @{ $status->{host}->{interfaces}} ) {
    foreach my $attr (keys %$interface) {
          foreach  my $service (  @{ $status->{services_status}->{services}} ) {
               foreach my $key (keys %$service) {
                    if ($service->{$key}->{ip} eq $interface->{$attr}->{IP} ){
                        return $interface->{$attr}->{IP};
                    }
          } 
        }
         
      }
    }  
  return "0.0.0.0"; 
}

sub get_status_diff($$) {
  my $previous_status =shift;
  my $status_r =shift;
  my $i=0;
  my @diff;   
  foreach  my $service (  @{ ${$status_r}->{services_status}->{services}} ) {
   
    foreach my $key (keys %$service) {
      print STDERR $key . "\n";    
      if ($service->{$key}->{status} ne $previous_status->{services_status}->{services}[$i]->{$key}->{status}
            || $service->{$key}->{code} ne $previous_status->{services_status}->{services}[$i]->{$key}->{code}
         )    
      {
            print STDERR "trigger";
          push @diff, {
         name     => $key ,
         type     => "services",
         ip    => $service->{$key}->{ip} ,
         state    => $service->{$key}->{status} ,
         code     => $service->{$key}->{code} , 
         previous_state =>$previous_status->{services_status}->{services}[$i]->{$key}->{status},
         previous_code =>$previous_status->{services_status}->{services}[$i]->{$key}->{code}
        };  
        
       }
       
     }
   $i++;
  }
   $i=0;
   foreach  my $instance (  @{ ${$status_r}->{instances_status}->{instances}} ) {
     foreach my $attr (keys %$instance) {
       my $skip=0; 
       print STDERR "ici test ssh\n"; 
       if ( (defined ($instance->{$attr}->{state}) ? $instance->{$attr}->{state}:"") ne 
          (defined ($previous_status->{instances_status}->{instances}[$i]->{$attr}->{state}) ? $previous_status->{instances_status}->{instances}[$i]->{$attr}->{state} :"")

        )    
       {
           
            if ((defined($instance->{$attr}->{state}) ? $instance->{$attr}->{state} : "" ) eq "running"){
             if( instance_check_ssh($instance->{$attr}->{ip}) ==0 ) {
                  print STDERR "skipping because ssh failed\n";
                  ${$status_r}->{instances_status}->{instances}[$i]->{$attr}->{state}="pending";
                     $skip=1;  
               
`   `         } 

         } 
         if ($skip==0 )   {
           print STDERR "trigger";
          
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
     print STDERR   $json_status_diff . "\n";
     return $json_status_diff;
}

sub get_90th_per($$)  {
    my $self = shift;
    my $index = shift;
    
    my @data = @_;
    $index= defined ($index) ? $index : 0;
    use POSIX qw(ceil floor);
    my $result;
    my $floor = floor($index);
    my $ceil = ceil($index);
    if ($floor == $ceil) {
        $result = $data[$index];
    } else {
        if ($data[$ceil]) {
            $result = ($data[$floor] + $data[$ceil]) / 2;
        } else {
            $result = $data[$floor];
        }
    }
    return $result;
}


sub service_sql_database($) {
    my $query = shift;
    my $host_info;
    my $err = "000000";
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
                    "$host_info->{datadir}/sandboxes/$host/my sql -h"
                  . $host_info->{ip} . " -P "
                  . $host_info->{mysql_port} . " -u"
                  . $host_info->{mysql_user}
                  . "  -p$host_info->{mysql_password} -e\""
                  . $query . "\"";
                worker_node_command( $TRIG, $host_info->{ip} );
                print STDERR "dsn: "
                  . $dsn . " : "
                  . $host_info->{mysql_user} . " "
                  . $host_info->{mysql_password} . " : "
                  . $query . "\n";

                # my $dbh = DBI->connect($dsn, $host_info->{mysql_user}, $host_info->{mysql_password});
                # my $sth = $dbh->do($query);
                # $sth->execute();
                # $sth->finish();
                # $dbh->disconnect();

            }
            catch Error with {
                print STDERR "ici7\n";
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
  my $param = "$self->{datadir}/sandboxes/$node/stop";
        $err   = worker_node_command( $param, $self->{ip} );
        $param = "rm -rf $self->{datadir}/sandboxes/$node";
        $err   = worker_node_command( $param, $self->{ip} );
  return $err;
}

sub service_status_mycheckpoint($$$) {
  my $host_vip=shift;
  my $host_info=shift;
  my $host=shift;
  my $mon=get_active_monitor();
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

        $err = worker_node_command( $cmd, $host_info->{ip} );
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
        
       my  $dbh2 = DBI->connect(
            $dsn2,
            $host_info->{mysql_user},
            $host_info->{mysql_password},
           {RaiseError=>0,PrintError=>1}
        );
        if (!$dbh2)  { 
          print STDERR "Error Memcache UDF DB connect failed \n";
          return 0;
        }
         my $sql="SELECT memc_set('test','test',0)";
        my $res = 0;
       
        my $sth2 = $dbh2->do($sql);
        if (!$sth2)  {    
             print STDERR "Error Mon connect memc_set \n";
            
        }
        $sql="SELECT memc_get('test') as c1";
          
        if ( my $stm = $dbh2->prepare($sql)){
                $stm->execute; 
                my($result) = $stm->fetchrow_array();
                $result= defined ($result) ? $result : "no";
                 
                if  ( $result eq "test" )   {
                   $res=1;
                } 
                $stm->finish(); 
           }  
         else {
             $dbh2->disconnect;
             return 0;
             print STDERR "Error Mon connect memc_GET\n";
          }
           
          
        if ($res == 0 ) {
             print STDERR "Setting UDF Memcache server\n";
            my $mem_info=get_active_memcache();
            $sql="SELECT memc_servers_set('". $mem_info->{ip} .":". $mem_info->{port}."')";
            print STDERR "'". $mem_info->{ip} .":". $mem_info->{port}."'";
            try {
             my $sth = $dbh2->do($sql);
            }
            catch Error with {
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
    print STDERR $dsn;
    use Error qw(:try);
    try {

        my $dbh =
          DBI->connect( $dsn, $self->{mysql_user},
            $self->{mysql_password} );
        my $param = "select 1";
        $dbh->do($param);
    }
    catch Error with {
        $err = "ER0003";
    };
 return $err;
}

sub service_status_memcache($$) {
  my $self = shift;
  my $node = shift;
  my $err = "000000";
  use Error qw(:try);
  print STDERR "connecting to memcache \n";
  try {

    my $memd = new Cache::Memcached {
        'servers' => [ $self->{ip} . ":" . $self->{port} ],
        'debug'   => 0,
        'compress_threshold' => 10_000,
    };

    $memd->set( "key_test", "test" );
    my $val = $memd->get("key_test");
    $err = "ER0005";
    if ( $val eq "test" ) { $err = "000000"; }
  }  catch Error with {
    $err = "ER0005";
  };
  return $err;
}

sub service_start_memcache($$) {
  my $self = shift;
  my $node = shift;
  my $err = "000000";

    # delete_pid_if_exists($SKYBASEDIR."/ncc/tmp/memcached.". $node .".pid",$self->{ip});
  my $param =
        $SKYBASEDIR
      . "/ncc/init.d/start-memcached "
      . $SKYBASEDIR
      . "/ncc/etc/memcached."
      . $node . ".cnf";
  print STDERR  "service_start_memcache : " . $param  ." on: ". $self->{ip};   
  $err = worker_node_command( $param, $self->{ip} );
  return $err;
}

sub service_start_tarantool($$) {
  my $self = shift;
  my $node = shift;
  my $err = "000000";

   
  my $param =
        $SKYBASEDIR
      . "/tarantool/bin/tarantool_box -B -c "
      . $SKYBASEDIR
      . "/ncc/etc/tarantool."
      . $node . ".cnf";
  print STDERR  "service_start_tarantool : " . $param  ." on: ". $self->{ip};   
  $err = worker_node_command( $param, $self->{ip} );
  return $err;
}



sub service_start_database($$) {
    my $self = shift;
    my $node = shift;
   
    my $err = "000000";

    
    my $param = "$SKYDATADIR/sandboxes/$node/send_kill";
    $err = worker_node_command( $param, $self->{ip} );

    $param = "$SKYDATADIR/sandboxes/$node/start";
    $err = worker_node_command( $param, $self->{ip} );
    
    my $memcaches = get_all_memcaches();
    $param =
"$SKYDATADIR/sandboxes/$node/my sql -uroot -p$self->{mysql_password} -e\""
          . "SELECT memc_servers_set('"
          . $memcaches . "');\"";
    $err = worker_node_command( $param, $self->{ip} );
    return $err;
}

sub service_start_mycheckpoint($$) {
  my $self = shift;
  my $node = shift;
  my $host=get_active_master_hash();
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
   $err = worker_node_command( $cmd , $self->{ip} );
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
          . $SKYBASEDIR
          . "/ncc/tmp/mysql-proxy."
          . $node
          . ".pid --log-file="
          . $SKYDATADIR
          . "/log/mysql-proxy."
          . $node . ".log  1>&2 ";
  $err = worker_node_command( $param, $self->{ip} );
  return $err;
}

sub service_start_keepalived($$) {
  my $self = shift;
  my $node = shift;
  my $err = "000000";  
  my $param =
            $SKYBASEDIR
          . "/keepalived/sbin/keepalived -f  "
          . $SKYBASEDIR
          . "/ncc/etc/keepalived."
          . $node . ".cnf";
  $err = worker_node_command( $param, $self->{ip} );
  return $err;
}

sub service_start_haproxy($$) {
  my $self = shift;
  my $node = shift;
  my $err = "000000";  
  my $param =
            $SKYBASEDIR
          . "/haproxy/sbin/haproxy -f "
          . $SKYBASEDIR
          . "/ncc/etc/haproxy."
          . $node
          . ".cnf -p "
          . $SKYBASEDIR
          . "/ncc/tmp/haproxy."
          . $node . ".pid ";
  $err = worker_node_command( $param, $self->{ip} );
  return $err;
}

sub service_start_bench($$$$) {
    my $self = shift;
    my $node = shift;
    my $type = shift;
    my $bench_info=get_active_lb();
    my $err = "000000";
   
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
    $err = worker_node_command( $cmd, $self->{ip} );
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
     $err = worker_node_command( $cmd, $self->{ip} ); 
     $cmd="killall client";
     $err = worker_node_command( $cmd, $self->{ip} ); 
     my $outfile =$SKYDATADIR. "/" .$node."/driver/mix.log";

     my $json_res = dbt2_parse_mix( $outfile );
     return $json_res;
}

sub service_stop_database($$) {
    my $self = shift;
    my $node = shift;
    my $err = "000000";

    my $param = "$SKYDATADIR/sandboxes/$node/send_kill";
    $err = worker_node_command( $param, $self->{ip} );
    return $err;
}
sub service_stop_mysqlproxy($$) {
  my $self = shift;
  my $name = shift;
  my $err = "000000";
    
  my $param =
            "kill -9 `cat "
          . $SKYBASEDIR
          . "/ncc/tmp/mysql-proxy."
          . $name . ".pid`";
 print STDERR  $param ."\n";
  $err = worker_node_command( $param, $self->{ip} );
  return $err;
}

sub service_stop_memcache($$) {
  my $self = shift;
  my $name = shift;
  my $err = "000000";
  my $param =
            "kill -9 `cat "
          . $SKYBASEDIR
          . "/ncc/tmp/memcached."
          . $name . ".pid`";
  $err = worker_node_command( $param, $self->{ip} );
  return $err;
}

sub service_stop_keepalived($$) {
  my $self = shift;
  my $name = shift;
  my $err = "000000";
  my $param = "killall keepalived ";
  $err = worker_node_command( $param, $self->{ip} );
  return $err;
}

sub service_stop_haproxy($$) {
  my $self = shift;
  my $name = shift;
  my $err = "000000";
  my $param =
          "kill -9 `cat " . $SKYBASEDIR . "/ncc/tmp/haproxy." . $name . ".pid`";
 $err = worker_node_command( $param, $self->{ip} );
 return $err;
}

sub service_stop_mycheckpoint($$) {
  my $self = shift;
  my $name = shift;
  my $err = "000000";
  my $param =
            "kill -9 `cat "
          . $SKYBASEDIR
          . "/ncc/tmp/mycheckpoint."
          . $name . ".pid`";
  $err = worker_node_command( $param, $self->{ip} );
  return $err;
}

sub service_install_bench($$) {
 my $self=shift;
 my $name = shift;
 my $cmd="mkdir ". $SKYDATADIR ."/".$name;
 worker_node_command( $cmd, $self->{ip} );
 $cmd="mkdir ". $SKYDATADIR ."/".$name."/client";
 worker_node_command( $cmd, $self->{ip} );
 $cmd="mkdir ". $SKYDATADIR ."/".$name."/driver";
 worker_node_command( $cmd, $self->{ip} );
 
 
 $cmd=$SKYBASEDIR
      . "/dbt2/bin/datagen  -w "
      .  $self->{warehouse}
      . " -d "
      . $SKYDATADIR 
      ."/".$name
      ." --mysql";
 worker_node_command( $cmd, $self->{ip} );
 my $master= get_active_master();
 mysql_do_command($master,"DROP DATABASE IF EXISTS dbt2");

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
      ." -l -e INNODB"; ;
 worker_node_command( $cmd, $self->{ip} );
 
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
  print STDERR  "service_start_tarantool : " . $param  ." on: ". $self->{ip};   
  $err = worker_node_command( $param, $self->{ip} );
  $param =
        "chown skysql:skysql "
      . $SKYDATADIR 
      . "/"
      . $node;
  $err = worker_node_command( $param, $self->{ip});
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
    if ( $type eq 'router' ) {

        my $param = "$SKYBASEDIR/ncc/init.d/routerd start";
        $err = worker_node_command( $param, $self->{ip} );
        return $err;
    }
    my $SANDBOX =
        "low_level_make_sandbox --force --no_show" . "  "
      . " -c server-id=$self->{mysql_port} "
      . " --db_user=$self->{mysql_user} "
      . " --db_password=$self->{mysql_password} "
      . " -d $node "
      . " --sandbox_port=$self->{mysql_port} "
      . " --upper_directory=$SKYDATADIR/sandboxes "
      . " --install_version=5.5 --no_ver_after_name "
      . " --basedir=$SKYBASEDIR/$self->{mysql_version} "
      . " --my_file=$self->{mysql_cnf}";

    my $le_localtime = localtime;
    print $LOG $le_localtime . " $SANDBOX\n";
    print STDOUT $le_localtime . " $SANDBOX\n";
    $err = worker_node_command( $SANDBOX, $self->{ip} );
    my $GRANT =
"$SKYDATADIR/sandboxes/$node/my sql -uroot -p$self->{mysql_password} -e\""
      . "GRANT replication slave ON *.* to \'$self->{replication_user}\'@\'%\' IDENTIFIED BY \'$self->{replication_password}\';"
      . "GRANT ALL ON *.* to \'$self->{mysql_user}\'@\'%\' IDENTIFIED BY \'$self->{mysql_password}\';"
      . "\"";
    worker_node_command( $GRANT, $self->{ip} );

    if ( $type eq 'spider' || $type eq 'monitor' ) {
        my $SPIDER =
"$SKYDATADIR/sandboxes/$node/my sql -uroot -p$self->{mysql_password} < $SKYBASEDIR/ncc/scripts/install_spider.sql";
        $err = worker_node_command( $SPIDER, $self->{ip} );

    }
    else {
        my $HANDLERSOCKET =
"$SKYDATADIR/sandboxes/$node/my sql -uroot -p$self->{mysql_password} -e\"INSTALL PLUGIN handlersocket SONAME \'handlersocket.so\';\"";
        $err = worker_node_command( $HANDLERSOCKET, $self->{ip} );
    }
    my $GEARMANUDF =
"$SKYDATADIR/sandboxes/$node/my sql -uroot -p$self->{mysql_password} < $SKYBASEDIR/ncc/scripts/gearmanudf.sql";
    $err = worker_node_command( $GEARMANUDF, $self->{ip} );

    my $param =
"$SKYDATADIR/sandboxes/$node/my sql -uroot -p$self->{mysql_password} -e\""
      . "SELECT gman_servers_set('127.0.0.1');\"";
    $err = worker_node_command( $param, $self->{ip} );

    my $MEMCACHEUDF =
"$SKYDATADIR/sandboxes/$node/my sql -uroot -p$self->{mysql_password} < $SKYBASEDIR/ncc/scripts/install_memcacheudf.sql";
    $err = worker_node_command( $MEMCACHEUDF, $self->{ip} );

    my $GTTID =
"$SKYDATADIR/sandboxes/$node/my sql -uroot -p$self->{mysql_password} < $SKYBASEDIR/ncc/scripts/gttid.sql";
    $err = worker_node_command( $GTTID, $self->{ip} );
    my $memcaches = get_all_memcaches();

    $param =
"$SKYDATADIR/sandboxes/$node/my sql -uroot -p$self->{mysql_password} -e\""
      . "SELECT memc_servers_set('"
      . $memcaches . "');\"";
    $err = worker_node_command( $param, $self->{ip} );

    report_status( $self, "Install node", $err, $node );
    return $err;
}

sub service_sync_database($$$) {
    my $self   = shift;
    my $node   = shift;
    my $master = shift;
  
    print STDERR "mysql -h$master->{ip} ";

#	my $param =   "mysql -h$self->{ip} -P$self->{mysql_port} -u $self->{mysql_user} -p$self->{mysql_password} -e \'stop slave;change master to master_host=\\\"$master->{ip}\\\", master_user=\\\"$master->{replication_user}\\\", master_password=\\\"$master->{replication_password}\\\", master_port=$master->{mysql_port};\';mysqldump -u $self->{mysql_user} -p$self->{mysql_password} -h $master->{ip} -P$master->{mysql_port} --single-transaction --master-data --all-databases | mysql -u $self->{mysql_user} -p$self->{mysql_password} -h$self->{ip} -P$self->{mysql_port};mysql -h$self->{ip} -P$self->{mysql_port} -u $self->{mysql_user} -p$self->{mysql_password} -e \'start slave;\' ";
    my $param =
"mysql -h$self->{ip} -P$self->{mysql_port} -u $self->{mysql_user} -p$self->{mysql_password} -e \'stop slave;change master to master_host=\"$master->{ip}\", master_user=\"$master->{replication_user}\", master_password=\"$master->{replication_password}\", master_port=$master->{mysql_port};\';mysqldump -u $self->{mysql_user} -p$self->{mysql_password} -h $master->{ip} -P$master->{mysql_port} --single-transaction --master-data --all-databases | mysql -u $self->{mysql_user} -p$self->{mysql_password} -h$self->{ip} -P$self->{mysql_port};mysql -h$self->{ip} -P$self->{mysql_port} -u $self->{mysql_user} -p$self->{mysql_password} -e \'start slave;\' ";

    worker_node_command( $param, $self->{ip} );
}


sub instance_check_ssh($){
    my $ip =shift;
    my $command="ssh -q -i "
    . $SKYBASEDIR
    . $sshkey. " "
    . ' -o "BatchMode=yes"  -o "StrictHostKeyChecking=no" '
    . $ip
    .' "echo 2>&1" && echo "OK" || echo "NOK"';
  my  $result = `$command`;
  print STDERR "\nsortie du ssh test :".$command. " : " . $result . "test\n";
   $result =~ s/\n//g; 
     if ( $result eq "OK"){ 
        return 1;
    }
  return 0;
}

sub instance_heartbeat_collector($$) {
    my $status =shift;
    my $json_status = shift ;
    
    my $json    = new JSON;
    print STDERR "Receive heartbeat..\n";
    #my $status =
    #  $json->allow_nonref->utf8->relaxed->escape_slash->loose
    #  ->allow_singlequote->allow_barekey->decode($json_status);
   
    my $err = "000000";
    my $host_info;
    my $host_vip = get_active_master();
    my $source_ip = get_source_ip_from_status($status) ;
    print STDERR "heratbeat from ". $source_ip;
    my $mem_info=get_active_memcache();
 
    print STDERR "Process the ping with memcache: ". $mem_info->{ip} . ":" . $mem_info->{port}."\n";
    
   
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
          
            my $previous_status = $json->allow_nonref->utf8->relaxed->escape_slash->loose
            ->allow_singlequote->allow_barekey->decode($previous_json_status);
            # pass a reference as the status need to be change in case of ssh failed 
            my $json_triggers = get_status_diff($previous_status,\$status);
            worker_doctor_command($json_triggers,$gearman_ip);
            $json_status= $json->allow_blessed->convert_blessed->encode($status);
            print STDERR "after diff: ". $json_status;
       }     
       
    #}  catch Error with {
    #     print STDERR "\nbogus get bogus\n";
    #    $err = "ER0011";
    #};

    try {
        print STDERR "\nStoring hearbeat to memcache..\n";
       
        $memd->set( "status".   $source_ip, $json_status );
        $memd->set( "status", $json_status );
        
        
       }  catch Error with {
         print STDERR "bogus set memcache\n";
        $err = "ER00012";
    };
    my $json_todo = $memd->get("actions");
    if (!$json_todo )
    {
              print STDERR "No actions in memcache heartbeat setting empty json  \n";
              $memd->set( "actions", '{"actions":[]}' );
    }

    foreach my $host ( keys( %{ $config->{db} } ) ) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        print STDERR "Connect to db: " . $host . "\n";
        
        # Check memcache_udf
        service_status_memcache_fromdb($host_info);
        service_install_mycheckpoint( $host_vip , "mon_" . $host);
        service_status_mycheckpoint($host_vip,$host_info,$host);
        
    }
    return $err;
    
}


sub database_rolling_restart(){ 
 my $host_info;
 my $host_slave;
 foreach my $host ( keys( %{ $config->{db} } ) ) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        if  ( $host_info->{mode} eq "slave" ){
                $ret = service_do_command( $host_info, $host, "stop" );
                $host_slave=$host_info;
        }         
       
  }


 foreach my $host ( keys( %{ $config->{db} } ) ) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        if  ( $host_info->{status} eq "master" ){
           service_switch_database($host_slave); 
           $ret = service_do_command( $host_info, $host, "stop" );
       }         
       
  }
 
}
sub service_switch_database($) {
    my $switchhost = shift;
    my $host_info;
    my $err = "000000";

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

        if ( $drap == 1 ) {
            $line =~ s/^(.*)status(.*)slave(.*)$/\tstatus\t\t\t\tmaster/gi;

        }
        else {
            $line =~ s/^(.*)status(.*)(slave|master)(.*)$/\tstatus\t\t\t\tslave/gi;
        }
        print $out $line;
        next;
    }
    system("rm -f $file.old");
    system("mv $file $file.old");
    system("mv $file.new $file");
    system("chmod 660 $file");

   
    stop_all_proxy();
    bootstrap_config();
    foreach my $host ( keys( %{ $config->{db} } ) ) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        if ( $host eq $switchhost ) {
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



sub service_do_command($$$) {
    my $self = shift;
    my $node = shift;
    my $cmd  = shift;
    my $param = "";
    my $err   = "000000";
      print STDERR "Service Do command \n";

    if ( $cmd eq "start" && is_ip_localhost($self->{ip})==0) {
        # get the instances status from memcache 
        my $mem_info=get_active_memcache();

        print STDERR "Get the status in memcache: ". $mem_info->{ip} . ":" . $mem_info->{port}."\n";

        
        my $memd = new Cache::Memcached {
               'servers' => [ $mem_info->{ip} . ":" . $mem_info->{port} ],
               'debug'   => 0,
               'compress_threshold' => 10_000,
        };

           my $cloud = get_active_cloud();
           my $json_cloud       = new JSON ;
           my $json_cloud_str = $json_cloud->allow_nonref->utf8->encode($cloud);
            
           my $json_status = $memd->get("status");
           if (!$json_status )
           {
              print STDERR "No status in memcache \n";
              report_status( $self, $param,  "ER0014", $node );
              return "ER0014";  
           }
           my $json_todo = $memd->get("actions");
           if (!$json_todo )
           {
              print STDERR "No actions in memcache \n";
              report_status( $self, $param,  "ER0015", $node );
              return "ER0015";  
           }
           
           my $status = $json_cloud->allow_nonref->utf8->relaxed->escape_slash->loose
            ->allow_singlequote->allow_barekey->decode($json_status);
           
           my $todo =  $json_cloud->allow_nonref->utf8->relaxed->escape_slash->loose
            ->allow_singlequote->allow_barekey->decode($json_todo);
                
            
          if ( is_ip_from_status_present($status,$self->{ip})==1) {
             print STDERR "Service Ip is found in status \n";   
             if ( is_ip_from_status_running($status,$self->{ip})==0) {
                my $instance=get_instance_id_from_status_ip($status,$self->{ip});
                # not running but present need to start 
                my $start_instance='{"level":"instances","command":{"action":"start","group":"'.$instance.'","type":"all"},"cloud":'. $json_cloud_str. '}';
                worker_cloud_command($start_instance,$gearman_ip);
                
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
                  report_status( $self, $param,  "ER0016", $node );
                  return "ER0016";
              }
         } else 
         {
              print STDERR "Service Ip is not found in status \n";
             my $launch_instance='{"level":"instances","command":{"action":"launch","group":"ScrambleDB","type":"all","ip":"'.$self->{ip}.'"},"cloud":'. $json_cloud_str. '}';
             worker_cloud_command($launch_instance,$gearman_ip);
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
           print  "service_status_database". $err ;
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
    
    report_status( $self, $param, $err, $node );
    return $err;
}

sub delete_pid_if_exists($$) {
    my $pidfile = shift;
    my $ip      = shift;
    my $err     = "000000";

    if ( -e $pidfile ) {
        open PIDHANDLE, "$pidfile";
        my $localpid = <PIDHANDLE>;
        close PIDHANDLE;
        chomp $localpid;
        $err =
          worker_node_command( "tail -f /dev/null --pid " . $localpid, $ip );
    }
    return $err;
}


sub worker_node_command($$) {
    my $cmd    = shift;
    my $ip     = shift;
    my $client = Gearman::XS::Client->new();
    my $res ="000000";
    $client->add_servers($ip);
    print STDOUT $ip . ' ' . $cmd . '\n';
    
    ( my $ret, my $result ) = $client->do( 'node_cmd', $cmd );

    if ( $ret == GEARMAN_SUCCESS ) {
        if ( $result eq "true" ) {
            $res = "000000";
        }
        else { $res = "ER0003"; }

    }
    else { $res = "ER0002"; }
    report_action($ip,$cmd,$res);
    return $res;

}

sub worker_cloud_command($$) {
    my $cmd    = shift;
    my $ip     = shift;
    my $client = Gearman::XS::Client->new();
    $client->add_servers($ip);
    print STDOUT $ip . ' ' . $cmd . '\n';
    #$client->set_timeout($gearman_timeout);
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

sub worker_config_command($$) {
    my $cmd    = shift;
    my $ip     = shift;
    my $client = Gearman::XS::Client->new();
    $client->add_servers($ip);
    print STDERR "Worker_config_command for ip :". $ip ."\n";
    #$client->set_timeout($gearman_timeout);
    ( my $ret, my $result ) = $client->do( 'write_config', $cmd );

    if ( $ret == GEARMAN_SUCCESS ) {
        return "ER0006";
    }
    elsif ( !defined $result ) {
        return "ER0006";
    }
    elsif ( $result eq '{"result":{"status":"00000"}}' ) {
        return "000000";
    }
    else {
        return "000000";
    }

}



sub worker_doctor_command($$) {
    my $cmd    = shift;
    my $ip     = shift;
    my $client = Gearman::XS::Client->new();
    $client->add_servers($ip);

    #$client->set_timeout($gearman_timeout);
    ( my $ret, my $result ) = $client->do( 'consult_cmd', $cmd );

    if ( $ret == GEARMAN_SUCCESS ) {
        return "ER0006";
    }
    elsif ( !defined $result ) {
        return "ER0006";
    }
    else {
        return $result;
    }

}

sub RPad($$$) {

    my $str = shift;
    my $len = shift;
    my $chr = shift;
    $chr = " " unless ( defined($chr) );
    return substr( $str . ( $chr x $len ), 0, $len );
}    

sub report_status($$$) {
    my $self = shift;
    my $cmd  = shift;
    my $err  = shift;
    my $host = shift;
    print STDERR $cmd . '\n';
    my $le_localtime = localtime;
    print $LOG $le_localtime . " $cmd\n";
    my $status ="na";
    if ( $self->{status}) { 
     $status=$self->{status};
    }
    push(@console ,
       '{"'.$host .'":{"time":"'
      . $le_localtime
      . '","name":"'
      .  $host
      . '","ip":"'
      .  $self->{ip} 
      . '","mode":"'
      .  $self->{mode}  
      . '","status":"'
      .  $status
      . '","code":"'
      . $err
      . '","state":"'
      . $ERRORMESSAGE{$err}  
      . '"}}'
   ); 
 
}
sub report_action($$$) {
    my $ip = shift;
    my $cmd  = shift;
    my $err  = shift;
    my $le_localtime = localtime;
    print '{"time":"'
      . $le_localtime
      . '","ip":"'
      . $ip 
      . '","code":"'
      . $err
      . '","command":"'
      . $cmd  
      . '"}';
    push(@actions ,
       '{"time":"'
      . $le_localtime
      . '","ip":"'
      . $ip 
      . '","code":"'
      . $err
      . '","command":"'
      . $cmd  
      . '"}'
   );
   
}




sub replace_config_line($$$) {
    my $file   = shift;
    my $strin  = shift;
    my $strout = shift;

    open my $in,  '<', $file       or die "Can't read old file: $!";
    open my $out, '>', "$file.new" or die "Can't write new file: $!";

    while (<$in>) {
        s/^$strin(.*)$/$strout/gi;
        print $out $_;
    }

    close $out;
    system("rm -f $file.old");
    system("mv $file $file.old");
    system("mv $file.new $file");
    system("chmod 660 $file");
}

sub bootstrap_config() {
    print STDERR "bootstrap_config\n"; 
    my $my_home_user = $ENV{HOME};
    my $cmd =
        'string="`cat '
      . $SKYBASEDIR
      . '/ncc/etc/id_rsa.pub`"; sed -e "\|$string|h; \${x;s|$string||;{g;t};a\\" -e "$string" -e "}" $HOME/.ssh/authorized_keys > $HOME/.ssh/authorized_keys2 ;mv $HOME/.ssh/authorized_keys $HOME/.ssh/authorized_keys_old;mv $HOME/.ssh/authorized_keys2 $HOME/.ssh/authorized_keys';
    my $err = "000000";

    my @ips = get_all_sercive_ips();
    foreach (@ips) {
        my $command =
          "{command:{action:'write_config',group:'localhost',type:'all'}}";
         my $action =  
        "scp -i "
              . $SKYBASEDIR
              . $sshkey." "
              . $SKYBASEDIR
              . "/ncc/etc/cloud.cnf "
              . $_ . ":"
              . $SKYBASEDIR
              . "/ncc/etc" ;  
        system( $action);
        print STDERR $action;    
        $err = worker_config_command( $command, $_ );
        
    }
    print STDERR "End bootstrap_config\n";
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
    my @ips = get_all_sercive_ips();
    foreach (@ips) {
        if ( is_ip_localhost($_) == 0 ) {
            
            $command =
                "scp -q -i  "
              . $SKYBASEDIR
              . $sshkey ." "
              . $SKYDATADIR
              . "/skystack.tar.gz "
              . $_ . ":/tmp";
            system($command);
            my @dest= split('/', $SKYBASEDIR);
            pop(@dest);
              $command =
                "ssh -q -i  "
              . $SKYBASEDIR
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


    my @ips = get_all_sercive_ips();
    foreach (@ips) {
        if ( is_ip_localhost($_) ==0) {

            $command =
                "scp -q -i "
              . $SKYBASEDIR
              . $sshkey. " "
              . $SKYDATADIR
              . "/ncc.tar.gz "
              . $_ . ":/tmp";
            system($command);
            $command =
                "ssh -q -i "
              . $SKYBASEDIR
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
            service_do_command( $host_info, $host, "stop" );
        }
    }

}

sub start_all_proxy() {

    my $host_info;
    my $err = "000000";
    foreach my $host ( keys( %{ $config->{proxy} } ) ) {
        $host_info = $config->{proxy}->{default};
        $host_info = $config->{proxy}->{$host};
        if ( $host_info->{mode} eq "mysql-proxy" ) {
            service_do_command( $host_info, $host, "start" );
        }
    }
}



sub uniq {
    my %seen;
    return grep { !$seen{$_}++ } @_;
}

sub tokenize_sql($) {
    my $query = shift;
    my $re    = qr{
		(
			(?:--|\#)[\ \t\S]*      # single line comments
			|
			(?:<>|<=>|>=|<=|==|=|!=|!|<<|>>|<|>|\|\||\||&&|&|-|\+|\*(?!/)|/(?!\*)|\%|~|\^|\?)
									# operators and tests
			|
			[\[\]\(\),;.]            # punctuation (parenthesis, comma)
			|
			\'\'(?!\')              # empty single quoted string
			|
			\"\"(?!\"")             # empty double quoted string
			|
			".*?(?:(?:""){1,}"|(?<!["\\])"(?!")|\\"{2})
									# anything inside double quotes, ungreedy
			|
			`.*?(?:(?:``){1,}`|(?<![`\\])`(?!`)|\\`{2})
									# anything inside backticks quotes, ungreedy
			|
			'.*?(?:(?:''){1,}'|(?<!['\\])'(?!')|\\'{2})
									# anything inside single quotes, ungreedy.
			|
			/\*[\ \t\n\S]*?\*/      # C style comments
			|
			(?:[\w:@]+(?:\.(?:\w+|\*)?)*)
									# words, standard named placeholders, db.table.*, db.*
			|
			\n                      # newline
			|
			[\t\ ]+                 # any kind of white spaces
		)
	   }smx;
    my @ltokens = $query =~ m{$re}smxg;

    @ltokens = grep( !/^[\s\n\r]*$/, @ltokens );

    return wantarray ? @ltokens : \@ltokens;
}
