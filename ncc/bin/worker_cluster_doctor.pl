#!/usr/bin/env perl
use strict;
use Class::Struct;
use warnings FATAL => 'all';
use Common::Config;
use Gearman::XS qw(:constants);
use Gearman::XS::Client;
use Gearman::XS::Worker;
use JSON;
use Cache::Memcached;
use DBI;
use Data::Dumper;


our $SKYBASEDIR = $ ENV {SKYBASEDIR};
our $SKYDATADIR = $ ENV {SKYDATADIR};
our $gearman_timeout = 2000;
our $gearman_ip            ="localhost";
our $mysql_connect_timeout=3;
our $console = "{result:{status:'00000'}}";
our $config = new SKY::Common::Config::;
my $conf="etc/cloud.cnf";
$config->read($conf);
$config->check('SANDBOX');


my $worker = new Gearman::XS::Worker;
my $ret = $worker->add_server($gearman_ip,0);
if ($ret != GEARMAN_SUCCESS) {
		printf(STDERR "%s\n", $worker->error());
		exit(1);
}

$ret = $worker->add_function("consult_cmd", 0, \&consult_cmd, 0);
if ($ret != GEARMAN_SUCCESS) {
		printf(STDERR "%s\n", $worker->error());
}

while (1) {

		my $ret = $worker->work();
		if ($ret != GEARMAN_SUCCESS) {
			printf(STDERR "%s\n", $worker->error());
		}
}


 
sub consult_cmd() {
    my ($job, $options) = @_;
    my $command = $job->workload();
    print STDERR "Receive command : " .$command ." \n";
    my $json = new JSON;
    my $diff_status = $json->allow_nonref->utf8->relaxed->escape_slash->loose->allow_singlequote->allow_barekey->decode($command);
    $console= "";
    $config->read("etc/cloud.cnf");
    $config->check('SANDBOX');
    my $mem_info=get_active_memcache();

    print STDERR "Get the actions in memcache: ". $mem_info->{ip} . ":" . $mem_info->{port}."\n";

        
    my $memd = new Cache::Memcached {
               'servers' => [ $mem_info->{ip} . ":" . $mem_info->{port} ],
               'debug'   => 0,
               'compress_threshold' => 10_000,
     };
      my $json_todo = $memd->get("actions");
      
        if (!$json_todo )
        {
           print STDERR "No actions in memcache \n";
           return "ER0015";  
        }
        my $json_status = $memd->get("status");
        if (!$json_status )
        {
           print STDERR "No status in memcache \n";
           return "ER0015";  
        }

        my $status = $json->allow_nonref->utf8->relaxed->escape_slash->loose
         ->allow_singlequote->allow_barekey->decode($json_status);

        my $todo =  $json->allow_nonref->utf8->relaxed->escape_slash->loose
         ->allow_singlequote->allow_barekey->decode($json_todo);
       foreach  my $action (  @{ $todo->{actions}} ) {
        foreach  my $trigger(  @{ $diff_status->{events}} ) {
        
            print STDERR "action ip:".$action->{event_ip} . "\n";
            print STDERR "event ip:".$trigger->{ip} . "\n";
            
            print STDERR "action type:" . $action->{event_type} ."\n";
            print STDERR "event type:".$trigger->{type} . "\n";

            print STDERR "action state:" . $action->{event_state} ."\n";
            print STDERR "event state:" . $trigger->{state} ."\n";
            if  ($action->{event_ip} eq $trigger->{ip} && 
                 $action->{event_type} eq $trigger->{type} &&
                 $action->{event_state} eq $trigger->{state}
                ){
                 
                  my $command= '{"level":"'.$action->{do_level}. '","command":{"action":"'.$action->{do_action}.'","group":"'.$action->{do_group}.'","type":"all"} } ';
                  print STDERR "Test pass runing action : ". $command ."\n";
                  worker_cloud_command($command,$gearman_ip);  
                  $memd->set( "actions", '{"actions":[]}' );
                  sleep 1;
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