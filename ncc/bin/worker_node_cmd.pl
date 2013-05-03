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
use warnings;
use JSON;
use Getopt::Std;
use Gearman::XS qw(:constants);
use Gearman::XS::Worker;
use Scramble::ClusterLog;
use Scramble::ClusterConfig;
our $config               = new Scramble::ClusterConfig;
our $log                  = new Scramble::ClusterLog;
our $SKYBASEDIR            = $ENV{SKYBASEDIR};
$config->read($SKYBASEDIR."/ncc/etc/cloud.cnf");
$log->set_logs($config);
my %opts;
if (!getopts('h:p', \%opts)) {
    usage();
    exit(1);
}

my $host	= $opts{h}	|| '';
my $port	= $opts{p}	|| 0;

my $worker = new Gearman::XS::Worker;

my $ret = $worker->add_server($host, $port);
if ($ret != GEARMAN_SUCCESS) {
    $log->log_debug("[gearman_worker] Error: ".$worker->error(),1,"node"); 
    exit(1);
}

$ret = $worker->add_function("node_cmd", 0, \&node_cmd, 0);
if ($ret != GEARMAN_SUCCESS) {
    $log->log_debug("[gearman_worker] Error: ".$worker->error(),1,"node"); 
   
}

while (1) {
    my $ret = $worker->work();
    if ($ret != GEARMAN_SUCCESS) {
    	$log->log_debug("[gearman_worker] Error: ".$worker->error(),1,"node"); 
    }
    else 
    {
      $log->log_debug("[gearman_worker] GEARMAN WORKER SUCCESS!",1,"node"); 

    }
}

sub usage {
    printf("\nusage: %s [-h <host>] [-p <port>]\n", $0);
    printf("\t-h <host>  - job server host\n");
    printf("\t-p <port>  - job server port\n");
}

sub node_cmd {
    my ($job, $options) = @_;

    my $command		= $job->workload();
    chomp $command;
    print STDOUT "<$command>\n";

#open(my $res,"$command") || die "Failed: $!\n";
my  $res =  `$command`; 

  my $json = new JSON;   

 if (  $? == -1 )
 {      
       my $result  = 
       {
        command    => $command,
        result     => "",
        return     => "ER0018"
       };
      
       my $json_result= $json->allow_blessed->convert_blessed->encode($result);
       return $json_result;
 }   
    else {
      # $res =~ s/\n//g; 

       my $result  = 
       {
        command    => $command,
        result     => $res,
        return     => "000000"
       };
       
       my $json_result= $json->allow_blessed->convert_blessed->encode($result);
       return $json_result;
    }
 }

exit;

