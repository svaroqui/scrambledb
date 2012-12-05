#!/usr/bin/env perl

use strict;
use warnings;


use Getopt::Std;

use Gearman::XS qw(:constants);
use Gearman::XS::Worker;
#use Proc::PID_File;
#use Proc::Daemon;




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
    printf(STDERR "%s\n", $worker->error());
    exit(1);
}

$ret = $worker->add_function("node_cmd", 0, \&node_cmd, 0);
if ($ret != GEARMAN_SUCCESS) {
    printf(STDOUT "%s\n", $worker->error());
}

while (1) {
    my $ret = $worker->work();
    if ($ret != GEARMAN_SUCCESS) {
    	printf(STDOUT "%s\n", $worker->error());
    }
	else 
	{
	 printf(STDOUT "%s\n", "GEARMAN WORKER SUCCESS!");

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
system($command);
if ( $? == -1 )
{      
        return "false"
}   
    else {
        return "true"
    }
}

exit;

