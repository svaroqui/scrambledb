#!/usr/local/skysql/perl/bin/perl
use strict;
use Class::Struct;
use warnings FATAL => 'all';

use Scramble::Common::ClusterUtils;
use Scramble::Common::Config;


our $SKYBASEDIR            = $ENV{SKYBASEDIR};
our $SKYDATADIR            = $ENV{SKYDATADIR};
our $config                = new Scramble::Common::Config::;
$config->read($SKYBASEDIR."/ncc/etc/cloud.cnf");
$config->check('SANDBOX');

my $cloud = Scramble::Common::ClusterUtils::get_active_cloud($config);
my $sshkey = $SKYBASEDIR . "/ncc/etc/" . $cloud->{public_key} ;
my $cmd= "ssh -f -i ". $sshkey . " -L 4730:127.0.0.1:4730 ".  $cloud->{elastic_ip} . " -N < /dev/null > ".$SKYDATADIR. "/log/tunnel.log 2>&1 & ";

system ($cmd);
$cmd='ps ax|grep ssh|grep 4730|grep 4730 |awk "{ print  $1 }" | head -n1';
my $pid=`$cmd`;

system ("echo '". $pid ."' > ". $SKYDATADIR ."/tmp/tunnel.pid ");

print STDERR "Starting private instance ";



