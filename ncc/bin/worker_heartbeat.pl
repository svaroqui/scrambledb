#!../perl/bin/perl
use strict;
use Class::Struct;
use warnings FATAL => 'all';
use Sys::Hostname;
use Gearman::XS qw(:constants);
use Gearman::XS::Client;
use JSON;
our $SKYDATADIR = $ ENV {SKYDATADIR};


my $interface;
my %IPs;
foreach (qx{ (LC_ALL=C /sbin/ifconfig -a 2>&1) }) {
    $interface = $1 if /^(\S+?):?\s/;
    next unless defined $interface;
    $IPs{$interface}->{STATE} = uc($1) if /\b(up|down)\b/i;
    $IPs{$interface}->{IP}    = $1     if /inet\D+(\d+\.\d+\.\d+\.\d+)/i;
}


open my $LOG , q{>>}, $SKYDATADIR."/log/hearbeat.log"
    or die "can't create 'ncc.log'\n";



my $config_file = "etc/cloud.cnf";
my $action ="ping";
my $group="all"; 
my $type="db";
if ( ! defined $type ) {$type='all';}



my $command="{command:{action:'$action',group:'$group',type:'$type'}}";

print "$command\n";
my $json = new JSON;
my $json_text = $json->allow_nonref->utf8->relaxed->escape_slash->loose->allow_singlequote->allow_barekey->decode($command);
my $TIME=10;

sub gearman_client() {
  my $client = Gearman::XS::Client->new();
  $client->add_servers("localhost");
  (my $ret,my $result) = $client->do('cluster_cmd', $command);
  print  $ret;
    if ($ret == 0) {
        printf(STDOUT "%s\n", $client->error());
    }
	else 
	{
	 printf(STDOUT "%s\n",  $result);

	}
}

while ( 1 )  
{
   sleep $TIME;
   gearman_client();
 }
 

