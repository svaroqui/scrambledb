#!/usr/local/skysql/perl/bin/perl
use strict;
use Class::Struct;
use warnings FATAL => 'all';
use Sys::Hostname;
use Gearman::XS qw(:constants);
use Gearman::XS::Client;
use JSON;







sub gearman_client() {
  my $client = Gearman::XS::Client->new();
  $client->add_servers("127.0.0.1");
 
  (my $ret, my $result) = $client->do('cloud_cmd', '{"command":{"group":"all","action":"status","type":"all"},"cloud":{"template":"ami-45f8702c","zone":"us-east-1b","version":"1.5","status":"master","region":"us-east","key":"SDS145000","host":"na","elastic_ip":"107.21.41.133","password":"2LU5gIQ8bbA8hhuaGBYDVvxCZikzuOyfTxDSQzSZ","ex_vdc":"na","vpc":"vpc-7032621b","user":"AKIAJR7YEOZPXASJCYDQ","public_key":"SDS145000.pem","subnet":"subnet-49326222","security_groups":"sg-2128c74e","instance_type":"t1.micro","driver":"EC2"}}');
 if ( $ret == GEARMAN_SUCCESS ) {
      if ( !defined $result ) {
        print STDERR  "ER0006";
     } else { 
     my $json      = new JSON;
my @json_text =
      $json->utf8->relaxed->allow_singlequote->allow_barekey->decode($result);
use Data::Dumper;
$Data::Dumper::Terse     = 1;       # don't output names where feasible
    $Data::Dumper::Quotekeys = 0;
    $Data::Dumper::Indent    = 1;       # mild pretty print
    $Data::Dumper::Pair      = ":";
    $Data::Dumper::Indent    = 1;
    $Data::Dumper::Useqq     = 0; 
 print  STDERR Dumper(@json_text);
        
     }
    
   }  

 #print STDOUT $result;
    
}


gearman_client();



