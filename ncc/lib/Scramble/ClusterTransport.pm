# Copyright (c) 2013 Stephane VAROQUI http://skysql.com/
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish, dis-
# tribute, sublicense, and/or sell copies of the Software, and to permit
# persons to whom the Software is furnished to do so, subject to the fol-
# lowing conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABIL-
# ITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
# SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE.


package Scramble::ClusterTransport;
use Scramble::ClusterUtils;
use Scramble::ClusterLog;
use Gearman::XS qw(:constants);
use Gearman::XS::Client;
our $log = new Scramble::ClusterLog;

use strict;
use warnings FATAL => 'all';
our $gearman_timeout       = 3000;


sub worker_node_command($$) {
    my $cmd    = shift;
    my $ip     = shift;
    my $client = Gearman::XS::Client->new();
    my $res ="000000";
    $client->add_servers($ip);
    $log->log_debug("[worker_node_command] Info: Send to ip :". $ip ,1);
    $log->log_debug("[worker_node_command] Info: $cmd" ,1);
    
    
    ( my $ret, my $result ) = $client->do( 'node_cmd', $cmd );

    if ( $ret == GEARMAN_SUCCESS ) {
        $log->log_debug("[worker_node_command] Info: $cmd".$result ,1);
        
    }
    else { 

       
       my $result_error  = 
       {
        command    => $cmd,
        result     => "",
        return     => "ER0002"
       };
       my $json = new JSON;
       $result= $json->allow_blessed->convert_blessed->encode($result_error);
    }
    $log->report_action($ip,$cmd,$res,get_result_from_node_cmd($result));
    return get_return_error_from_node_cmd($result);

}


sub worker_config_command($$) {
    my $cmd    = shift;
    my $ip     = shift;
    my $client = Gearman::XS::Client->new();
    $client->add_servers($ip);
    $log->log_debug("[worker_config_command] Info: Worker_config_command for ip :". $ip ,1);
    $log->log_debug($cmd,2);
   
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

sub get_return_error_from_node_cmd($){
 my $result=shift; 
 my $json      = new JSON;
 print STDERR $result;
 my $perl_class = $json->allow_nonref->utf8->relaxed->escape_slash->loose->allow_singlequote->allow_barekey->decode($result);
 if (defined($perl_class->{return})) {
      if ( $perl_class->{return}  eq "000000") {
          return $perl_class->{return};
      } 
      return $perl_class->{return};
 } else {
   return "ER0030";
 }  
}

sub get_result_from_node_cmd($){
 my $result=shift; 
 my $json      = new JSON;
 print STDERR $result;
 my $perl_class = $json->allow_nonref->utf8->relaxed->escape_slash->loose->allow_singlequote->allow_barekey->decode($result);
 if (defined($perl_class->{result})) {
      if ( $perl_class->{return}  eq "000000") {
          return $perl_class->{result};
      } 
      return "";
 } else {
   return "";
 }  
}