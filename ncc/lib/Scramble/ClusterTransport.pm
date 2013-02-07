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
use Gearman::XS qw(:constants);
use Gearman::XS::Client;


use strict;
use warnings FATAL => 'all';
our $gearman_timeout       = 3000;


sub worker_node_command($$) {
    my $cmd    = shift;
    my $ip     = shift;
    my $client = Gearman::XS::Client->new();
    my $res ="000000";
    $client->add_servers($ip);
    Scramble::ClusterUtils::log_debug("[worker_node_command] Info: Send to ip :". $ip ,1);
    Scramble::ClusterUtils::log_debug("[worker_node_command] Info: $cmd" ,1);
    
    
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


sub worker_config_command($$) {
    my $cmd    = shift;
    my $ip     = shift;
    my $client = Gearman::XS::Client->new();
    $client->add_servers($ip);
    Scramble::ClusterUtils::log_debug("[worker_config_command] Info: Worker_config_command for ip :". $ip ,1);
    Scramble::ClusterUtils::log_debug($cmd,2);
   
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
