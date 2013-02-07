#!/usr/bin/env perl

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

package Scramble::ClusterLog;


use strict;
use warnings FATAL => 'all';
use Log::Log4perl qw(:easy);
use English qw( -no_match_vars );
use JSON;



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




our $VERSION = '0.01';
#-------------------------------------------------------------------------------

sub new {
  my $class = shift;   
	
    my $self  = 
       {
        _log_level     => 2,
        console => [],
        actions => [],
               @_,
       };
  return bless $self, $class;

}
#-------------------------------------------------------------------------------





sub log_debug($$$){  
  my $self = shift;  
  my $message =shift;
  my $level=shift;
  if ($level <= $self->{_log_level}){
   my $le_localtime = localtime;
   print STDERR $le_localtime ." ";
   print STDERR $message;
   print STDERR "\n";
  }
 # open my $LOG, q{>>}, $SKYDATADIR . "/log/worker_cluster_cmd.log"
 # or die "can't c   reate 'worker_cluster_cmd.log'\n";  

}

sub init_console($){
 my $self = shift;    
 my @cmd_console;
 my @cmd_action;
 @{$self->{console}}=@cmd_console;
 @{$self->{actions}}=@cmd_action;
}

sub get_console($) {
 my $self = shift;   
 return @{$self->{console}};
}


sub get_actions($) {
 my $self = shift;   
 return @{$self->{actions}};    
 
}


sub init($$) {
	my $file = shift;
	my $progam = shift;

	my @paths = qw(/etc ./);

	# Determine filename
	my $fullname;
	foreach my $path (@paths) {
		if (-r "$path/$file") {
			$fullname = "$path/$file";
			last;
		}
	}

	# Read configuration from file
	if ($fullname) {
		Log::Log4perl->init($fullname);
		return;
	}

	# Use default configuration
	my $conf = "
		log4perl.logger = INFO, LogFile

		log4perl.appender.LogFile                           = Log::Log4perl::Appender::File
		log4perl.appender.LogFile.Threshold                 = INFO 
		log4perl.appender.LogFile.filename                  = $progam.log
		log4perl.appender.LogFile.recreate                  = 1
		log4perl.appender.LogFile.layout                    = PatternLayout
		log4perl.appender.LogFile.layout.ConversionPattern  = %d %5p %m%n
	";
	Log::Log4perl->init(\$conf);

}

sub debug() {
	my $stdout_appender =  Log::Log4perl::Appender->new(
		'Log::Log4perl::Appender::Screen',
		name      => 'ScreenLog',
		stderr    => 0
	);
	my $layout = Log::Log4perl::Layout::PatternLayout->new('%d %5p %m%n');
	$stdout_appender->layout($layout);
	Log::Log4perl::Logger->get_root_logger()->add_appender($stdout_appender);
	Log::Log4perl::Logger->get_root_logger()->level($DEBUG);
}


sub report_action($$$$) {
    my $self = shift;
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
    push(@{$self->{actions}} ,
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

sub report_status($$$$$) {
    my $self = shift;
    my $hostinfo = shift;
    my $cmd  = shift;
    my $err  = shift;
    my $host = shift;
    $self->log_debug( "[report_status] ". $cmd , 1);
    my $le_localtime = localtime;
    my $status ="na";
    if ( $hostinfo->{status}) { 
     $status=$hostinfo->{status};
    }
    push(@{$self->{console}},
       '{"'.$host .'":{"time":"'
      . $le_localtime
      . '","name":"'
      .  $host
      . '","ip":"'
      .  $hostinfo->{ip} 
      . '","mode":"'
      .  $hostinfo->{mode}  
      . '","status":"'
      .  $status
      . '","code":"'
      . $err
      . '","state":"'
      . $ERRORMESSAGE{$err}  
      . '"}}'
   ); 
 
}


sub log_json($$$){ 
  my $self = shift;  
  my $json_text =shift;
  my $level=shift;
  
  my $json      = new JSON;
  my @perl_class = $json->allow_nonref->utf8->relaxed->escape_slash->loose->allow_singlequote->allow_barekey->decode($json_text);

  
  if ($level <= $self->{_log_level}){  
    use Data::Dumper;
    $Data::Dumper::Terse     = 1;       
    $Data::Dumper::Quotekeys = 0;
    $Data::Dumper::Indent    = 1;       
    $Data::Dumper::Pair      = ":";
    $Data::Dumper::Indent    = 1;
    $Data::Dumper::Useqq     = 0; 
    print  STDERR Dumper(@perl_class);
  
    }       
}
 


1;

