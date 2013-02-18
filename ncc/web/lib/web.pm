package web;
use Dancer ':syntax';
use English qw( -no_match_vars );
use strict;
use Sys::Hostname;
use Gearman::XS qw(:constants);
use Gearman::XS::Client;

sub get_grid_status_services($) {
  my $status =shift;  
  my @statusgrid;   
  my $host_info;

   
    foreach my $service (  @{ $status->{"services"}} )  {
      
       foreach my $host (keys $service) {
      
      my $host_info = $service->{$host};
       my $action_add  = 
       {
        name       => $host,
        mode       => $host_info->{"mode"},
        ip         => $host_info->{"ip"}, 
        status    => $host_info->{"status"},
        state     => $host_info->{"state"},
        time  => $host_info->{"time"}
       
       };
    push(@statusgrid , $action_add);   

    }
  }
    
  return \@statusgrid;
}

sub get_grid_config_clouds($) {
  my $status =shift;  
  my @statusgrid;   
  my $host_info;

   
    foreach my $service  (keys( %{ $status->{"cloud"}} ) ) {
        my $host_info = $status->{"cloud"}->{$service};
        $host_info->{'id'} = $service; 
        push(@statusgrid ,$host_info);   
  }
    
  return \@statusgrid;
}


sub get_grid_status_instances($) {
  my $status =shift;  
  my @statusgrid;   
  my $host_info;

   
    foreach my $service (  @{ $status->{"instances"}} )  {
      
       foreach my $host (keys $service) {
      
      my $host_info = $service->{$host};
       my $action_add  = 
       {
      
        name       => $host_info->{"id"},
        ip         => $host_info->{"ip"}, 
        state     => $host_info->{"state"},
       
       };
    push(@statusgrid , $action_add);   

    }
  }
    
  return \@statusgrid;
}


sub get_json_local_infos(){
  system("cat /proc/meminfo |  grep MemTotal | awk '{print \$2}'"); 
  my $ram =$? ;

   my $interface;
   my %IPs;
    foreach (qx{ (LC_ALL=C /sbin/ifconfig -a 2>&1) }) {
        $interface = $1 if /^(\S+?):?\s/;
        next unless defined $interface;
        $IPs{$interface}->{STATE} = uc($1) if /\b(up|down)\b/i;
        $IPs{$interface}->{STATE} =defined( $IPs{$interface}->{STATE}) ?  $IPs{$interface}->{STATE} : "na";
        $IPs{$interface}->{IP}    = $1     if /inet\D+(\d+\.\d+\.\d+\.\d+)/i; 
        $IPs{$interface}->{IP} =defined( $IPs{$interface}->{IP}) ?  $IPs{$interface}->{IP} : "na";
    }
  my $json       = new JSON ;
  my $json_interfaces = $json->allow_nonref->utf8->encode(\%IPs);
  return $json_interfaces;
 }



sub gearman_client($) {
  my $command =shift;  
  my $client = Gearman::XS::Client->new();
  $client->add_servers("localhost");
 
  (my $ret, my $result) = $client->do('cluster_cmd', $command);
    $result =~ s/\n//g; 
    use JSON;
    my $json      = new JSON;
    my $json_text =
    $json->allow_nonref->utf8->relaxed->escape_slash->loose->allow_singlequote->allow_barekey->decode($result);
    return $json_text;
    
    
}



our $VERSION = '0.1';
set serializer => 'JSON';

get '/' => sub {
    template 'index';
};
get '/status' => sub {
    template 'status';    
};

get '/services/status' => sub {
   
        my $command='{"level":"services","command":{"action":"status","group":"all","type":"all"}, "host":{"interfaces":['. get_json_local_infos .'] }}';
        my $res=gearman_client($command);
        my $resgrid=get_grid_status_services($res);
        return $resgrid ;
       
};
get '/instances/status' => sub {
   
        my $command='{"level":"instances","command":{"action":"status","group":"all","type":"all"}, "host":{"interfaces":['. get_json_local_infos .'] }}';
        my $res=gearman_client($command);
        my $resgrid=get_grid_status_instances($res);
        return $resgrid ;
       
};

get '/services/:action/:group/:type' => sub {
       
        my $level ="services";

        my $action =params->{action};
        my $group=params->{group}; 
        my $type=params->{type};
        if ( ! defined $type ) {$type='all';}
        if ( ! defined $group ) {$group='all';}
        if ( ! defined $level ) {$level='service';}
        my $command='{"level":"'. $level .'","command":{"action":"'.$action.'","group":"'.$group.'","type":"'.$type.'"}, "host":{"interfaces":['. get_json_local_infos .'] }}';
        my $resjson=gearman_client($command);
        
        return $resjson ;
};


get '/config/:action/:group/:type' => sub {
 my $level ="config";

        my $action =params->{action};
        my $group=params->{group}; 
        my $type=params->{type};
        my $command='{"level":"'. $level .'","command":{"action":"'.$action.'","group":"'.$group.'","type":"'.$type.'"}, "host":{"interfaces":['. get_json_local_infos .'] }}';
        my $resjson=gearman_client($command);
        
        return $resjson ;
};

get '/config/getclouds' => sub {
 my $level ="config";

        my $command='{"level":"config","command":{"action":"display","group":"all","type":"all"}, "host":{"interfaces":['. get_json_local_infos .'] }}';
        my $res=gearman_client($command);
        my $resgrid=get_grid_config_clouds($res);
        return $resgrid ;
        
};


true;
