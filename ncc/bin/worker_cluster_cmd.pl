#!/usr/bin/env perl
use strict;
use Class::Struct;
use warnings FATAL => 'all';
use Common::Config;
use Sys::Hostname;
use Gearman::XS qw(:constants);
use Gearman::XS::Client;
use Gearman::XS::Worker;
use Cache::Memcached;
use Data::Dumper;
use JSON;
use DBI;

struct 'host' => {
    name                 => '$',
    ip                   => '$',
    port                 => '$',
    peer                 => '$',
    mode                 => '$',
    replication_user     => '$',
    replication_password => '$',
    mysql_port           => '$',
    mysql_user           => '$',
    mysql_password       => '$',
    mysql_version        => '$',
    mysql_cnf            => '$',
    datadir              => '$'
};

struct 'nosql' => {
    name => '$',
    ip   => '$',
    mode => '$',
    port => '$'
};

our %ERRORMESSAGE = (
    "000000" => "On",
    "ER0001" => "SQL command failure",
    "ER0002" => "Remote manager communication failure ",
    "ER0003" => "Database communication failure ",
    "ER0004" => "Remote manager command failure ",
    "ER0005" => "Memcache communication failure ",
    "ER0006" => "Remote write configuration failure ",
    "ER0007" => "Remote monitoring execution failure ",
    "ER0008" => "Can't create remote monitoring db",
    "ER0009" => "Database error in memc_set() ",
    "ER0010" => "Database error in memc_servers_set() "

);

our $hashcolumn;
our $createtable           = "";
our $like                  = "none";
our $database              = "";
our $SKYBASEDIR            = $ENV{SKYBASEDIR};
our $SKYDATADIR            = $ENV{SKYDATADIR};
our $gearman_timeout       = 2000;
our $mysql_connect_timeout = 3;
our $config                = new SKY::Common::Config::;

$config->read("etc/cloud.cnf");
$config->check('SANDBOX');



my %ServiceIPs;
my @console;
my $cloud = get_master_cloud();
my  $sshkey="/ncc/etc/" . $cloud->{public_key} ;


open my $LOG, q{>>}, $SKYDATADIR . "/log/worker_cluster_cmd.log"
  or die "can't create 'worker_cluster_cmd.log'\n";

my $worker = new Gearman::XS::Worker;
my $ret = $worker->add_server( '', 0 );

if ( $ret != GEARMAN_SUCCESS ) {
    printf( STDERR "%s\n", $worker->error() );
    exit(1);
}

$ret = $worker->add_function( "cluster_cmd", 0, \&cluster_cmd, 0 );
if ( $ret != GEARMAN_SUCCESS ) {
    printf( STDERR "%s\n", $worker->error() );
}

while (1) {

    my $ret = $worker->work();
    if ( $ret != GEARMAN_SUCCESS ) {
        printf( STDERR "%s\n", $worker->error() );
    }
}

sub is_ip_localhost($) {
    
    my $testIP = shift;
    
    my %IPs;
    my $interface;

    foreach (qx{ (LC_ALL=C /sbin/ifconfig -a 2>&1) }) {
        $interface = $1 if /^(\S+?):?\s/;
        next unless defined $interface;
        $IPs{$interface}->{STATE} = uc($1) if /\b(up|down)\b/i;
        $IPs{$interface}->{IP}    = $1     if /inet\D+(\d+\.\d+\.\d+\.\d+)/i;
    }

    foreach my $key ( sort keys %IPs ) {
        if ( $IPs{$key}->{IP} eq $testIP ) {
            return 0;
        }
    }
    return 1;
}

sub add_ip_to_list($) {
    my $testIP = shift;
    $ServiceIPs{$testIP}=0;
    return 1;
}


sub cluster_cmd {
    my ( $job, $options ) = @_;
    my $action;
    my $group;
    my $type;
    my $query;
    my $table   = "";
    my $command = $job->workload();
    my $json    = new JSON;
    my $json_text =
      $json->allow_nonref->utf8->relaxed->escape_slash->loose
      ->allow_singlequote->allow_barekey->decode($command);
    my $peer_host_info = "";
    my $myhost         = hostname;
    my $ddlallnode     = 0;
    my $ddlallproxy    = 0;
    my $ret            = "true";
    my @cmd_console;
    @console=@cmd_console;
    $config->read("etc/cloud.cnf");
    $config->check('SANDBOX');

    my $json2       = new JSON;
    my $json_config = $json2->allow_blessed->convert_blessed->encode($config);
    $Data::Dumper::Terse     = 1;       # don't output names where feasible
    $Data::Dumper::Quotekeys = 0;
    $Data::Dumper::Indent    = 1;       # mild pretty print
    $Data::Dumper::Pair      = " = ";
    $Data::Dumper::Indent    = 2;
    $Data::Dumper::Useqq     = 0;

    #  print  STDERR Dumper($config);
    #  print  STDERR $json_config;

    print STDERR "Receive command : $command\n";
    $action   = $json_text->{command}->{action};
    $group    = $json_text->{command}->{group};
    $type     = $json_text->{command}->{type};
    $query    = $json_text->{command}->{query};
    $database = $json_text->{command}->{database};
    my $level  = $json_text->{level};
    
   
       
     if ( $level eq "instances" ){
        my $json_cloud       = new JSON ;
        my $json_cloud_str = $json_cloud->allow_nonref->utf8->encode($cloud);
        my $json_cmd       = new JSON ;
        my $json_cmd_str = $json_cmd->allow_nonref->utf8->encode($json_text->{command});
        
        $json_cloud_str =' {"command":'.$json_cmd_str.',"cloud":'.$json_cloud_str.'}'; 
        print  STDERR $json_cloud_str;
        worker_cloud_command($json_cloud_str,"localhost");

     }  
    else {
        if ( $action eq "sql" ) {
            $like       = "";
            $ddlallnode = spider_is_ddl_all_node($query);
            if ( $like ne "" ) { $query = spider_rewrite_query_like(); }
        }
        elsif ( $action eq "start" ) {
            $ret = bootstrap_config();
        }
        elsif ( $action eq "bootstrap_binaries" ) {
            $ret = bootstrap_binaries();
        }
        elsif ( $action eq "bootstrap_ncc" ) {
            $ret = bootstrap_ncc();
        }
        elsif ( $action eq "bootstrap_config" ) {
            $ret = bootstrap_config();
        }
        elsif ( $action eq "rolling_restart" ) {
            $ret = mysql_rolling_restart();
        }
        elsif ( $action eq "ping" ) {
            print STDERR "Entering :start monitoring\n";
            $ret = ping_election($json_text);

        }

        foreach my $host ( sort( keys( %{ $config->{db} } ) ) ) {
            my $host_info = $config->{db}->{default};
            $host_info = $config->{db}->{$host};
            add_ip_to_list($host_info->{ip});
            if ( $host_info->{mode} ne "spider" ) {
                $peer_host_info = $config->{db}->{ $host_info->{peer}[0] };
            }
            my $pass = 1;
            print STDOUT $group . " vs " . $host;
            if ( $group ne $host ) { $pass = 0 }

            if ( $pass == 0 && $group eq "local" && $myhost eq $host_info->{ip} ) {
                $pass = 1;
            }
            if ( $pass == 0 && $group eq "all" ) { $pass = 1 }
            if ( $type ne $host_info->{mode} && $type ne 'all' ) { $pass = 0 }
            if ( $pass == 1 ) {
                my $le_localtime = localtime;
                print $LOG $le_localtime . " processing " . $host . " on $myhost\n";
                if ( $action eq "install" ) {
                    $ret = install_sandbox( $host_info, $host, $host_info->{mode} );
                }
                if ( $action eq "stop" ) {
                    $ret = node_cmd( $host_info, $host, $action );
                }
                if ( $action eq "sync" ) {
                    $ret = node_sync( $host_info, $host, $peer_host_info );
                }
                if ( $action eq "start" ) {
                    $ret = node_cmd( $host_info, $host, $action, $query );
                }
                if ( $action eq "remove" ) {
                    $ret = node_cmd( $host_info, $host, $action, $query );
                }
                if ( $action eq "restart" ) {
                    $ret = node_cmd( $host_info, $host, $action );
                }
                if ( $action eq "status" ) {
                    $ret = node_cmd( $host_info, $host, $action );
                }
                if ( $action eq "join" ) { $ret = spider_node_join( $host_info, $host ); }
                if ( $action eq "switch" ) { $ret = mha_master_switch($host); }
                if ( $action eq "sql" ) {
                    $ret = spider_node_sql( $host_info, $host, $action, $query, $database,
                        $ddlallnode );
                }
            }
        }
        if ( $action eq "sql" && ( $ddlallnode == 0 || $ddlallnode == 2 ) ) {
            spider_create_table_info( $query, $ddlallnode );
        }

        foreach my $nosql ( sort( keys( %{ $config->{nosql} } ) ) ) {
            my $host_info = $config->{nosql}->{default};
            $host_info = $config->{nosql}->{$nosql};
            add_ip_to_list($host_info->{ip});
            my $pass = 1;
            print STDOUT $group . " vs " . $nosql;
            if ( $group ne $nosql ) { $pass = 0 }

            if ( $pass == 0 && $group eq "local" && $myhost eq $host_info->{ip} ) {
                $pass = 1;
            }
            if ( $pass == 0 && $group eq "all" ) { $pass = 1 }
            if ( $type ne $host_info->{mode} && $type ne 'all' ) { $pass = 0 }
            if ( $pass == 1 ) {
                my $le_localtime = localtime;
                if ( $action eq "stop" ) {
                    $ret = node_cmd( $host_info, $nosql, $action );
                }
                if ( $action eq "start" ) {
                    $ret = node_cmd( $host_info, $nosql, $action, $query );
                }
                if ( $action eq "restart" ) {
                    $ret = node_cmd( $host_info, $nosql, $action );
                }
                if ( $action eq "status" ) {
                    $ret = node_cmd( $host_info, $nosql, $action );
                }
            }

        }

        foreach my $host ( sort( keys( %{ $config->{proxy} } ) ) ) {
            my $host_info = $config->{proxy}->{default};
            $host_info = $config->{proxy}->{$host};
            add_ip_to_list($host_info->{ip});
            my $pass = 1;
            print STDOUT $group . " vs " . $host;
            if ( $group ne $host ) { $pass = 0 }

            if ( $pass == 0 && $group eq "local" && $myhost eq $host_info->{ip} ) {
                $pass = 1;
            }
            if ( $pass == 0 && $group eq "all" ) { $pass = 1 }
            if ( $type ne $host_info->{mode} && $type ne 'all' ) { $pass = 0 }
            if ( $pass == 1 ) {
                my $le_localtime = localtime;
                if ( $action eq "stop" ) {
                    $ret = node_cmd( $host_info, $host, $action );
                }
                if ( $action eq "start" ) {
                    $ret = node_cmd( $host_info, $host, $action, $query );
                }
                if ( $action eq "restart" ) {
                    $ret = node_cmd( $host_info, $host, $action );
                }
                if ( $action eq "status" ) {
                    $ret = node_cmd( $host_info, $host, $action );
                }
            }

        }
        foreach my $host ( sort( keys( %{ $config->{lb} } ) ) ) {
            my $host_info = $config->{lb}->{default};
            $host_info = $config->{lb}->{$host};
            add_ip_to_list($host_info->{ip});
            my $pass = 1;
            print STDOUT $group . " vs " . $host;
            if ( $group ne $host ) { $pass = 0 }

            if ( $pass == 0 && $group eq "local" && $myhost eq $host_info->{ip} ) {
                $pass = 1;
            }
            if ( $pass == 0 && $group eq "all" ) { $pass = 1 }
            if ( $type ne $host_info->{mode} && $type ne 'all' ) { $pass = 0 }
            if ( $pass == 1 ) {
                my $le_localtime = localtime;
                if ( $action eq "stop" ) {
                    $ret = node_cmd( $host_info, $host, $action );
                }
                if ( $action eq "start" ) {
                    $ret = node_cmd( $host_info, $host, $action, $query );
                }
                if ( $action eq "restart" ) {
                    $ret = node_cmd( $host_info, $host, $action );
                }
                if ( $action eq "status" ) {
                    $ret = node_cmd( $host_info, $host, $action );
                }
            }

        }

        foreach my $host ( sort( keys( %{ $config->{bench} } ) ) ) {
            my $host_info = $config->{bench}->{default};
            $host_info = $config->{bench}->{$host};
            my $pass = 1;
            print STDOUT $group . " vs " . $host;
            if ( $group ne $host ) { $pass = 0 }

            if ( $pass == 0 && $group eq "local" && $myhost eq $host_info->{ip} ) {
                $pass = 1;
            }
            if ( $pass == 0 && $group eq "all" ) { $pass = 1 }
            if ( $type ne $host_info->{mode} && $type ne 'all' ) { $pass = 0 }
            if ( $pass == 1 ) {
                my $le_localtime = localtime;
                if ( $action eq "install" ) {
                    $ret = install_bench( $host_info, $host, $action );
                }
                if ( $action eq "stop" ) {
                    $ret = stop_bench( $host_info, $host, $action );
                }
                if ( $action eq "start" ) {
                    $ret = start_bench( $host_info, $host, $action, $query );
                }
                if ( $action eq "restart" ) {
                    $ret = node_cmd( $host_info, $host, $action );
                }

            }

        }
          foreach my $host ( sort( keys( %{ $config->{monitor} } ) ) ) {
            my $host_info = $config->{monitor}->{default};
            $host_info = $config->{monitor}->{$host};
            my $pass = 1;
            print STDOUT $group . " vs " . $host;
            if ( $group ne $host ) { $pass = 0 }

            if ( $pass == 0 && $group eq "local" && $myhost eq $host_info->{ip} ) {
                $pass = 1;
            }
            if ( $pass == 0 && $group eq "all" ) { $pass = 1 }
            if ( $type ne $host_info->{mode} && $type ne 'all' ) { $pass = 0 }
            if ( $pass == 1 ) {
                my $le_localtime = localtime;

                if ( $action eq "stop" ) {
                    $ret = node_cmd( $host_info, $host, $action );
                }
                if ( $action eq "start" ) {
                    $ret = node_cmd( $host_info, $host, $action, $query );
                }
                if ( $action eq "restart" ) {
                    $ret = node_cmd( $host_info, $host, $action );
                }

            }

        }


        foreach my $key ( sort keys %ServiceIPs ) {
            print $key;
        }

   }
  
    return '{"'.$level.'":[' . join(',', @console) .']}';

}

sub lookup_table_name($) {
    my $lquery = shift;
    my @tokens = tokenize_sql($lquery);
    my $next_i = 1;
    for my $token (@tokens) {
        if ( lc $token eq "table" ) {
            return $tokens[$next_i];
        }
        $next_i++;
    }
}

sub spider_is_ddl_all_node($) {
    my $lquery = shift;
    my @tokens = tokenize_sql($lquery);
    my $create = 99;
    my $next_i = 1;
    for my $token (@tokens) {
        if ( lc $token eq "create" ) {
            if ( lc $tokens[$next_i] eq "table" ) {
                $create      = 1;
                $createtable = $tokens[ $next_i + 1 ];
                if ( scalar(@tokens) >= $next_i + 2 ) {
                    if ( $tokens[ $next_i + 2 ] eq "." ) {
                        $createtable = $tokens[ $next_i + 3 ];

#	$database = $tokens[$next_i+1];
#	need to be unquited but useless as the table in token is comming with the db
                    }
                }
            }
        }
        if ( lc $token eq "like" && $create == 1 ) {
            print STDERR "token:" . $token . " ct" . scalar(@tokens);
            $like = $tokens[$next_i];
            print STDERR " createtablelike: " . $like;

            if ( scalar(@tokens) >= $next_i + 2 ) {
                if ( $tokens[ $next_i + 1 ] eq "." ) {
                    print STDERR $like;
                    $like = $tokens[ $next_i + 2 ];
                }
            }
        }
        $next_i++;
    }
    if ( $create == 1 ) { return 0; }
    if (   lc $tokens[1] eq "trigger"
        || lc $tokens[1] eq "view"
        || lc $tokens[1] eq "procedure"
        || lc $tokens[1] eq "function" )
    {
        print STDERR "create statement to proxy node only \n";
        return 2;
    }
    return 1;
}

sub spider_rewrite_query_like() {

    my $host_info;

    foreach my $host ( keys( %{ $config->{db} } ) ) {
        if ( $config->{db}->{$host}->{mode} eq "master" ) {
            $host_info = $config->{db}->{default};
            $host_info = $config->{db}->{$host};
            my $dsn =
                "DBI:mysql:database="
              . $database
              . ";host="
              . $host_info->{ip}
              . ";port="
              . $host_info->{mysql_port}
              . ";mysql_connect_timeout="
              . $mysql_connect_timeout;
            my $dbh = DBI->connect(
                $dsn,
                $host_info->{mysql_user},
                $host_info->{mysql_password}
            );
            my $sql = "show create table " . $like;

            my $sth2 = $dbh->prepare($sql);
            $sth2->execute();
            print STDERR $sql;
            while ( my @row2 = $sth2->fetchrow_array() ) {
                my $createsql = $row2[1];
                print STDERR $createsql;

                return spider_swap_table_name($createsql);
            }

        }
    }

}

sub spider_swap_table_name($) {
    my $oriq   = shift;
    my $destq  = "";
    my @tokens = tokenize_sql($oriq);
    my $next_i = 1;
    for my $token (@tokens) {
        print STDERR $token . "\n";
        if ( lc $token eq "create" ) {
            if ( lc $tokens[$next_i] eq "table" ) {
                if ( $tokens[ $next_i + 2 ] eq "." ) {
                    $tokens[ $next_i + 3 ] = $createtable;
                }
                else {
                    $tokens[ $next_i + 1 ] = $createtable;
                }
            }
        }
        $destq = $destq . $token . " ";
        $next_i++;
    }
    return $destq;
}

sub spider_node_join($$) {
    my $host_joined     = shift;
    my $host_joined_key = shift;
    my $host_info;
    my $host_router;
    my $loop_host;

    my $dsn =
        "DBI:mysql:database="
      . $database
      . ";host="
      . $host_info->{ip}
      . ";port="
      . $host_info->{mysql_port}
      . ";mysql_connect_timeout="
      . $mysql_connect_timeout;

    my $database = "";
    foreach my $host ( keys( %{ $config->{db} } ) ) {
        if (   $host_joined_key ne $host
            && $config->{db}->{$host}->{mode} eq "master" )
        {
            $host_info = $config->{db}->{default};
            $host_info = $config->{db}->{$host};
        }
        if ( $config->{db}->{$host}->{mode} eq "router" ) {
            $host_router = $config->{db}->{default};
            $host_router = $config->{db}->{$host};
        }
    }
    $dsn =
        "DBI:mysql:database="
      . $database
      . ";host="
      . $host_info->{ip}
      . ";port="
      . $host_info->{mysql_port}
      . ";mysql_connect_timeout="
      . $mysql_connect_timeout;
    my $dbh = DBI->connect(
        $dsn,
        $host_info->{mysql_user},
        $host_info->{mysql_password}
    );
    my $sql =
" select table_schema, table_name from information_schema.tables where TABLE_SCHEMA<>'mysql' AND TABLE_SCHEMA<>'information_schema'";
    print STDERR $sql;
    my $result = $dbh->selectall_arrayref( $sql, { Slice => {} } );
    my $createsql = '';
    my $sth2;

    for my $i ( 0 .. $#$result ) {
        my $onlinealter =
            $SKYBASEDIR
          . "/ncc/bin/pt_online_schema_change h="
          . $host_router->{ip} . ",P="
          . $host_router->{mysql_port} . ",u="
          . $host_router->{mysql_user} . ",p="
          . $host_router->{mysql_password} . ",D="
          . $result->[$i]{table_schema} . ",t="
          . $result->[$i]{table_name}
          . " --execute";

        print STDERR $onlinealter;
        worker_node_command( $onlinealter, $host_router->{ip} );
    }
    $dbh->disconnect;
}

sub spider_node_sql($$$$$$$) {
    my $self       = shift;
    my $node       = shift;
    my $cmd        = shift;
    my $query      = shift;
    my $database   = shift;
    my $ddlallnode = shift;
    chop( my $my_arch = `arch` );
    my $param  = "";
    my $err    = "000000";
    my $result = "";

# do not produce per node action if only DDL on the proxy (tiggers,views,procedures,functions)
    if ( $ddlallnode == 2 ) {
        print STDERR "Skip sql action for data node " . $node . "\n";
        return 0;
    }
    if (
        $self->{mode} ne "router"
        && (   $self->{mode} eq "master"
            || $self->{mode} eq "slave"
            || $ddlallnode == 1 )
      )
    {

        try {
            my $dsn =
                "DBI:mysql:database="
              . $database
              . ";host="
              . $self->{ip}
              . ";port="
              . $self->{mysql_port}
              . ";mysql_connect_timeout="
              . $mysql_connect_timeout;
            my $dbh =
              DBI->connect( $dsn, $self->{mysql_user},
                $self->{mysql_password} );
            print STDERR $node . ": spider_node_sql : " . $query . "\n";
            $dbh->do($query);

            if ( $ddlallnode == 0 ) {

                my $unquote_tbl = $createtable;
                $unquote_tbl =~ s/`//g;
                my $requete =
"SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE table_name='$unquote_tbl' and table_schema='$database' and COLUMN_KEY='PRI' LIMIT 1 ";
                print STDERR $requete . "\n";
                my $sth = $dbh->prepare($requete);
                $sth->execute();
                while ( my @row = $sth->fetchrow_array ) {
                    $hashcolumn = $row['0'];
                }
            }
            else {

                #	my $sth = $dbh->prepare($query);
                #	$sth->execute();
                #	$sth->finish;
            }
            $dbh->disconnect;
        }
        catch Error with {
            $err = "ER0003";
        }
    }
}

sub spider_create_table_info($$) {
    my $query      = shift;
    my $ddlallnode = shift;
    my $err        = "000000";
    my $host_info;
    my $engine     = "";
    my $monitoring = "5012";

    if ( $ddlallnode == 0 ) {
        print STDERR "Create spider table on all proxy node \n";
        my $cptpart = 0;
        foreach my $host ( keys( %{ $config->{db} } ) ) {
            $host_info = $config->{db}->{default};
            $host_info = $config->{db}->{$host};
            if ( $host_info->{mode} eq "master" ) {
                if ( $cptpart ne 0 ) { $engine = $engine . ",\n"; }
                my $peer_host_info = $host_info->{peer}[0];
                $createtable =~ s/`//g;
                $engine =
                    $engine
                  . "\n partition pt"
                  . $cptpart
                  . " values in ("
                  . $cptpart
                  . ") comment ' table \""
                  . $createtable
                  . "\", host \""
                  . $host_info->{ip} . " "
                  . $config->{db}->{$peer_host_info}->{ip}
                  . "\", port \""
                  . $host_info->{mysql_port} . " "
                  . $config->{db}->{$peer_host_info}->{mysql_port} . "\"  '";

# $engine=$engine . "\n partition pt" . $cptpart ." values in (".$cptpart.") comment ' table \"".  $createtable ."\", host \"".$host_info->{ip} ." " .  $config->{db}->{$peer_host_info}->{ip} ."\", port \"".$host_info->{mysql_port} ." " .  $config->{db}->{$peer_host_info}->{mysql_port} .  "\" ,mbk \"2\", mkd \"2\", msi \"" . $monitoring  . "\" '";
                $cptpart++;
            }
        }
        $engine =
            " engine=Spider Connection 'user \""
          . $host_info->{mysql_user}
          . "\",password \""
          . $host_info->{mysql_password}
          . "\" '\n partition by list(mod("
          . $hashcolumn . ","
          . $cptpart . "))("
          . $engine . ")";
        foreach my $host ( keys( %{ $config->{db} } ) ) {
            $host_info = $config->{db}->{default};
            $host_info = $config->{db}->{$host};
            if (   $host_info->{mode} eq "spider"
                || $host_info->{mode} eq "monitor" )
            {
                my $dsn =
                    "DBI:mysql:database="
                  . $database
                  . ";host="
                  . $host_info->{ip}
                  . ";port="
                  . $host_info->{mysql_port}
                  . ";mysql_connect_timeout="
                  . $mysql_connect_timeout;
                my $dbh = DBI->connect(
                    $dsn,
                    $host_info->{mysql_user},
                    $host_info->{mysql_password}
                );
                my $requete = $query . " \n" . $engine . "\n";
                print STDOUT $requete;
                my $sth = $dbh->do($requete);
            }
        }
    }
    if ( $ddlallnode == 2 ) {
        sql_command_proxy($query);
    }
    return $err;
}

sub sql_command_proxy($) {
    my $query = shift;
    my $host_info;
    my $err = "000000";
    foreach my $host ( keys( %{ $config->{db} } ) ) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        if ( $host_info->{mode} eq "spider" ) {
            use Error qw(:try);
            my $dsn =
                "DBI:mysql:database="
              . $database
              . ";host="
              . $host_info->{ip}
              . ";port="
              . $host_info->{mysql_port}
              . ";mysql_connect_timeout="
              . $mysql_connect_timeout;
            try {
                my $TRIG =
                    "$host_info->{datadir}/sandboxes/$host/my sql -h"
                  . $host_info->{ip} . " -P "
                  . $host_info->{mysql_port} . " -u"
                  . $host_info->{mysql_user}
                  . "  -p$host_info->{mysql_password} -e\""
                  . $query . "\"";
                worker_node_command( $TRIG, $host_info->{ip} );
                print STDERR "dsn: "
                  . $dsn . " : "
                  . $host_info->{mysql_user} . " "
                  . $host_info->{mysql_password} . " : "
                  . $query . "\n";

# my $dbh = DBI->connect($dsn, $host_info->{mysql_user}, $host_info->{mysql_password});
# my $sth = $dbh->do($query);
# $sth->execute();
# $sth->finish();
# $dbh->disconnect();

            }
            catch Error with {
                print STDERR "ici7\n";
                $err = "ER0003";
            }

        }
    }
    return $err;
}




sub get_host_vip() {
    my $host_info;
    foreach my $vip ( keys( %{ $config->{lb} } ) ) {
        $host_info = $config->{lb}->{default};
        $host_info = $config->{lb}->{$vip};
        if ( $host_info->{mode} eq "keepalived" ) {
            return $host_info;
        }
    }
   return 0; 
}


sub get_master_cloud() {
    my $host_info;
    foreach my $host ( keys( %{ $config->{cloud} } ) ) {
        $host_info = $config->{cloud}->{default};
        $host_info = $config->{cloud}->{$host};
        if ( $host_info->{status} eq "master" ) {
            return $host_info;
        }
    }
   return 0; 
}

sub get_master_host() {
    my $host_info;
    foreach my $host ( keys( %{ $config->{db} } ) ) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        if ( $host_info->{mode} eq "master" ) {
            return $host_info;
        }
    }
   return 0; 
}
sub get_master_host_hash() {
    my $host_info;
    foreach my $host ( keys( %{ $config->{db} } ) ) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        if ( $host_info->{mode} eq "master" ) {
            return $host;
        }
    }
   return 0; 
}

sub get_memcache_director() {
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


sub start_bench($$$$) {
    my $self = shift;
    my $node = shift;
    my $type = shift;
    my $bench_info;
    my $host_info;
    my $err = "000000";
    foreach my $bench ( keys( %{ $config->{lb} } ) ) {

#  @abs_top_srcdir@/bin/client -u skysql -h 192.168.0.10 -a skyvodka -f -c 10 -s 10 -d dbt2 -l 3306  -o @abs_top_srcdir@/scripts/output/10/client
#   @abs_top_srcdir@/src/driver -d localhost -l 100 -wmin 1 -wmax 10 -w 10 -sleep 10 -outdir @abs_top_srcdir@/scripts/output/10/driver -tpw 10 -ktd 0 -ktn 0 -kto 0 -ktp 0 -kts 0 -ttd 0 -ttn 0 -tto 0 -ttp 0 -tts 0
        $host_info = $config->{lb}->{default};
        $host_info = $config->{lb}->{$bench};
        if ( $host_info->{mode} eq "haproxy" ) {
            $bench_info = $host_info;
        }
    }

# my $cmd=$SKYBASEDIR."/dbt2/bin/client  -c ".$self->{concurrency}." -d ".$self->{duration}." -n -w ".$self->{warehouse}." -s 10 -u ".$bench_info->{mysql_user}." -x ".$bench_info->{mysql_password} ." -H". $bench_info->{vip};
    my $cmd =
        "/bin/client -u "
      . $bench_info->{mysql_user} . " -h "
      . $bench_info->{vip} . " -a "
      . $bench_info->{mysql_user}
      . " -f -c 10 -s 10 -d dbt2 -l "
      . $bench_info->{port} . "  -o";
    $err = worker_node_command( $cmd, $self->{ip} );

}

sub mycheckpoint_create_master_db($$) {
  my $host_info=shift;
  my $db_name =shift;
    my $sql = "CREATE DATABASE if not exists ".$db_name ;
        my $dsn =
            "DBI:mysql:host="
          . $host_info->{ip}
          . ";port="
          . $host_info->{mysql_port}
          . ";mysql_connect_timeout="
          . $mysql_connect_timeout;

        my $dbh = DBI->connect(
            $dsn,
            $host_info->{mysql_user},
            $host_info->{mysql_password},
            {RaiseError=>1}
        );
        try {
        
            my $sth = $dbh->do($sql);
           
            
        }
        catch Error with {
            print STDERR "Mon connect Master failed\n";
            return 0;
        };
        $dbh->disconnect;
        return 1;  
}

sub mycheckpoint_start($) {
  my $self = shift;
  my $host=get_master_host_hash();
  my $host_info = $config->{db}->{$host};  
  my $err = "000000";
  
  my $cmd =
            $SKYBASEDIR
          . "/mycheckpoint/bin/mycheckpoint http "
          . " --user="
          . $host_info->{mysql_user}
          . " --password="
          . $host_info->{mysql_password}
          . " --host="
          . $host_info->{ip}
          . " --port="
          . $host_info->{mysql_port}
          . " --database=mon_"
          . $host
          . " --http-port=80 &";
   $err = worker_node_command( $cmd , $self->{ip} );
  
     return $err;

}

sub mycheckpoint_cmd($$$) {
  my $host_vip=shift;
  my $host_info=shift;
  my $host=shift;
   my $err; 
  my $cmd =
            $SKYBASEDIR
          . "/mycheckpoint/bin/mycheckpoint -o  --database=mon_"
          . $host
          . " --monitored-user="
          . $host_info->{mysql_user}
          . " --monitored-password="
          . $host_info->{mysql_password}
          . " --monitored-host="
          . $host_info->{ip}
          . " --monitored-port="
          . $host_info->{mysql_port}
          . " --user="
          . $host_vip->{mysql_user}
          . " --password="
          . $host_vip->{mysql_password}
          . " --host="
          . $host_vip->{ip}
          . " --port="
          . $host_vip->{mysql_port};

        $err = worker_node_command( $cmd, $host_info->{ip} );
        if(  $err eq "00000" ){
            return 1;
        }    
        else {
         return 0;
        } 
}

sub check_memcache_from_db($) {
     
      my $host_info=shift;
      my  $dsn2 =
            "DBI:mysql:host="
          . $host_info->{ip}
          . ";port="
          . $host_info->{mysql_port}
          . ";mysql_connect_timeout="
          . $mysql_connect_timeout;
        
       my  $dbh2 = DBI->connect(
            $dsn2,
            $host_info->{mysql_user},
            $host_info->{mysql_password},
           {RaiseError=>0,PrintError=>1}
        );
        if (!$dbh2)  { 
          print STDERR "Error Memcqche UDF DB connect failed \n";
          return 0;
        }
         my $sql="SELECT memc_set('test','test',0)";
        my $res = 0;
       
        my $sth2 = $dbh2->do($sql);
        if (!$sth2)  {    
             print STDERR "Error Mon connect memc_set \n";
             return 0;
        }
        $sql="SELECT memc_get('test') as c1";
          
        if ( my $stm = $dbh2->prepare($sql)){
                $stm->execute; 
                my($result) = $stm->fetchrow_array();
                $result= defined ($result) ? $result : "no";
                 
                if  ( $result eq "test" )   {
                   $res=1;
                } 
                $stm->finish(); 
           }  
         else {
             return 0;
             print STDERR "Error Mon connect memc_GET\n";
          }
           
          
        if ($res == 0 ) {
             print STDERR "Setting UDF Memcache server\n";
            my $mem_info=get_memcache_director();
            $sql="SELECT memc_servers_set('". $mem_info->{ip} .":". $mem_info->{port}."')";
            print STDERR "'". $mem_info->{ip} .":". $mem_info->{port}."'";
            try {

                my $sth = $dbh2->do($sql);
               

            }
            catch Error with {
                 print STDERR "Mon connect memc_servers_set\n";
                return 0;

            };  
        }
        $dbh2->disconnect;
        return 1;
}




sub ping_election($) {
    my $json_status = shift ;
    my $json    = new JSON;
    my $status =
      $json->allow_nonref->utf8->relaxed->escape_slash->loose
      ->allow_singlequote->allow_barekey->decode($json_status);
   
    my $err = "000000";
    my $host_info;
    my $host_vip = get_master_host();
    my $command;
    foreach my $ip ( keys( %{ $status->{host}->{interfaces}->{IP} }) ) {
        print $ip; 
    }
    foreach my $host ( keys( %{ $config->{db} } ) ) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        print STDERR "connect to db: " . $host . "\n";
        
        # Check memcache_udf
        check_memcache_from_db($host_info);
        mycheckpoint_create_master_db( $host_vip , "mon_" . $host);
        mycheckpoint_cmd($host_vip,$host_info,$host);
        
    }
    return $err;

}

sub mysql_rolling_restart(){ 
 my $host_info;
 my $host_slave;
 foreach my $host ( keys( %{ $config->{db} } ) ) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        if  ( $host_info->{mode} eq "slave" ){
                $ret = node_cmd( $host_info, $host, "stop" );
                $host_slave=$host_info;
        }         
       
  }


 foreach my $host ( keys( %{ $config->{db} } ) ) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        if  ( $host_info->{mode} eq "master" ){
           mha_master_switch($host_slave); 
           $ret = node_cmd( $host_info, $host, "stop" );
       }         
       
  }
 
}


sub install_bench() {

# /usr/local/skysql/dbt2/scripts/mysql/build_db.sh -w 3 -d dbt2 -f /var/lib/skysql/dbt2/  -h 192.168.0.100 -u skysql  -pskyvodka -e INNODB
}

sub install_sandbox($$$) {
    my $self = shift;
    my $node = shift;
    my $type = shift;
    my $err  = "000000";
    chop( my $my_arch = `arch` );
    if ( $type eq 'router' ) {

        my $param = "$SKYBASEDIR/ncc/init.d/routerd start";
        $err = worker_node_command( $param, $self->{ip} );
        return $err;
    }
    my $SANDBOX =
        "low_level_make_sandbox --force --no_show" . "  "
      . " -c server-id=$self->{mysql_port} "
      . " --db_user=$self->{mysql_user} "
      . " --db_password=$self->{mysql_password} "
      . " -d $node "
      . " --sandbox_port=$self->{mysql_port} "
      . " --upper_directory=$self->{datadir}/sandboxes "
      . " --install_version=5.5 --no_ver_after_name "
      . " --basedir=$SKYBASEDIR/$self->{mysql_version} "
      . " --my_file=$self->{mysql_cnf}";

    my $le_localtime = localtime;
    print $LOG $le_localtime . " $SANDBOX\n";
    print STDOUT $le_localtime . " $SANDBOX\n";
    $err = worker_node_command( $SANDBOX, $self->{ip} );
    my $GRANT =
"$self->{datadir}/sandboxes/$node/my sql -uroot -p$self->{mysql_password} -e\""
      . "GRANT replication slave ON *.* to \'$self->{replication_user}\'@\'%\' IDENTIFIED BY \'$self->{replication_password}\';"
      . "GRANT ALL ON *.* to \'$self->{mysql_user}\'@\'%\' IDENTIFIED BY \'$self->{mysql_password}\';"
      . "\"";
    worker_node_command( $GRANT, $self->{ip} );

    if ( $type eq 'spider' || $type eq 'monitor' ) {
        my $SPIDER =
"$self->{datadir}/sandboxes/$node/my sql -uroot -p$self->{mysql_password} < $SKYBASEDIR/ncc/scripts/install_spider.sql";
        $err = worker_node_command( $SPIDER, $self->{ip} );

    }
    else {
        my $HANDLERSOCKET =
"$self->{datadir}/sandboxes/$node/my sql -uroot -p$self->{mysql_password} -e\"INSTALL PLUGIN handlersocket SONAME \'handlersocket.so\';\"";
        $err = worker_node_command( $HANDLERSOCKET, $self->{ip} );
    }
    my $GEARMANUDF =
"$self->{datadir}/sandboxes/$node/my sql -uroot -p$self->{mysql_password} < $SKYBASEDIR/ncc/scripts/gearmanudf.sql";
    $err = worker_node_command( $GEARMANUDF, $self->{ip} );

    my $param =
"$self->{datadir}/sandboxes/$node/my sql -uroot -p$self->{mysql_password} -e\""
      . "SELECT gman_servers_set('127.0.0.1');\"";
    $err = worker_node_command( $param, $self->{ip} );

    my $MEMCACHEUDF =
"$self->{datadir}/sandboxes/$node/my sql -uroot -p$self->{mysql_password} < $SKYBASEDIR/ncc/scripts/install_memcacheudf.sql";
    $err = worker_node_command( $MEMCACHEUDF, $self->{ip} );

    my $GTTID =
"$self->{datadir}/sandboxes/$node/my sql -uroot -p$self->{mysql_password} < $SKYBASEDIR/ncc/scripts/gttid.sql";
    $err = worker_node_command( $GTTID, $self->{ip} );
    my $memcaches = list_memcaches();

    $param =
"$self->{datadir}/sandboxes/$node/my sql -uroot -p$self->{mysql_password} -e\""
      . "SELECT memc_servers_set('"
      . $memcaches . "');\"";
    $err = worker_node_command( $param, $self->{ip} );

    report_node( $self, "Install node", $err, $node );
    return $err;
}

sub node_cmd($$$) {
    my $self = shift;
    my $node = shift;
    my $cmd  = shift;
    chop( my $my_arch = `arch` );
    my $param = "";
    my $err   = "000000";
    if ( $cmd eq "remove" ) {
        $param = "$self->{datadir}/sandboxes/$node/stop";
        $err   = worker_node_command( $param, $self->{ip} );
        $param = "rm -rf $self->{datadir}/sandboxes/$node";
        $err   = worker_node_command( $param, $self->{ip} );
    }
    elsif ( $cmd eq "status" ) {

        if (   $self->{mode} eq "master"
            || $self->{mode} eq "slave"
            || $self->{mode} eq "spider"
            || $self->{mode} eq "actif"
            || $self->{mode} eq "mysql-proxy"
            || $self->{mode} eq "keepalived"
            || $self->{mode} eq "haproxy" )
        {
            my $port = "3306";
            if (   $self->{mode} eq "mysql-proxy"
                || $self->{mode} eq "keepalived"
                || $self->{mode} eq "haproxy" )
            {
                $port = $self->{port};
            }
            else {
                $port = $self->{mysql_port};
            }
            my $theip = $self->{ip};
            if ( $self->{mode} eq "keepalived" ) {
                $theip = $self->{vip};
            }
            my $dsn =
                "DBI:mysql:database=mysql;host="
              . $theip
              . ";port="
              . $port
              . ";mysql_connect_timeout="
              . $mysql_connect_timeout;
            print STDERR $dsn;
            use Error qw(:try);
            try {

                my $dbh =
                  DBI->connect( $dsn, $self->{mysql_user},
                    $self->{mysql_password} );
                $param = "select 1";
                $dbh->do($param);
            }
            catch Error with {
                $err = "ER0003";
            }
        }
        elsif ( $self->{mode} eq "memcache" ) {
            use Error qw(:try);
            print STDERR "connecting to memcache \n";
            try {

                my $memd = new Cache::Memcached {
                    'servers' => [ $self->{ip} . ":" . $self->{port} ],
                    'debug'   => 0,
                    'compress_threshold' => 10_000,
                };

                $memd->set( "key_test", "test" );
                my $val = $memd->get("key_test");
                $err = "ER0005";
                if ( $val eq "test" ) { $err = "000000"; }
            }
            catch Error with {
                $err = "ER0005";
            }

        }
    }
    elsif (
        $cmd eq "start"
        && (   $self->{mode} eq "master"
            || $self->{mode} eq "slave"
            || $self->{mode} eq "spider"
            || $self->{mode} eq "actif" )
      )
    {

        $param = "$self->{datadir}/sandboxes/$node/send_kill";
        $err = worker_node_command( $param, $self->{ip} );

        $param = "$self->{datadir}/sandboxes/$node/$cmd";
        $err = worker_node_command( $param, $self->{ip} );
        $param =
"$self->{datadir}/sandboxes/$node/my sql -uroot -p$self->{mysql_password} -e\""
          . "SELECT gman_servers_set('127.0.0.1');\"";
        $err = worker_node_command( $param, $self->{ip} );
        my $memcaches = list_memcaches();
        $param =
"$self->{datadir}/sandboxes/$node/my sql -uroot -p$self->{mysql_password} -e\""
          . "SELECT memc_servers_set('"
          . $memcaches . "');\"";

        $err = worker_node_command( $param, $self->{ip} );

    }
    elsif ( $cmd eq "start" && $self->{mode} eq "mysql-proxy" ) {
        $param =
            $SKYBASEDIR
          . "/mysql-proxy/bin/mysql-proxy --daemon --defaults-file=$SKYBASEDIR/ncc/etc/mysql-proxy."
          . $node
          . ".cnf --pid-file="
          . $SKYBASEDIR
          . "/ncc/tmp/mysql-proxy."
          . $node
          . ".pid --log-file="
          . $SKYDATADIR
          . "/log/mysql-proxy."
          . $node . ".log  1>&2 ";
        $err = worker_node_command( $param, $self->{ip} );
    }
    elsif ( $cmd eq "stop" && $self->{mode} eq "mysql-proxy" ) {
        $param =
            "kill -9 `cat "
          . $SKYBASEDIR
          . "/ncc/tmp/mysql-proxy."
          . $node . ".pid`";
        $err = worker_node_command( $param, $self->{ip} );
    }
    elsif ( $cmd eq "start" && $self->{mode} eq "keepalived" ) {
        $param =
            $SKYBASEDIR
          . "/keepalived/sbin/keepalived -f  "
          . $SKYBASEDIR
          . "/ncc/etc/keepalived."
          . $node . ".cnf";
        $err = worker_node_command( $param, $self->{ip} );
    }
    elsif ( $cmd eq "stop" && $self->{mode} eq "keepalived" ) {
        $param = "killall keepalived ";
        $err = worker_node_command( $param, $self->{ip} );
    }
    elsif ( $cmd eq "start" && $self->{mode} eq "haproxy" ) {
        $param =
            $SKYBASEDIR
          . "/haproxy/sbin/haproxy -f "
          . $SKYBASEDIR
          . "/ncc/etc/haproxy."
          . $node
          . ".cnf -p "
          . $SKYBASEDIR
          . "/ncc/tmp/haproxy."
          . $node . ".pid ";
        $err = worker_node_command( $param, $self->{ip} );
    }
    elsif ( $cmd eq "stop" && $self->{mode} eq "haproxy" ) {
        $param =
          "kill -9 `cat " . $SKYBASEDIR . "/ncc/tmp/haproxy." . $node . ".pid`";
        $err = worker_node_command( $param, $self->{ip} );
    }
    elsif ( $cmd eq "start" && $self->{mode} eq "memcache" ) {

# delete_pid_if_exists($SKYBASEDIR."/ncc/tmp/memcached.". $node .".pid",$self->{ip});
        $param =
            $SKYBASEDIR
          . "/ncc/init.d/start-memcached "
          . $SKYBASEDIR
          . "/ncc/etc/memcached."
          . $node . ".cnf";
        $err = worker_node_command( $param, $self->{ip} );
    }
    elsif ( $cmd eq "stop" && $self->{mode} eq "memcache" ) {
        $param =
            "kill -9 `cat "
          . $SKYBASEDIR
          . "/ncc/tmp/memcached."
          . $node . ".pid`";
        $err = worker_node_command( $param, $self->{ip} );
    }
    elsif ( $cmd eq "start" && $self->{mode} eq "mycheckpoint" ) {

        $err = mycheckpoint_start($self);
        
    }
    elsif ( $cmd eq "stop" && $self->{mode} eq "mycheckpoint" ) {
        $param =
            "kill -9 `cat "
          . $SKYBASEDIR
          . "/ncc/tmp/memcached."
          . $node . ".pid`";
        $err = worker_node_command( $param, $self->{ip} );
    }
    else {
        $param = "$self->{datadir}/sandboxes/$node/$cmd";
        $err = worker_node_command( $param, $self->{ip} );
    }
    print STDOUT $param;
    report_node( $self, $param, $err, $node );
    return $err;
}

sub delete_pid_if_exists($$) {
    my $pidfile = shift;
    my $ip      = shift;
    my $err     = "000000";

    if ( -e $pidfile ) {
        open PIDHANDLE, "$pidfile";
        my $localpid = <PIDHANDLE>;
        close PIDHANDLE;
        chomp $localpid;
        $err =
          worker_node_command( "tail -f /dev/null --pid " . $localpid, $ip );
    }
    return $err;
}


sub worker_node_command($$) {
    my $cmd    = shift;
    my $ip     = shift;
    my $client = Gearman::XS::Client->new();
    $client->add_servers($ip);
    print STDOUT $ip . ' ' . $cmd . '\n';
    #$client->set_timeout($gearman_timeout);
    #(my $ret,my $result) = $client->do_background('node_cmd', $cmd);
    ( my $ret, my $result ) = $client->do( 'node_cmd', $cmd );

    if ( $ret == GEARMAN_SUCCESS ) {
        if ( $result eq "true" ) {
            return "000000";
        }
        else { return "ER0003"; }

    }
    else { return "ER0002"; }
}

sub worker_cloud_command($$) {
    my $cmd    = shift;
    my $ip     = shift;
    my $client = Gearman::XS::Client->new();
    $client->add_servers($ip);
    print STDOUT $ip . ' ' . $cmd . '\n';
    #$client->set_timeout($gearman_timeout);
    #(my $ret,my $result) = $client->do_background('node_cmd', $cmd);
    ( my $ret, my $result ) = $client->do( 'cloud_cmd', $cmd );

    if ( $ret == GEARMAN_SUCCESS ) {
        if ( $result eq "true" ) {
            return "000000";
        }
        else { return "ER0003"; }

    }
    else { return "ER0002"; }

}

sub worker_config_command($$) {
    my $cmd    = shift;
    my $ip     = shift;
    my $client = Gearman::XS::Client->new();
    $client->add_servers($ip);

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

sub worker_cluster_command($$) {
    my $cmd    = shift;
    my $ip     = shift;
    my $client = Gearman::XS::Client->new();
    $client->add_servers($ip);

    #$client->set_timeout($gearman_timeout);
    ( my $ret, my $result ) = $client->do( 'cluster_cmd', $cmd );

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

sub RPad($$$) {

    my $str = shift;
    my $len = shift;
    my $chr = shift;
    $chr = " " unless ( defined($chr) );
    return substr( $str . ( $chr x $len ), 0, $len );
}    

sub report_node($$$) {
    my $self = shift;
    my $cmd  = shift;
    my $err  = shift;
    my $host = shift;
    print STDERR $cmd . '\n';
    my $le_localtime = localtime;
    print $LOG $le_localtime . " $cmd\n";
    push(@console ,
       '{"time":"'
      . $le_localtime
      . '","name":"'
      .  $host
      . '","ip":"'
      .  $self->{ip} 
      . '","mode":"'
      .  $self->{mode}  
      . '","error":"'
      . $err
      . '","state":"'
      . $ERRORMESSAGE{$err}  
      . '"}'
   );
   

}

sub node_sync($$$) {
    my $self   = shift;
    my $node   = shift;
    my $master = shift;
    chop( my $my_arch = `arch` );
    print STDERR "mysql -h$master->{ip} ";

#	my $param =   "mysql -h$self->{ip} -P$self->{mysql_port} -u $self->{mysql_user} -p$self->{mysql_password} -e \'stop slave;change master to master_host=\\\"$master->{ip}\\\", master_user=\\\"$master->{replication_user}\\\", master_password=\\\"$master->{replication_password}\\\", master_port=$master->{mysql_port};\';mysqldump -u $self->{mysql_user} -p$self->{mysql_password} -h $master->{ip} -P$master->{mysql_port} --single-transaction --master-data --all-databases | mysql -u $self->{mysql_user} -p$self->{mysql_password} -h$self->{ip} -P$self->{mysql_port};mysql -h$self->{ip} -P$self->{mysql_port} -u $self->{mysql_user} -p$self->{mysql_password} -e \'start slave;\' ";
    my $param =
"mysql -h$self->{ip} -P$self->{mysql_port} -u $self->{mysql_user} -p$self->{mysql_password} -e \'stop slave;change master to master_host=\"$master->{ip}\", master_user=\"$master->{replication_user}\", master_password=\"$master->{replication_password}\", master_port=$master->{mysql_port};\';mysqldump -u $self->{mysql_user} -p$self->{mysql_password} -h $master->{ip} -P$master->{mysql_port} --single-transaction --master-data --all-databases | mysql -u $self->{mysql_user} -p$self->{mysql_password} -h$self->{ip} -P$self->{mysql_port};mysql -h$self->{ip} -P$self->{mysql_port} -u $self->{mysql_user} -p$self->{mysql_password} -e \'start slave;\' ";

    worker_node_command( $param, $self->{ip} );
}

sub replace_config_line($$$) {
    my $file   = shift;
    my $strin  = shift;
    my $strout = shift;

    open my $in,  '<', $file       or die "Can't read old file: $!";
    open my $out, '>', "$file.new" or die "Can't write new file: $!";

    while (<$in>) {
        s/^$strin(.*)$/$strout/gi;
        print $out $_;
    }

    close $out;
    system("rm -f $file.old");
    system("mv $file $file.old");
    system("mv $file.new $file");
    system("chmod 660 $file");
}

sub list_slaves() {
    my $host_info;
    my $err = "000000";
    my @slaves;
    foreach my $host ( keys( %{ $config->{db} } ) ) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        if ( $host_info->{mode} eq "slave" ) {
            push( @slaves, $host_info->{ip} . ":" . $host_info->{mysql_port} );
        }
    }

    return join( ',', @slaves );
}

sub list_masters() {
    my $host_info;
    my $err = "000000";
    my @masters;
    foreach my $host ( keys( %{ $config->{db} } ) ) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        if ( $host_info->{mode} eq "master" ) {
            push( @masters, $host_info->{ip} . ":" . $host_info->{mysql_port} );
        }
    }
    return join( ',', @masters );
}

sub list_memcaches() {
    my $host_info;
    my $err = "000000";
    my @memcaches;
    foreach my $host ( keys( %{ $config->{db} } ) ) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        if ( $host_info->{mode} eq "memcache" ) {
            push( @memcaches,
                $host_info->{ip} . ":" . $host_info->{mysql_port} );
        }
    }
    return join( ',', @memcaches );
}

sub list_cluster_ips() {
    my $host_info;
    my $err = "000000";
    my @ips;
    foreach my $host ( keys( %{ $config->{db} } ) ) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        push( @ips, $host_info->{ip} );
    }
    return uniq(@ips);
}

sub mha_master_switch($) {
    my $switchhost = shift;
    my $host_info;
    my $err = "000000";

    my $file = "$SKYBASEDIR/ncc/etc/cloud.cnf";
    open my $in,  '<', $file       or die "Can't read old file: $!";
    open my $out, '>', "$file.new" or die "Can't write new file: $!";
    my $line = "";
    my $drap = 0;
    while ( $line = <$in> ) {

        # print STDERR $line . " ". $drap . "\n";
        if ( $line =~ /^\s*<\s*(\w+)\s+([\w\-_]+)\s*>\s*$/ ) {
            my $type = $1;
            my $name = $2;

            # print STDERR $name . $switchhost. "\n" ;
            if ( $name eq $switchhost ) {
                $drap = 1;
            }
            else {
                $drap = 0;
            }
        }

        if ( $drap == 1 ) {
            $line =~ s/^(.*)mode(.*)slave(.*)$/\tmode\t\t\t\tmaster/gi;

        }
        else {
            $line =~ s/^(.*)mode(.*)(slave|master)(.*)$/\tmode\t\t\t\tslave/gi;
        }
        print $out $line;
        next;
    }
    system("rm -f $file.old");
    system("mv $file $file.old");
    system("mv $file.new $file");
    system("chmod 660 $file");

   
    stop_all_proxy();
    bootstrap_config();
    foreach my $host ( keys( %{ $config->{db} } ) ) {
        $host_info = $config->{db}->{default};
        $host_info = $config->{db}->{$host};
        if ( $host eq $switchhost ) {
            my $cmd =
                $SKYBASEDIR
              . "/perl/bin/perl "
              . $SKYBASEDIR
              . "/mha4mysql/bin/masterha_master_switch --master_state=alive --orig_master_is_new_slave  --conf="
              . $SKYBASEDIR
              . "/ncc/etc/mha.cnf --interactive=0 --new_master_host="
              . $host_info->{ip}
              . " --new_master_port="
              . $host_info->{mysql_port};
            system($cmd);
        }
    }
    start_all_proxy();
    return $err;
}

sub bootstrap_config() {
   
    my $my_home_user = $ENV{HOME};
    my $cmd =
        'string="`cat '
      . $SKYBASEDIR
      . '/ncc/etc/id_rsa.pub`"; sed -e "\|$string|h; \${x;s|$string||;{g;t};a\\" -e "$string" -e "}" $HOME/.ssh/authorized_keys > $HOME/.ssh/authorized_keys2 ;mv $HOME/.ssh/authorized_keys $HOME/.ssh/authorized_keys_old;mv $HOME/.ssh/authorized_keys2 $HOME/.ssh/authorized_keys';
    my $err = "000000";

    my @ips = list_cluster_ips();
    foreach (@ips) {
        my $command =
          "{command:{action:'write_config',group:'localhost',type:'all'}}";
        system( "scp -i "
              . $SKYBASEDIR
              . $sshkey." "
              . $SKYBASEDIR
              . "/ncc/etc/cloud.cnf "
              . $_ . ":"
              . $SKYBASEDIR
              . "/ncc/etc" );
        $err = worker_config_command( $command, $_ );

    }

    #$cms="cat <<EOF_LO0 > /etc/sysconfig/network-scripts/ifcfg-lo:1
    #DEVICE=lo:1
    #IPADDR=192.168.0.10
    #NETMASK=255.255.255.255
    #NAME=loopback
    #ONBOOT=yes
    #EOF_LO0";
    return $err;
}

sub bootstrap_binaries() {
    my $my_home_user = $ENV{HOME};
    my $command =
        "tar cz -C "
      . $SKYBASEDIR
      . "/.. -f "
      . $SKYDATADIR
      . "/skystack.tar.gz  ./skysql";
    system($command);
    my $err = "000000";
    my @ips = list_cluster_ips();
    foreach (@ips) {
        if ( is_ip_localhost($_) == 1 ) {
            
            $command =
                "scp -q -i  "
              . $SKYBASEDIR
              . $sshkey ." "
              . $SKYDATADIR
              . "/skystack.tar.gz "
              . $_ . ":/tmp";
            system($command);
            my @dest= split('/', $SKYBASEDIR);
            pop(@dest);
              $command =
                "ssh -q -i  "
              . $SKYBASEDIR
              . $sshkey. " "
              . $_
              . " \"tar -xvz -f /tmp/skystack.tar.gz -C "
              . join('/',@dest)
              . " && "
              . $SKYBASEDIR
              . "/ncc/init.d/clusterd stop && "
              . $SKYBASEDIR
              . "/ncc/init.d/clusterd start  \"";
            system($command);
        }
    }
    return $err;
}

sub bootstrap_ncc() {
    my $command =
      "tar czv -C " . $SKYBASEDIR . " -f " . $SKYDATADIR . "/ncc.tar.gz  ./ncc";

    system($command);
    my $my_home_user = $ENV{HOME};
    my $err          = "000000";


    my @ips = list_cluster_ips();
    foreach (@ips) {
        if ( is_ip_localhost($_) == 1 ) {

            $command =
                "scp -q -i "
              . $SKYBASEDIR
              . $sshkey. " "
              . $SKYDATADIR
              . "/ncc.tar.gz "
              . $_ . ":/tmp";
            system($command);
            $command =
                "ssh -q -i "
              . $SKYBASEDIR
              . $sshkey. " "
              . $_
              . " \"tar -xzv -f /tmp/ncc.tar.gz -C "
              . $SKYBASEDIR . " && "
              . $SKYBASEDIR
              . "/ncc/init.d/clusterd stop && "
              . $SKYBASEDIR
              . "/ncc/init.d/clusterd start  \"";
            system($command);
        }
    }
    return $err;
}

sub stop_all_proxy() {

    my $host_info;
    my $err = "000000";
    foreach my $host ( keys( %{ $config->{proxy} } ) ) {
        $host_info = $config->{proxy}->{default};
        $host_info = $config->{proxy}->{$host};
        if ( $host_info->{mode} eq "mysql-proxy" ) {
            node_cmd( $host_info, $host, "stop" );
        }
    }

}

sub start_all_proxy() {

    my $host_info;
    my $err = "000000";
    foreach my $host ( keys( %{ $config->{proxy} } ) ) {
        $host_info = $config->{proxy}->{default};
        $host_info = $config->{proxy}->{$host};
        if ( $host_info->{mode} eq "mysql-proxy" ) {
            node_cmd( $host_info, $host, "start" );
        }
    }
}



sub uniq {
    my %seen;
    return grep { !$seen{$_}++ } @_;
}

sub tokenize_sql($) {
    my $query = shift;
    my $re    = qr{
		(
			(?:--|\#)[\ \t\S]*      # single line comments
			|
			(?:<>|<=>|>=|<=|==|=|!=|!|<<|>>|<|>|\|\||\||&&|&|-|\+|\*(?!/)|/(?!\*)|\%|~|\^|\?)
									# operators and tests
			|
			[\[\]\(\),;.]            # punctuation (parenthesis, comma)
			|
			\'\'(?!\')              # empty single quoted string
			|
			\"\"(?!\"")             # empty double quoted string
			|
			".*?(?:(?:""){1,}"|(?<!["\\])"(?!")|\\"{2})
									# anything inside double quotes, ungreedy
			|
			`.*?(?:(?:``){1,}`|(?<![`\\])`(?!`)|\\`{2})
									# anything inside backticks quotes, ungreedy
			|
			'.*?(?:(?:''){1,}'|(?<!['\\])'(?!')|\\'{2})
									# anything inside single quotes, ungreedy.
			|
			/\*[\ \t\n\S]*?\*/      # C style comments
			|
			(?:[\w:@]+(?:\.(?:\w+|\*)?)*)
									# words, standard named placeholders, db.table.*, db.*
			|
			\n                      # newline
			|
			[\t\ ]+                 # any kind of white spaces
		)
	   }smx;
    my @ltokens = $query =~ m{$re}smxg;

    @ltokens = grep( !/^[\s\n\r]*$/, @ltokens );

    return wantarray ? @ltokens : \@ltokens;
}
