#!/usr/bin/python
from gearman import GearmanWorker
import json
from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver
from boto.ec2.connection import EC2Connection

import libcloud.security
import logging
import os
logging.basicConfig(filename=os.getenv('SKYDATADIR', '/var/lib/skysql') + "/log/boto.log", level=logging.DEBUG)

# The function that will do the work
def add_vcloud_node(worker, job):
   print job.data
   vcloud = get_driver(Provider.VCLOUD)
   driver = vcloud('skysql@David_CHANIAL_1001895', '!davixx#',
               host='vcloud.hegerys.com', api_version='1.5')
   nodes=driver.list_nodes()
   print nodes
   return job.data

def cloud_cmd(worker, job): 
   print job.data
   config=json.loads(job.data) 
   print config
   res ='' 
   print config['command'] 
   
   print config['command']['action'] 

   if config["command"]["action"] == "launch": 
       print "launch instance .."
       res=launching_ec2_instances(config) 
 
   if config["command"]["action"] == "status": 
       print "status instances .."  
       res=status_ec2_instances(config) 

   if config["command"]["action"] == "start": 
       print "start instance .."  + config["command"]["group"]
       res=start_ec2_instances(config)

   if config["command"]["action"] == "stop": 
       print "stop instance .."  + config["command"]["group"]
       res=stop_ec2_instances(config) 

   if config["command"]["action"] == "terminate": 
       print "terminate instance .."  + config["command"]["group"] 
       res=terminate_ec2_adresse(config) 
    
   if config["command"]["action"] == "associate": 
       print "associate elastic & vip .."  + config["command"]["group"]  
       res=associate_ec2_adresse(config) 

   if config["command"]["action"] == "status_vip": 
       print "status vip .."  + config["command"]["group"] 
       res=status_ec2_vip_interface(config)
   
   if config["command"]["action"] == "associate_vip": 
       print "associate vip only .."  + config["command"]["group"]  
       res=associate_ec2_vip(config)  
       
   if config["command"]["action"] == "disassociate": 
       print "disassociate elastic ip & vip .."  + config["command"]["group"]  
       res=disassociate_ec2_adresse(config)  
  
   if config["command"]["action"] == "status_eip": 
       print "status elastic ip .."  + config["command"]["group"]   
       res= status_ec2_eip(config)   
   if config["command"]["action"] == "attach_vip": 
       res=associate_ec2_vip(config)

   if res ==0 :
       res ="000000"   
   if res ==1 :
       res ="ERR0050"   
   return res

def stop_ec2_instances(config):
   import boto
   conn = boto.connect_ec2(aws_access_key_id=config["cloud"]["user"],aws_secret_access_key=config["cloud"]["password"],debug=1) 
   res=conn.stop_instances(instance_ids=[config["command"]["group"]]) 
   for i in res:
      d.append({'id' : i.id , 'ip' : i.private_ip_address, 'state' : i.state}) 
   return json.dumps(d)


def start_ec2_instances(config):
   import boto
   conn = boto.connect_ec2(aws_access_key_id=config["cloud"]["user"],aws_secret_access_key=config["cloud"]["password"],debug=1)   
   res=conn.start_instances(instance_ids=[config["command"]["group"]]) 
   d=[] 
   for i in res:
      d.append({'id' : i.id , 'ip' : i.private_ip_address, 'state' : i.state}) 
   return json.dumps(d)


def terminate_ec2_instances(config):
   import boto
   conn = boto.connect_ec2(aws_access_key_id=config["cloud"]["user"],aws_secret_access_key=config["cloud"]["password"],debug=1)   
   res=conn.terminate_instances(instance_ids=[config["command"]["group"]])  
   return json.dumps(res)


def status_ec2_instances(config):
    import boto
    #boto.config.set('Boto','http_socket_timeout','20')  
    import simplejson as json 
    conn = boto.connect_ec2(aws_access_key_id=config["cloud"]["user"],aws_secret_access_key=config["cloud"]["password"],debug=1)    
    print "ici3"
    reservations=conn.get_all_instances()
    #adresses=get_all_network_interfaces(allocation_id=[config["cloud"]["elastic_ip_id"]])
    #adresses=get_all_network_interfaces()
    import pprint 
    
    d= [] 
    #pp.pprint(adresses)
    for reservation in reservations:
       for i in reservation.instances:
          d.append({i.id : {'id' : i.id , 'ip' : i.private_ip_address, 'state' : i.state, 'interface' : i.networkInterfaceId, 'reservation' : reservation.id }})   
    return  json.dumps(d)

def associate_ec2_adresse(config):
    import boto
    conn =boto.connect_ec2(aws_access_key_id=config["cloud"]["user"],aws_secret_access_key=config["cloud"]["password"],debug=1)       
    conn.associate_address(allocation_id=config["cloud"]["elastic_ip_id"], network_interface_id=config["instance"]["interface"], allow_reassociation=True)
    conn.attach_network_interface(  network_interface_id=config["cloud"]["interface_vip_id"],instance_id=config["instance"]["id"] , device_index=1 )    
    
    return 1

def associate_ec2_vip(config):  
    import boto
    conn =boto.connect_ec2(aws_access_key_id=config["cloud"]["user"],aws_secret_access_key=config["cloud"]["password"],debug=1)    
    print config["command"]["group"] 
    conn.attach_network_interface(  network_interface_id=config["cloud"]["interface_vip_id"],instance_id=config["command"]["group"]  , device_index=1 )    
    return 1

def disassociate_ec2_adresse(config):
    import boto
    
    conn =boto.connect_ec2(aws_access_key_id=config["cloud"]["user"],aws_secret_access_key=config["cloud"]["password"],debug=1)       
    #conn.disassociate_address( allocation_id=config["cloud"]["elastic_ip_id"])
    #conn.release_address(allocation_id=config["cloud"]["elastic_ip_id"])
    filters = {'private_ip_address': '10.0.0.10'} 
    addresses =conn.get_all_network_interfaces(filters=filters)
    from pprint import pprint
    for i in addresses:
        print i.attachment.id
        conn.detach_network_interface( i.attachment.id )    
        print "icitest"
    
    return 1

def status_ec2_eip(config):
    import boto
    d=[]
    conn =boto.connect_ec2(aws_access_key_id=config["cloud"]["user"],aws_secret_access_key=config["cloud"]["password"],debug=1)       
    eips=conn.get_all_addresses(addresses=[config["cloud"]["elastic_ip"]])
    for i in eips:
        print i.public_ip
        print i.instance_id  
        d.append({i.public_ip : { 'instance_id' : i.instance_id, 'association_id' : i.association_id }})
    return json.dumps(d )
   
def status_ec2_vip_interface(config):
    import boto
    conn =boto.connect_ec2(aws_access_key_id=config["cloud"]["user"],aws_secret_access_key=config["cloud"]["password"],debug=1)       
    addresses =conn.get_all_network_interfaces()
    d=[]
    for i in addresses:
        print i.id
        print i.status
        attachment_id="na"
        try:
           i.attachment
        except NameError:
           i.attachment=None 
        if i.attachment is None:  
           attachment_id="na"
           instance_id="na"
        else:   
           attachment_id=i.attachment.id   
           instance_id=i.attachment.instance_id
           
        print attachment_id
        print instance_id
        print i.private_ip_address
        d.append({i.id : {'id' : i.id , 'status' : i.status, 'attachment_id' : attachment_id  , 'instance_id' : instance_id, 'ip' : i.private_ip_address }})

        return  json.dumps(d)

         
    return 0    

def random_md5like_hash():
    available_chars= string.hexdigits[:16]
    return ''.join(random.choice(available_chars) for dummy in xrange(32))

def launching_ec2_instances(config):
   import boto
   conn =boto.connect_ec2(aws_access_key_id=config["cloud"]["user"],aws_secret_access_key=config["cloud"]["password"],debug=1)    
   
   reservation = conn.run_instances( config["cloud"]["template"] ,
        key_name=config["cloud"]["key"],
        subnet_id=config["cloud"]["subnet"],
        instance_type=config["cloud"]["instance_type"],
        security_group_ids=[ config["cloud"]["security_groups"]], 
        private_ip_address=config["command"]["ip"],
        placement=config["cloud"]["zone"])
   i = reservation.instances[0] 
   status = i.update()
   while status == 'pending':
      time.sleep(10)
      print('waiting 10s... ')
      status = i.update()
   if status == 'running':
      print('running adding tag... ')
      import hashlib
      conn.create_tags([i.id], {"name": "ScrambleDB" +random_md5like_hash()})
      # i.add_tag("Name","{{ScambleDB}}")
      
   else:
      print('Instance status: ' + status)
    
   #     security_groups=[ config["cloud"]["security_groups"]])
   
   return json.dumps(reservation)    

# Establish a connection with the job server on localhost--like the client,
# multiple job servers can be used.
worker = GearmanWorker(['127.0.0.1:4731'])

# register_task will tell the job server that this worker handles the "echo"
# task
worker.register_task('cloud_cmd', cloud_cmd)

# Once setup is complete, begin working by consuming any tasks available
# from the job server
print 'working...'
worker.work()


