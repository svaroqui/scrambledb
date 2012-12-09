#!/usr/bin/python
from gearman import GearmanWorker
from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver

import libcloud.security


# The function that will do the work
def addnode(worker, job):
   print job.data
   vcloud = get_driver(Provider.VCLOUD)
   driver = vcloud('skysql@David_CHANIAL_1001895', '!davixx#',
               host='vcloud.hegerys.com', api_version='1.5')
   nodes=driver.list_nodes()
   print nodes
   return job.data




# Establish a connection with the job server on localhost--like the client,
# multiple job servers can be used.
worker = GearmanWorker(['127.0.0.1'])

# register_task will tell the job server that this worker handles the "echo"
# task
worker.register_task('addnode', addnode)

# Once setup is complete, begin working by consuming any tasks available
# from the job server
print 'working...'
worker.work()


