

socket = require("socket")
require("Memcached")
cache = Memcached.Connect("localhost")
print(socket._VERSION)
