import random
from twisted.internet import reactor
from twisted.python import log
from kademLAN.network import Server
import sys

log.startLogging(sys.stdout)

def done(result):
    print("Key result:", result)
    server.stop()
    reactor.stop()

def setDone(result, server):
    server.get("a key").addCallback(done)

def bootstrapDone(server):
    print("Server after bootstrap:{}".format(server))
    server.set("a key", "a value").addCallback(setDone, server)

server = Server(random.randint(32768, 61000))
server.listen(bootstrapDone, server)
#server.bootstrap([("1.2.3.4", 8468)]).addCallback(bootstrapDone, server)

reactor.run()
