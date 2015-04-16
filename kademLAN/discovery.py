import sys
from threading import Thread
import zmq
import uuid
import logging
import struct
import socket
import time
from pyre.zactor import ZActor
from pyre.zbeacon import ZBeacon


BEACON_VERSION = 1
ZRE_DISCOVERY_PORT = 5670
REAP_INTERVAL = 10.0  # Once per second

logger = logging.getLogger("Discover")
logger.propagate = False
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler(sys.stdout))

class Discover(object):
    def __init__(self, port=8080, *args, **kwargs):
        self._ctx = zmq.Context()
        self._terminated = False  # API shut us down
        self._verbose = False  # Log all traffic (logging module?)
        self.beacon_port = ZRE_DISCOVERY_PORT  # Beacon port number
        self.interval = 0  # Beacon interval 0=default
        self.beacon = None  # Beacon actor
        self.beacon_socket = None  # Beacon socket for polling
        self.poller = zmq.Poller()  # Socket poller
        self.identity = uuid.uuid4()  # Our UUID as object
        self.bound = False
        self.name = str(self.identity)[:6]  # Our public name (default=first 6 uuid chars)
        self.endpoint = ""  # Our public endpoint
        self.port = port  # Our inbox port, if any
        self.status = 0  # Our own change counter
        self.peers = {}  # Hash of known peers, fast lookup
        self.headers = {}  # Our header values
        # TODO: gossip stuff
        # self.start()
        self.run_thread = Thread(target=self.run)
        self.run_thread.start()

        # def __del__(self):
        # destroy beacon

    def start(self):
        # TODO: If application didn't bind explicitly, we grab an ephemeral port
        # on all available network interfaces. This is orthogonal to
        # beaconing, since we can connect to other peers and they will
        # gossip our endpoint to others.
        if self.beacon_port:
            # Start beacon discovery
            self.beacon = ZActor(self._ctx, ZBeacon)

            if self._verbose:
                self.beacon.send_unicode("VERBOSE")


            # Our hostname is provided by zbeacon
            self.beacon.send_unicode("CONFIGURE", zmq.SNDMORE)
            self.beacon.send(struct.pack("I", self.beacon_port))
            hostname = self.beacon.recv_unicode()

            self.endpoint = "tcp://%s:%d" % (hostname, self.port)

            # Set broadcast/listen beacon
            transmit = struct.pack('cccb16sH', b'Z', b'R', b'E',
                                   BEACON_VERSION, self.identity.bytes,
                                   socket.htons(self.port))
            self.beacon.send_unicode("PUBLISH", zmq.SNDMORE)
            self.beacon.send(transmit)
            # construct the header filter  (to discard none zre messages)
            filter = struct.pack("ccc", b'Z', b'R', b'E')
            self.beacon.send_unicode("SUBSCRIBE", zmq.SNDMORE)
            self.beacon.send(filter)

            self.beacon_socket = self.beacon.resolve()
            self.poller.register(self.beacon_socket, zmq.POLLIN)

    def stop(self):
        logger.debug("Pyre node: stopping beacon")
        if self.beacon:
            stop_transmit = struct.pack('cccb16sH', b'Z', b'R', b'E',
                                        BEACON_VERSION, self.identity.bytes,
                                        socket.htons(0))
            self.beacon.send_unicode("PUBLISH", zmq.SNDMORE)
            self.beacon.send(stop_transmit)
            # Give time for beacon to go out
            time.sleep(0.001)
            self._terminated = True
            # Give time for thread to exit
            time.sleep(5)
            self.poller.unregister(self.beacon_socket)
            self.beacon.destroy()
            self.beacon = None
            self.beacon_socket = None

        self.beacon_port = 0

    # Find or create peer via its UUID string
    def require_peer(self, identity, endpoint):
        """

        :param identity:
        :param endpoint:
        :return: endpoint of peer
        """
        p = self.peers.get(identity)
        if not p:
            self.peers[identity] = endpoint
        return p


    def get_peers(self):
        return list(self.peers.values())


    #  Remove a peer from our data structures
    def remove_peer(self, peer):
        # To destroy peer, we remove from peers hash table (dict)
        self.peers.pop(peer)

    def recv_beacon(self):
        # Get IP address and beacon of peer
        msgs = self.beacon_socket.recv_multipart()
        ipaddress = msgs.pop(0)
        frame = msgs.pop(0)

        beacon = struct.unpack('cccb16sH', frame)
        # Ignore anything that isn't a valid beacon
        if beacon[3] != BEACON_VERSION:
            logger.warning("Invalid ZRE Beacon version: {0}".format(beacon[3]))
            return

        peer_id = uuid.UUID(bytes=beacon[4])
        port = socket.ntohs(beacon[5])
        # if we receive a beacon with port 0 this means the peer exited
        if port:
            peer = self.require_peer(peer_id, (str(ipaddress.decode('UTF-8')), port))
        else:
            # Zero port means peer is going away; remove it if
            # we had any knowledge of it already
            peer = self.peers.get(peer_id)
            # remove the peer (delete)
            if peer:
                logger.debug("Received 0 port beacon, removing peer {0}".format(peer))
                self.remove_peer(peer_id)

            else:
                logger.warning(self.peers)
                logger.warning("We don't know peer id {0}".format(peer_id))

    # TODO: Handle gossip dat

    # --------------------------------------------------------------------------
    # This is the actor that runs a single node; it uses one thread, creates
    # a zyre_node object at start and destroys that when finishing.
    def run(self):
        while not self._terminated:
            items = dict(self.poller.poll(1000))
            if self.beacon_socket in items and items[self.beacon_socket] == zmq.POLLIN:
                self.recv_beacon()


if __name__ == "__main__":
    dis = Discover()
    logger.info("Starting Beacon")
    dis.start()

    try:
        while True:
            logger.info("Peers:{}".format(dis.get_peers()))
            time.sleep(1)
    except KeyboardInterrupt:
        dis.stop()