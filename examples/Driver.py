from twisted.internet import reactor
from twisted.python import log
from kademLAN.network import Server
import pickle
import sys
import os
import random
import time
import math
import threading
import psutil
from optparse import OptionParser


KEY = 0
PROB = 1
TIME = 2
DEL_T = 30
START_T = time.time()
TIME_DICT = {}
BYTES_DICT = {}
SUCC_COUNT = 0


def done(result, key, start_time):
    global SUCC_COUNT
    latency = time.time() - start_time
    print("GET_RESULT Key:{} result:{} Time:{}".format(key, result, latency))
    SUCC_COUNT += 1
    sent = psutil.net_io_counters(pernic=True)['eth0'].bytes_sent
    recv = psutil.net_io_counters(pernic=True)['eth0'].bytes_recv
    print("TOTAL_BYTES Sent:{} Recv:{}".format(sent - startSent, recv - startRecv))
    TIME_DICT[key] = {"result": result, "latency": latency}
    BYTES_DICT["sent"] = sent - startSent
    BYTES_DICT["recv"] = recv - startRecv


def bootstrapDone(found, server, results, stab_t, runtime):
    print("Start Time:{}".format(START_T))
    reactor.callInThread(driveFromFile, options.path, server, results, stab_t, runtime)
    print("Called the driver in a separate thread")


def parseDriveFile(driveFile):
    lines = driveFile.readlines()
    drivelist = []

    for line in lines:
        line.strip('\n')
        if len(line) > 0:
            attr = line.split(",")
            # Should this node participate or not
            if random.random() <= float(attr[PROB]):
                # Randomize the get event time around the given timestamp
                arr = [attr[KEY], float(attr[TIME])]
                drivelist.append(arr)

    return drivelist


"""
wILL BE RUN ON A SEPARATE THREAD
"""


def driveFromFile(drivePath, server, results, stab_t, runtime):
    print("File path: {}".format(drivePath))
    driveFile = open(drivePath)
    driveList = parseDriveFile(driveFile)
    print("DriveList: {}".format(driveList))

    print("Waiting for {}seconds for stabilization".format(stab_t))
    print("Total Runtime: {}".format(runtime))
    # Some stabilization time
    time.sleep(stab_t)

    for event in driveList:
        key = event[0]
        server.set(key, key)
        time.sleep(0.2)
        print("Issued PUT at:{} for Key:{}".format(time.time(), key))

    START_T = time.time()
    # for each event in list, do the action at the appropriate time
    for event in driveList:
        key = event[0]
        get_t = event[1]
        # wait till the correct time
        while (time.time() - START_T) < get_t:
            time.sleep(0.1)
            pass
        start_time = time.time()
        server.get(key).addCallback(done, key, start_time)
        print("Issued get at:{} for Key:{}".format(start_time, key))

    # finished experiment. Wait for last result just in case.
    print("finished experiment. Wait for last result just in case.")
    while (time.time() - START_T) < (runtime + 10):
        time.sleep(0.1)
    print("finished experiment. Stopped Reactor. Saving Results")

    print("Number of Gets issued:{}".format(len(driveList)))
    print("Number of Gets successful:{}".format(SUCC_COUNT))
    #Save the logged data to a file
    resultFile = open(results, 'w+b')
    res_dict = {}
    res_dict["bytes"] = BYTES_DICT
    res_dict["latency"] = TIME_DICT
    res_dict["success"] = SUCC_COUNT
    res_dict["issued"] = len(driveList)
    pickle.dump(res_dict, resultFile, protocol=2)
    server.stop()
    reactor.callFromThread(reactor.stop)
    return


startSent = psutil.net_io_counters(pernic=True)['eth0'].bytes_sent
startRecv = psutil.net_io_counters(pernic=True)['eth0'].bytes_recv
parser = OptionParser()

parser.add_option("-f", "--file", type="str", dest="path", help="Path of Driver file")
# parser.add_option("-p", "--port", type="int", dest="port", help="Listening Port")
#parser.add_option("-B", "--bootstrap", type="str", dest="bootip", help="IP Address of Bootstrap server")
#parser.add_option("-P", "--BPort", type="int", dest="bootport", help="Port of Bootstrap server")
#parser.add_option("-i", "--isBootstrap", action="store_true", dest="isboot", help="Is this the bootstrap server?",
#                  default=False)
parser.add_option("-l", "--log", type="str", dest="log", help="Logfile", default=None)
parser.add_option("-r", "--resultFile", type="str", dest="result", help="resultFile", default="result")
parser.add_option("-s", "--stabT", type="int", dest="stab_t", help="Stabilization time", default=30)
parser.add_option("-t", "--time", type="int", dest="runTime", help="Run time", default=30)
(options, args) = parser.parse_args()

if options.log == None:
    log.startLogging(sys.stdout)
else:
    logfile = open(options.log, 'w')
    log.startLogging(logfile)

server = Server(random.randint(32768, 61000), ksize=2, alpha=1)
server.listen(bootstrapDone, None, server, options.result, options.stab_t, options.runTime)
# if options.isboot is True:
#     bootstrapDone(None, server, options.result, options.stab_t, options.runTime)
# else:
#     # Wait for sometime before trying to bootstrap
#     time.sleep(5)
#     server.bootstrap([(options.bootip, options.bootport)]).addCallback(bootstrapDone, server, options.result,
#                                                                        options.stab_t, options.runTime)

reactor.run()
