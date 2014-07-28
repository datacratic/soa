###
# Marc Cardinal, 26 October 2012
# Copyright (c) 2012 Datacratic. All rights reserved.
###

import socket
import datetime
import json

gConfig = {
    'APP': {
        'longname': "Zabbix Sender",
        'shortname': "ZbxSndr",
        'localtz': "America/Montrea",
        'zabbix': {
            'tz': "UTC",
            'host': "zabbix.recoset.com",
            'rpcport': 10051,
            },
        },
    }

def zabSend(config, host, key, value, timestamp):
    request = json.dumps({  'request': "sender data",
                            'data':[
                                    {
                                    'host': host,
                                    'key': key,
                                    'value': str(value),
                                    'clock': timestamp },
                                   ],
                            'clock': datetime.datetime.utcnow().strftime("%s")
                            })
    return tcpClient(config['host'], config['rpcport'], request)

def tcpClient(host, port, message):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    sock.send(message)
    reply = sock.recv(16384) # Set a limit at 16k
    sock.close()
    return reply


def send(host, key, value, timestamp=None):
    if host is None or key is None or value is None:
        raise ValueError("Missing one of the mandatory parameter: host, key, value.")

    if not timestamp:
        timestamp = datetime.datetime.utcnow().strftime("%s")

    result = zabSend(gConfig['APP']['zabbix'], host, key, value, timestamp)
    if not result.find("success"):
        print "ERROR: " + result
    #    raise Exception("ERROR: " + result)

