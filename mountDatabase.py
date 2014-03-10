#/usr/bin/env python

import sys
import pymongo

if len(sys.argv) == 8:
    gitemID, host, port, database, collection, girderHost, girderPort = sys.argv[1:]
elif len(sys.argv) == 6:
    girderHost = 'localhost'
    girderPort = 27017
    itemID, host, port, database, collection = sys.argv[1:]
else:
    print >> sys.stderr, '[usage] python %s itemID host port database collection [girderHost girderPort]' % sys.argv[0]
    sys.exit(1)


girderDb = pymongo.MongoClient(host=girderHost, port=int(girderPort))['girder']
settings = girderDb['setting']

mounts = settings.find_one({'key': 'mongoReader.mounts'})

if mounts is None:
    mounts = {'key': 'mongoReader.mounts', 'value': {}}

mounts['value'][itemID] = {
            'host': host,
            'port': int(port),
            'database': database,
            'collection': collection
        }

settings.insert(mounts)

