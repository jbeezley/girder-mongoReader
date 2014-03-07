#!/usr/bin/env python

import json
import pymongo

from girder.events import bind
from girder import logger
from girder.utility import model_importer

mounts = {}

def dbStreamer(cursor):
    # returns a function that streams the data one record at a time

    def stream():
        addComma = False
        yield '[\n'
        for record in cursor:
            s = json.dumps(record, ensure_ascii=False, default=str)
            if addComma:
                s = ',\n' + s
            addComma = True
            yield s
        yield '\n]'

    return stream


def downloadHandler(event):
    m = mounts.get(event.info['id'])
    if m is None:
        return
    try:
        logger.info('handler called')
        response = m.download(event.info['params'])
        event.preventDefault()
        event.addResponse(response)
    except Exception as e:
        logger.info('exception caught in mongoReader: ' + str(e))

class MongoMount(object):

    def __init__(self, mongoObj):
        
        # get mongo server, defaulting to localhost if not specified
        host = mongoObj.get('host', 'localhost')
        port = mongoObj.get('port', 27017)
        
        try:
            client = pymongo.MongoClient(host=host, port=port)
        except pymongo.errors.ConnectionFailure:
            raise Exception("Could not connect to mongodb server at %s:%i" % (host, port))

        # get database name
        dbName = mongoObj.get('database')
        if dbName is None:
            raise Exception("No database source specified")
        elif not dbName in client.database_names():
            raise Exception("The database '%s' does not exist" % dbName) 

        db = client[dbName]

        # get the collection from the database
        collection = mongoObj.get('collection')
        if collection is None:
            raise Exception("No collection was specified")

        self.collection = db[collection]
    
    def download(self, query):

        # process a download request from a mongodb query
        
        limit = int(query.pop('limit', 0))
        skip = int(query.pop('offset', 0))
        sort = query.pop('sort', '_id')
        sortdir = int(query.pop('sortdir', pymongo.ASCENDING))

        # get the cursor
        cursor = self.collection.find(query)
        
        # modify the cursor
        cursor.limit(limit)
        cursor.skip(skip)
        cursor.sort(sort, sortdir)
        
        # return a streaming function to the response handler
        return dbStreamer(cursor)

mounts = {}

def load(info):
     
    logger.info('mongoReader loaded')

    # TODO: define mount points in girder config
    m = MongoMount({
        'database': 'healthMap',
        'collection': 'records'
        })
    mounts['53161aa911212641955cfc7a'] = m

    bind('rest.get.item/:id/download.before', 'mongoReader.download', downloadHandler)

