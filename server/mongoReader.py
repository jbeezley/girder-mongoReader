#!/usr/bin/env python

import json
import pymongo
import bson.json_util

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
    # get item id and see if it corresponds to a mounted database
    # if so return the collection, or do nothing

    m = mounts.get(event.info['id'])
    if m is None:
        return
    try:
        logger.info('mongoReader handler called')
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
    
    @classmethod
    def parseQuery(cls, query):
        
        # process a download request from a mongodb query
        try:
            limit = int(query.pop('limit', 0))
        except ValueError:
            raise ValueError("limit argument ('%s') is not an int" % limit)
        try:
            skip = int(query.pop('offset', 0))
        except ValueError:
            raise ValueError("offset argument ('%s') is not an int" % skip)
        sort = query.pop('sort', '_id')
        try:
            sortdir = int(query.pop('sortdir', pymongo.ASCENDING))
        except ValueError:
            raise ValueError("sortdir argument ('%s') is not an int" % sortdir)
        
        # use bson to parse the cursor into a dictionary
        for key, value in query.iteritems():
            # try to parse using bson, but pass the original string if it fails
            try:
                query[key] = bson.json_util.loads(query[key])
            except ValueError:
                pass

        return {
                'limit': limit,
                'skip': skip,
                'sort': sort,
                'sortdir': sortdir,
                'query': query
            }



    def download(self, query):
        # copy the query dict so we don't change the original
        query = query.copy()

        # parse the query
        parsed = self.parseQuery(query)

        limit = parsed['limit']
        skip = parsed['skip']
        sort = parsed['sort']
        sortdir = parsed['sortdir']
        query = parsed['query']

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

    setting = model_importer.ModelImporter().model('setting')
    config = setting.get('mongoReader.mounts', default={})
    for itemID, target in config.iteritems():
        mounts[itemID] = MongoMount(target)

    bind('rest.get.item/:id/download.before', 'mongoReader.download', downloadHandler)

