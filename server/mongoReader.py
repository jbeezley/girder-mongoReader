#!/usr/bin/env python

import json
import pymongo
import bson.json_util

from girder.constants import AccessType
from girder.events import bind
from girder import logger
from girder.utility import model_importer
from girder.api.describe import Description
from girder.api.rest import loadmodel, Resource, RestException

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


class MongoMounts(Resource):
    
    mongoMountAttribute = 'mongoMount'

    def __init__(self, *args, **kwargs):

        super(MongoMounts, self).__init__(*args, **kwargs)
        self._mounts = {}

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
    
    def connectToMongoCollection(self, params):
        
        self.requireParams(('host', 'port', 'database', 'collection'), params)
        host = params['host']
        port = params['port']
        try:
            port = int(port)
        except ValueError:
            raise RestException('Invalid port number')
        try:
            client = pymongo.MongoClient(host=host, port=port)
        except pymongo.errors.ConnectionFailure:
            raise RestException('Could not connect to mounted database')

        dbName = params['database']
        if not dbName in client.database_names():
            raise RestException('Database does not exist')

        db = client[dbName]
        
        logger.info('Connected to mongo collection at %s:%i/%s/%s' % (host, port, dbName, params['collection']))
        
        return db[params['collection']]

    def getCollection(self, id, params):

        collection = self._mounts.get(id)
        if collection is None:
            collection = self.connectToMongoCollection(params)
            self._mounts[id] = collection

        return collection

    @loadmodel(map={'id': 'item'}, model='item', level=AccessType.ADMIN)
    def createMount(self, item, params):
        self.requireParams(('host', 'port', 'database', 'collection'), params)
        item[self.mongoMountAttribute] = {
                'host': params['host'],
                'port': params['port'],
                'database': params['database'],
                'collection': params['collection']
        }
        item = self.model('item').updateItem(item)
        return item

    createMount.description = (
            Description('Mount an external mongo collection to an item.')
            .notes('The item must already exist and be writable.')
            .param('id', 'The ID of the item', paramType='path')
            .param('host', 'The host name or ip of the mongo instance.')
            .param('port', 'The port that the mongo instance is listening on.', dataType='int')
            .param('database', 'The database in the mongo instance to read.')
            .param('collection', 'The collection in the database to mount.')
            .errorResponse()
    )

    @loadmodel(map={'id': 'item'}, model='item', level=AccessType.READ)
    def download(self, item, event):
        
        mountObject = item.get(self.mongoMountAttribute)
        if mountObject is None:
            return
        
        logger.info('Bypassing default route to item ' + str(event.info['id']))
        query = event.info['params']
        
        # copy the query dict so we don't change the original
        query = query.copy()

        # parse the query
        parsed = self.parseQuery(query)

        limit = parsed['limit']
        skip = parsed['skip']
        sort = parsed['sort']
        sortdir = parsed['sortdir']
        query = parsed['query']
        
        # connect to the database if no connection yet exists
        collection = self.getCollection(event.info['id'], mountObject)

        # get the cursor
        cursor = collection.find(query)
        
        # modify the cursor
        cursor.limit(limit)
        cursor.skip(skip)
        cursor.sort(sort, sortdir)
        
        # return a streaming function to the response handler
        event.addResponse(dbStreamer(cursor))
        event.preventDefault()

def load(info):
     
    logger.info('mongoReader loaded')
    m = MongoMounts()
    bind('rest.get.item/:id/download.before', 'mongoReader.download', 
            lambda event: m.download(id=event.info['id'], event=event))
    info['apiRoot'].item.route('PUT', (':id', 'create_mount', ), m.createMount)

