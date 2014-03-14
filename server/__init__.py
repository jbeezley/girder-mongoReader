from mongoReader import MongoMounts
from girder import events

def load(info):
    mount = MongoMounts()
    events.bind('rest.get.item/:id/download.before', 'mongoReader.download', 
            lambda event: mount.download(id=event.info['id'], event=event))
    info['apiRoot'].item.route('PUT', (':id', 'mount', ), mount.createMount)
