#!/usr/bin/env python
__author__='hubbard'
__date__ ='$May 6, 2009 9:29:42 AM$'

import logging
import os
from magnet import pole, field
import redis
import dap_getter
from exceptions import OSError

class Wallet(pole.BasePole):
    """The Wallet class manages the cache. Yeah, I like puns.

    Uses:
    - redis for dataset directory
    - http error codes (200, 404) in message to denote returns
    - 'dataset' as a routing key
    - 'dataset_reply' for responses
    - dap_tools to download
    - amoeba for Redis instance
    - ':' as KVS separator in Redis

    TODO: Persistant redis connection
    TODO: figure out how to get magnet to setup logging for us, or get an init method
    """

    def __repr__(self):
        """Cribbed from the magnet example - useful?"""
        return "Wallet DAP cache manager"

    ##########################
    # Magnet actions
    def action_dset_query(self, msg):
        """Query - is a dataset (or redis regex) in the cache?"""
        # TODO: Move this to DF Agent?
        dsetName = msg['payload']
        logging.info('Got query for "%s"' % dsetName)
        qr = self.dataset_query(dsetName)
        if qr != []:
            self.reply_found(dsetName, str(qr))
        else:
            self.reply_notfound(dsetName)

    def action_dset_fetch(self, msg):
        """Command - please go add dataset to the cache"""
        dsetName = msg['payload']
        logging.info('Got fetch command for "%s"' % dsetName)
        msg, rc = self.dataset_fetch(dsetName)
        if rc == 200:
            self.reply_cached(dsetName)
        else:
            self.reply_dset_error(dsetName, msg)

    def action_dset_purge(self, msg):
        """Command - remove a dataset from the cache"""
        dsetName = msg['payload']
        logging.info('Got purge command for dataset "%s"' % dsetName)
        rc = self.dataset_purge(dsetName)
        if rc == 200:
            logging.info('Dataset purged')
            self.reply_purged(dsetName)
        else:
            self.reply_dset_error(dsetName, msg)

    ###########################
    # Internal methods
    def makeMsg(self, method, payload, returnCode=200):
        """Convenience method to create a message in the magnet style"""
        msg = dict()
        msg['method'] = method
        msg['payload'] = payload
        msg['return_code'] = str(returnCode)
        return msg

    def kvsPrefix(self):
        """Prefix in redis for datasets"""
        return 'datasets:'

    def dataset_fetch(self, dsetUrl):
        """Use dap_getter to download dataset"""
        logging.info('Starting download of dataset "%s"' % dsetUrl)
        rc = dap_getter.copytonc(dsetUrl)
        if rc != None:
            logging.info('Dataset "%s" downloaded OK to "%s"' % (dsetUrl, rc))
            # Add to directory
            self.dataset_update(dsetUrl, rc)
            return rc, 200
        else:
            logging.error('Dataset "%s" not downloaded - error was "%s"' (dsetUrl, rc))
            return rc, 500

    def dataset_query(self, dsetRegex):
        """Query dataset(s) status from Redis directory"""
        logging.info('Query for dataset regex "%s"' % dsetRegex)
        kvs = redis.Redis(host='amoeba.ucsd.edu')
        foo = kvs.keys('%s%s' % (self.kvsPrefix(), dsetRegex))
        kvs.disconnect()
        return foo

    def dataset_update(self, dsetName, localName):
        """Add dataset to Redis post-download"""
        logging.info('Updating redis with dataset "%s":%s' % (dsetName, localName))
        kvs = redis.Redis(host='amoeba.ucsd.edu')

        # Key is original name, value is local name
        kvs.set('%s%s' % (self.kvsPrefix(), dsetName), localName)
        kvs.disconnect()

    def dataset_purge(self, dsetName):
        """Purge a dataset from cache and directory"""
        logging.info('Purging dataset "%s"' % dsetName)

        kvs = redis.Redis(host='amoeba.ucsd.edu')
        localName = kvs.get('%s%s' % (self.kvsPrefix(), dsetName))
        if localName == None:
            logging.warning('Dataset not found in Redis')
            kvs.disconnect()
            return 500
        try:
            rc = os.remove(localName)
            if rc == None:
                rc = 200
            else:
                rc = 501
        except OSError:
            rc = 501
            
        # Delete from redis even if couldn't delete from disk...skew error
        kvs.delete('%s%s' % (self.kvsPrefix(), dsetName))
        kvs.disconnect()
        return rc

    def reply_purged(self, dsetName):
        """Inform client of successful cache removal"""
        logging.info('cache purge succeeded')
        reply = self.makeMsg('dataset_reply', 'Dataset "%s" purged OK' % dsetName, 200)
        self.sendMessage(reply, 'dataset')

    def reply_cached(self, dsetName):
        """Inform client of successful cache addition"""
        logging.info('cache op succeeded')
        reply = self.makeMsg('dataset_reply', 'Dataset "%s" cached OK' % dsetName, 200)
        self.sendMessage(reply, 'dataset')

    def reply_dset_error(self, dsetName, msg):
        """Inform client of cache failure"""
        logging.error('Cache op of "%s" failed, "%s"' % (dsetName, msg))
        # TODO: Propagate real error code & perror string
        reply = self.makeMsg('dataset_reply', 'Error caching dataset "%s": File error' % dsetName, 500)
        self.sendMessage(reply, 'dataset')

    def reply_notfound(self, dsetName):
        """Query returned no results."""
        logging.warning('Returning 404 on dataset expression "%s"' % dsetName)
        reply = self.makeMsg('dataset_reply', 'Dataset "%s" not found' % dsetName, 404)
        self.sendMessage(reply, 'dataset')

    def reply_found(self, dsetName, listing):
        """Dataset or regex is present in cache. FTW!"""
        logging.info('Cache hit on dataset expression "%s"' % dsetName)
        reply = self.makeMsg('dataset_reply', listing, 200)
        self.sendMessage(reply, 'dataset')

###################################################################################
# The plugin system uses this
wallet = Wallet(routing_pattern='dataset')
