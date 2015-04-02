# -*- coding: utf-8 -*-

""" Given pid, reindexes item using bdr's bell data and api data. """

import json, os, pprint
import requests
from bell_code import bell_logger
from bell_code.tasks.indexer import CustomIndexUpdater

logger = bell_logger.setup_logger()
idxr = CustomIndexUpdater( logger )


class BdrItemReindexer( object ):
    """ Reindexes a single pid.
        Note, run on dev server for solr post access.
        Note, tasks.indexer.CustomIndexUpdater() needs its own set of environmental variables. """

    def __init__( self ):
        self.ACCESSION_NUMBER = unicode( os.environ[u'BELL_ONEOFF_REINDEX_BDR_ITEM__ACCESSION_NUMBER'] )
        self.PID = unicode( os.environ[u'BELL_ONEOFF_REINDEX_BDR_ITEM__PID'] )
        self.BDR_PUBLIC_ITEM_API_URL_ROOT = u'https://repository.library.brown.edu/api/pub/items'

    def reindex_bdr_item( self ):
        """ Manages source data-grab and reindexer call.
            Called manually. """
        bell_dct = self.grab_bell_data()
        idxr.update_custom_index_entry( accession_number, bell_dct, self.PID )
        return

    def grab_bell_data( self, pid ):
        """ Grabs bell json; returns dict.
            Called by reindex_bdr_item() """
        url = u'%s/%s/' % ( self.BDR_PUBLIC_ITEM_API_URL_ROOT, pid )
        logger.debug( u'in _grab_bell_data(); url, `%s`' % url )
        r = requests.get( url )
        api_dct = r.json()
        bell_jsn_url = dct[u'links'][u'content_datastreams'][u'bell_metadata']
        logger.debug( u'in _grab_bell_data(); bell_jsn_url, `%s`' % bell_jsn_url )
        r2 = requests.get( bell_jsn_url )
        bell_dct = r.json()
        logger.debug( u'in _grab_bell_data(); bell_dct, `%s`' % pprint.pformat(bell_dct) )
        return bell_dct

    # end class BdrItemReindexer




if __name__ == u'__main__':
    reindexer = BdrItemReindexer()
    reindexer.reindex_bdr_item()
