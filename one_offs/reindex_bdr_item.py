# -*- coding: utf-8 -*-

from __future__ import unicode_literals

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
        self.ACCESSION_NUMBER = unicode( os.environ['BELL_ONEOFF_REINDEX_BDR_ITEM__ACCESSION_NUMBER'] )
        self.PID = unicode( os.environ['BELL_ONEOFF_REINDEX_BDR_ITEM__PID'] )
        self.BDR_PUBLIC_ITEM_API_URL_ROOT = 'https://repository.library.brown.edu/api/pub/items'

    def reindex_bdr_item( self ):
        """ Manages source data-grab and reindexer call.
            Called manually. """
        api_dct = self.grab_api_dct()
        bell_dct = self.grab_bell_dct( api_dct )
        idxr.update_custom_index_entry( self.ACCESSION_NUMBER, bell_dct, self.PID )
        return

    def grab_api_dct( self ):
        """ Grabs api data.
            Called by reindex_bdr_item() """
        url = '%s/%s/' % ( self.BDR_PUBLIC_ITEM_API_URL_ROOT, self.PID )
        logger.debug( 'in grab_api_dct(); url, `%s`' % url )
        r = requests.get( url )
        api_dct = r.json()
        return api_dct

    def grab_bell_dct( self, api_dct ):
        """ Grabs source json & cleans accession_number.
            Called by reindex_bdr_item() """
        bell_jsn_url = api_dct['links']['content_datastreams']['bell_metadata']
        logger.debug( 'in grab_bell_dct(); bell_jsn_url, `%s`' % bell_jsn_url )
        r = requests.get( bell_jsn_url )
        bell_dct = r.json()
        src_accession_number = bell_dct['calc_accession_id']
        bell_dct['calc_accession_id'] = src_accession_number.strip()
        logger.debug( 'in grab_bell_dct(); bell_dct, `%s`' % pprint.pformat(bell_dct) )
        return bell_dct

    # end class BdrItemReindexer




if __name__ == '__main__':
    reindexer = BdrItemReindexer()
    reindexer.reindex_bdr_item()
