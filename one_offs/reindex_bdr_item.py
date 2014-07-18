# -*- coding: utf-8 -*-

""" Given pid, reindexes item using bdr's bell data and api data. """

import json, os, pprint
import logging.handlers
import requests
from bell_code import bell_logger
from bell_code.tasks.indexer import Indexer

logger = bell_logger.setup_logger()
idxr = Indexer( logger )


class BdrItemReindexer( object ):

    def __init__( self ):
        self.PID = unicode( os.environ[u'BELL_ONEOFF_REINDEX_BDR_ITEM__PID'] )
        self.SOLR_URL_ROOT = unicode( os.environ[u'BELL_ONEOFF_REINDEX_BDR_ITEM__SOLR_URL_ROOT'] )
        self.BDR_BELL_DATA_URL_PATTERN = unicode( os.environ[u'BELL_ONEOFF_REINDEX_BDR_ITEM__BDR_BELL_DATA_URL_PATTERN'] )
        self.BDR_PUBLIC_ITEM_API_URL_PATTERN = unicode( os.environ[u'BELL_ONEOFF_REINDEX_BDR_ITEM__BDR_PUBLIC_ITEM_API_URL_PATTERN'] )

    def reindex_bdr_item( self ):
        """ Controls grabbing source metadata and image data, building solr-dict, and posting to solr. """
        bell_dict = self._grab_bell_data( self.PID, self.BDR_BELL_DATA_URL_PATTERN )
        metadata_solr_dict = idxr.build_metadata_only_solr_dict( self.PID, bell_dict )
        bdr_api_links_dict = self._grab_bdr_api_links_data( self.PID, self.BDR_PUBLIC_ITEM_API_URL_PATTERN )
        updated_solr_dict = idxr.add_image_metadata( metadata_solr_dict, bdr_api_links_dict )
        logger.debug( u'- in reindex_bdr_item.BdrItemReindexer.reindex_bdr_item(); updated_solr_dict, `%s`' %  pprint.pformat(updated_solr_dict) )
        validity = idxr.validate_solr_dict( updated_solr_dict )
        if validity:
            post_result = idxr.post_to_solr( updated_solr_dict )
            logger.debug( u'- in reindex_bdr_item.BdrItemReindexer.reindex_bdr_item(); post_result for pid `%s`, %s' % (self.PID, post_result) )
        return

    def _grab_bell_data( self, pid, bdr_bell_data_url_pattern ):
        """ Grabs bell json; returns dict. """
        url = bdr_bell_data_url_pattern % pid
        logger.debug( u'in _grab_bell_data(); url, `%s`' % url )
        r = requests.get( url )
        bell_dict = r.json()
        logger.debug( u'in _grab_bell_data(); type(bell_dict), `%s`' % type(bell_dict) )
        logger.debug( u'in _grab_bell_data(); bell_dict, `%s`' % pprint.pformat(bell_dict) )
        return bell_dict

    def _grab_bdr_api_links_data( self, pid, bdr_public_item_api_url_pattern ):
        """ Grabs and returns link info from item-api json.
            The links dict is used by tasks.indexer to add to the solr-metadata if needed. """
        url = bdr_public_item_api_url_pattern % pid
        logger.debug( u'in _grab_bdr_api_links_data(); url, `%s`' % url )
        r = requests.get( url )
        api_dict = r.json()
        links_dict = api_dict[u'links']
        logger.debug( u'in _grab_bdr_api_links_data(); links_dict, `%s`' % pprint.pformat(links_dict) )
        return links_dict

    ## end class BdrItemReindexer()




if __name__ == u'__main__':
    reindexer = BdrItemReindexer()
    reindexer.reindex_bdr_item()
