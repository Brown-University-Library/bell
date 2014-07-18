# -*- coding: utf-8 -*-

""" Given pid, reindexes item using bdr's bell data and api data. """

import json
import logging.handlers
import requests
from bell_code import bell_logger
from bell_code.tasks.indexer import Indexer

logger = bell_logger.setup_logger()
idxr = Indexer( logger )


class BdrItemReindexer( object ):

    def __init__( self ):
        self.JSON_STRING = os.environ[u'BELL_ONEOFF_JSON_STRING']
        self.json_dict = json.loads( self.JSON_STRING )
        self.PID = self.json_dict[ u'pid' ]
        self.SOLR_URL_ROOT = self.json_dict[ u'solr_url_root' ]
        self.BDR_BELL_DATA_URL_PATTERN = self.json_dict[ u'bdr_bell_data_url_pattern' ]
        self.BDR_PUBLIC_ITEM_API_URL_PATTERN = self.json_dict[ u'bdr_public_item_api_url_pattern' ]

    def reindex_bdr_item( self ):
        """ Controls grabbing source metadata and image data, building solr-dict, and posting to solr. """
        bell_dict = self._grab_bell_data( self.PID )
        metadata_solr_dict = idxr.build_metadata_only_solr_dict( self.PID, bell_dict )
        bdr_api_links_dict = self._grab_bdr_api_links_data( self.PID, self.BDR_PUBLIC_ITEM_API_URL_PATTERN )
        updated_solr_dict = idxr.add_image_metadata( metadata_solr_dict, bdr_api_links_dict )
        validity = idxr.validate_solr_dict( updated_solr_dict )
        if validity:
            post_result = bell_indexer.post_to_solr( updated_solr_dict )
            self.logger.debug( u'- in reindex_bdr_item.BdrItemReindexer.reindex_bdr_item(); post_result for pid `%s`, %s' % (pid, post_result) )
        else:
            self.logger.debug( u'- in reindex_bdr_item.BdrItemReindexer.reindex_bdr_item(); why not valid?'
        return

    def _grab_bell_data( self, pid, bdr_bell_data_url_pattern ):
        """ Grabs bell json; returns dict. """
        url = bdr_bell_data_url_pattern % pid
        logger.debug( u'in _grab_bell_data(); url, `%s`' % url )
        r = requests.get( url )
        bell_dict = r.json
        return bell_dict

    def _grab_bdr_api_links_data( self, pid, bdr_public_item_api_url_pattern ):
        """ Grabs and returns link info from item-api json.
            The links dict is used by tasks.indexer to add to the solr-metadata if needed. """
        url = bdr_public_item_api_url_pattern % pid
        logger.debug( u'in _grab_bdr_api_links_data(); url, `%s`' % url )
        r = requests.get( url )
        links_dict = r.json
        return links_dict

    ## end class BdrItemReindexer()




if __name__ == u'__main__':
    reindexer = BdrItemReindexer()
    reindexer.reindex_bdr_item()
