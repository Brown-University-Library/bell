# -*- coding: utf-8 -*-

""" Grabs all items in bell collection.
    Iterates through them, updating custom index.
        - Checking:
            - accession number in correct place.
            - (future) existence of both jp2 and master if either exists
            - (future) initial stream check on jp2 and master image
    TODO: create another script: 'index_custom__remove_old.py' """

import json, os, pprint, sys, time
import requests
from bell_code import bell_logger


class Reindexer( object ):
    """ Updates bell custom index with all bdr-data (from bdr-solr).
        if __name__... at bottom indicates how to run this script. """

    def __init__( self ):
        self.logger = bell_logger.setup_logger()

    def reindex_all_bdr( self,
        bdr_collection_pid,
        solr_root_url,
        output_json_path ):
        solr_query_docs = self._run_bdr_solr_query( bdr_collection_pid, solr_root_url )
        accession_number_validation = self._run_accession_number_validation( solr_query_docs )
        pprint.pprint( accession_number_validation )

    ## grab initial minimal solr data

    def _run_bdr_solr_query( self, bdr_collection_pid, solr_root_url ):
        """ Returns _solr_ doc list.
            Example result: [ {pid:bdr123, identifier:[acc_num_a,other_num_b], mods_id_bell_accession_number_ssim:None_or_acc_num_a, other:...}, etc. ]
            Called by reindex_all_bdr(). """
        doc_list = []
        for i in range( 100 ):  # would handle 50,000 records
        # for i in range( 2 ):  # would handle 50,000 records
            data_dict = self.__query_solr( i, bdr_collection_pid, solr_root_url )
            docs = data_dict[u'response'][u'docs']
            doc_list.extend( docs )
            if not len( docs ) > 0:
                break
        # self.logger.info( u'in _run_studio_solr_query(); doc_list, %s' % pprint.pformat(doc_list) )
        return doc_list

    def __query_solr( self, i, bdr_collection_pid, solr_root_url ):
        """ Queries solr for iterating start-row.
            Returns results dict.
            Called by _run_bdr_solr_query() """
        new_start = i * 500  # for solr start=i parameter (cool, eh?)
        # new_start = i * 2  # for solr start=i parameter (cool, eh?)
        params = {
            u'q': u'rel_is_member_of_ssim:"%s"' % bdr_collection_pid,
            u'fl': u'pid,mods_id_bell_accession_number_ssim,primary_title',
            u'rows': 500, u'start': new_start, u'wt': u'json' }
        r = requests.get( solr_root_url, params=params, verify=False )
        self.logger.info( u'in __query_solr(); r.url, %s' % r.url )
        data_dict = json.loads( r.content.decode(u'utf-8', u'replace') )
        time.sleep( .1 )
        # self.logger.info( u'in __query_solr(); data_dict, %s' % pprint.pformat(data_dict) )
        return data_dict

    ## validate accession number

    def _run_accession_number_validation( self, solr_query_docs ):
        """ Takes list of dicts from solr query.
            Ensures each entry has an accession number.
            Raises an exception and prints list of entries that don't have accession-numbers in expected places.
            Returns True on success.
            Called by reindex_all_bdr() """
        bad_apples = []
        counter = 0
        for item_dict in solr_query_docs:
            counter += 1
            if item_dict.get( u'mods_id_bell_accession_number_ssim', None ) == None:
                bad_apples.append( item_dict )
            elif len( item_dict[u'mods_id_bell_accession_number_ssim'] ) == 0:  # empty list
                bad_apples.append( item_dict )
        print u'%s items checked; bad apples...' % counter; pprint.pprint( bad_apples )
        if len( bad_apples ) > 0:
            return False
        else:
            return True


if __name__ == u'__main__':
    """ Assumes env is activated.
        ( 'ICRAB' used as a namespace prefix for this 'index_custom__reindex_all_bdr.py' file. ) """
    bdr_collection_pid=os.environ.get( u'BELL_ICRAB__COLLECTION_PID' )
    solr_root_url=os.environ.get( u'BELL_ICRAB__SOLR_ROOT' )
    output_json_path=os.environ.get( u'BELL_ICRAB__OUTPUT_JSON_PATH' )
    reindexer = Reindexer()
    reindexer.reindex_all_bdr(
        bdr_collection_pid, solr_root_url, output_json_path )
