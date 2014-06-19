# -*- coding: utf-8 -*-

""" Rebuilds bell custom index.
    To run:
    - activate virtual environment
    - run this script. """

import json, os, pprint
import redis, rq
from bell_code import bell_logger
from bell_code.foundation.acc_num_to_data import SourceDictMaker
from bell_code.foundation.acc_num_to_pid import PidFinder
from bell_code.tasks.indexer import Indexer


logger = bell_logger.setup_logger()


class CustomReindexer( object ):

    def make_initial_json( self, fmpro_xml_path, fmpro_json_path ):
        """ Converts raw filemakerpro xml to json.
            Structure is a dict of acession-number to data. """
        json_maker = SourceDictMaker()
        json_maker.convert_fmproxml_to_json( fmpro_xml_path, fmpro_json_path )
        with open( fmpro_json_path ) as f:
            fmpro_json_dict = json.loads( f.read() )
        assert sorted( fmpro_json_dict.keys() ) == [ u'count', u'datetime', u'items' ], sorted( fmpro_json_dict.keys() )
        return

    def make_pid_dict( self, bdr_collection_pid, fmpro_json_path, bdr_search_api_root, output_json_path ):
        """ Creates a json file containing an accession-number to pid dict. """
        pid_finder = PidFinder()
        pid_finder.make_dict(
            bdr_collection_pid, fmpro_json_path, bdr_search_api_root, output_json_path )
        with open( output_json_path ) as f:
            json_dict = json.loads( f.read() )
        assert sorted( json_dict.keys() ) == [ u'count', u'datetime', u'final_accession_pid_dict' ], sorted( json_dict.keys() )
        # assert json_dict[u'count'][u'count_null'] == 0  # count_null SHOULD be zero but i'm skipping this issue for now
        return

    def make_pid_list( self, collection_pid, bdr_search_api_root ):
        """ Returns a list of pids for the given collection_pid."""
        pid_finder = PidFinder()
        doc_list = pid_finder._run_studio_solr_query( collection_pid, bdr_search_api_root )
        bdr_pid_list = []
        for entry in doc_list:
            bdr_pid_list.append( entry[u'pid'] )
        bdr_pid_list = sorted( bdr_pid_list )
        print u'- bdr_pid_list...'
        return bdr_pid_list

    def make_pids_to_remove( self, pids_from_collection, pids_for_accession_number_json_path ):
        """ Returns a list of pids for removal from custom index, and, perhaps, later, bdr. """
        with open( pids_for_accession_number_json_path ) as f:
            json_dict = json.loads( f.read() )
        assert sorted( json_dict.keys() ) == [ u'count', u'datetime', u'final_accession_pid_dict' ]
        pids_for_accession_numbers = json_dict[u'final_accession_pid_dict'].values()
        pids_to_remove_set = set(pids_from_collection) - set(pids_for_accession_numbers)
        pids_to_remove_list = list( pids_to_remove_set )
        print u'pids_to_remove_list...'
        pprint.pprint( pids_to_remove_list )
        return pids_to_remove_list

    def remove_pid_from_custom_index( self, pid ):
        """  Removes entry from custom index. """
        logger = bell_logger.setup_logger()
        idxr = Indexer( logger )
        idxr.delete_item( pid )

    ## end class CustomReindexer()


reindexer = CustomReindexer()
bell_q = rq.Queue( u'bell:job_queue', connection=redis.Redis() )

def run_start_reindex_all():
    """ Starts full custom-reindex process
        Calls to build a json file of bell latest xml export.
        Process:
        - build list of all accession-numbers from latest run.
        - get pids for those accession-numbers.
          - (there should be a pid for each accession number)
          - this is the list of accession-numbers/pids to reindex
        - build list of all pids in bell collection
        - build list of all pids not in the to-index list
          - this is the list of accession-numbers/pids to remove from the index
        - enqueue all the remove jobs
        - enqueue all the reindex jobs
        """
    fmpro_xml_path = os.environ[u'BELL_ANTD__FMPRO_XML_PATH']
    fmpro_json_path = os.environ[u'BELL_ANTD__JSON_OUTPUT_PATH']
    reindexer.make_initial_json( fmpro_xml_path, fmpro_json_path )  # just savws to json; nothing returned
    bell_q.enqueue_call(
        func=u'bell_code.one_offs.rebuild_custom_index.run_make_pid_dict_from_bell_data',
        kwargs={} )
    return

def run_make_pid_dict_from_bell_data():
    """ Calls to create a json file containing an accession_number-to-pid dict. """
    bdr_collection_pid=os.environ[u'BELL_ANTP__COLLECTION_PID']
    fmpro_json_path=os.environ[u'BELL_ANTP__BELL_DICT_JSON_PATH']  # file of dict of bell-accession-number to metadata
    bdr_search_api_root=os.environ[u'BELL_ANTP__SOLR_ROOT']
    output_json_path=os.environ[u'BELL_ANTP__OUTPUT_JSON_PATH']
    reindexer.make_pid_dict( bdr_collection_pid, fmpro_json_path, bdr_search_api_root, output_json_path )
    bell_q.enqueue_call(
        func=u'bell_code.one_offs.rebuild_custom_index.run_make_pid_list_from_bdr_data',
        kwargs={} )
    return

def run_make_pid_list_from_bdr_data():
    """ Calls for a list of pids for the given collection_pid. """
    bdr_collection_pid=os.environ[u'BELL_ANTP__COLLECTION_PID']
    bdr_search_api_root=os.environ[u'BELL_ANTP__SOLR_ROOT']
    collection_pids = reindexer.make_pid_list( bdr_collection_pid, bdr_search_api_root )
    bell_q.enqueue_call(
        func=u'bell_code.one_offs.rebuild_custom_index.run_make_pids_to_remove',
        kwargs={ u'pids_from_collection': collection_pids } )
    return

def run_make_pids_to_remove( pids_from_collection ):
    """ Calls for a list of pids to remove. """
    assert type(pids_from_collection) == list
    pids_for_accession_number_json_path = unicode( os.environ[u'BELL_ANTP__OUTPUT_JSON_PATH'] )
    pids_to_remove = reindexer.make_pids_to_remove( pids_from_collection, pids_for_accession_number_json_path )
    bell_q.enqueue_call(
        func=u'bell_code.one_offs.rebuild_custom_index.run_make_pids_to_update()',
        kwargs={} )
    for pid in pids_to_remove:
        bell_q.enqueue_call(
            func=u'bell_code.one_offs.rebuild_custom_index.run_remove_pid_from_custom_index',
            kwargs={ u'pid': pid } )
    return

def run_remove_pid_from_custom_index( pid ):
    """ Calls to remove pid from custom bell index. """
    assert type(pid) == unicode
    # reindexer.remove_pid_from_custom_index( pid )
    return




if __name__ == u'__main__':
    run_start_reindex_all()
