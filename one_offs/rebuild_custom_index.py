# -*- coding: utf-8 -*-

""" Rebuilds bell custom index.
    To run:
    - activate virtual environment
    - run this script. """

import json, os, pprint
import redis, rq
from bell_code.foundation.acc_num_to_data import SourceDictMaker
from bell_code.foundation.acc_num_to_pid import PidFinder


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
        "Returns a list of pids for the given collection_pid."
        pid_finder = PidFinder()
        doc_list = pid_finder._run_studio_solr_query( collection_pid, bdr_search_api_root )
        bdr_pid_list = []
        for entry in doc_list:
            bdr_pid_list.append( entry[u'pid'] )
        bdr_pid_list = sorted( bdr_pid_list )
        print u'- bdr_pid_list...'
        pprint.pprint( bdr_pid_list )
        return bdr_pid_list

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
        kwargs={}
        )
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
        kwargs={}
        )
    return

def run_make_pid_list_from_bdr_data():
    """ Calls for a list of pids for the given collection_pid. """
    bdr_collection_pid=os.environ[u'BELL_ANTP__COLLECTION_PID']
    bdr_search_api_root=os.environ[u'BELL_ANTP__SOLR_ROOT']
    collection_pids = reindexer.make_pid_list( bdr_collection_pid, bdr_search_api_root )
    bell_q.enqueue_call(
        func=u'bell_code.one_offs.rebuild_custom_index.run_make_pids_to_remove()',
        kwargs={ u'collection_pids': collection_pids }
        )
    return

def run_build_pids_to_remove( collection_pids ):
    """

if __name__ == u'__main__':
    run_start_reindex_all()
