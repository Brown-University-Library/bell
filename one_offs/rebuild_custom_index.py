# -*- coding: utf-8 -*-

""" Rebuilds bell custom index.
    To run:
    - activate virtual environment
    - run this script. """

import json, os
import redis, rq
from bell_code.foundation.acc_num_to_data import SourceDictMaker
from bell_code.foundation.acc_num_to_pid import PidFinder


class CustomReindexer( object ):

    def make_initial_json( self, fmpro_xml_path, fmpro_json_path ):
        """ Converts raw filemakerpro xml to json. """
        json_maker = SourceDictMaker()
        json_maker.convert_fmproxml_to_json( fmpro_xml_path, fmpro_json_path )
        return

    def make_pid_dict( self, bdr_collection_pid, bell_dict_json_path, solr_root_url, output_json_path ):
        """ Adds pids to filemakerpro json. """
        pid_finder = PidFinder()
        pid_finder.make_dict(
        bdr_collection_pid, bell_dict_json_path, solr_root_url, output_json_path )
        with open( output_json_path ) as f:
            json_dict = json.loads( f.read() )
        assert sorted( json_dict.keys() ) == [ u'count', u'datetime', u'final_accession_pid_dict' ], sorted( json_dict.keys() )
        assert json_dict[u'count'][u'count_null'] == 0
        return

    ## end class CustomReindexer()


reindexer = CustomReindexer()
bell_idxr_q = rq.Queue( u'bell:reindexer', connection=redis.Redis() )

def run_start_reindex_all():
    """ Starts full custom-reindex process
        Builds list of all accession-numbers from latest run.
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
    reindexer.make_initial_json( fmpro_xml_path, fmpro_json_path )
    bell_idxr_q.enqueue_call(
        func=u'bell_code.rebuild_custom_index.run_make_pid_dict_from_bell_data',
        kwargs={}
        )
    return

def run_make_pid_dict_from_bell_data():
    """ Creates a json file containing an accession_number-to-pid dict. """
    bdr_collection_pid=os.environ[u'BELL_ANTP__COLLECTION_PID']
    bell_dict_json_path=os.environ[u'BELL_ANTP__BELL_DICT_JSON_PATH']  # file of dict of bell-accession-number to metadata
    solr_root_url=os.environ[u'BELL_ANTP__SOLR_ROOT']
    output_json_path=os.environ[u'BELL_ANTP__OUTPUT_JSON_PATH']
    reindexer.make_pid_dict( bdr_collection_pid, bell_dict_json_path, solr_root_url, output_json_path )
    bell_idxr_q.enqueue_call(
        func=u'bell_code.rebuild_custom_index.run_make_pid_list_from_bdr_data',
        kwargs={}
        )
    return

def run_make_pid_list_from_bdr_data():
    """ Returns a list of pid-to-data dict entries from querying the bell bdr-collection pid. """
    pass


if __name__ == u'__main__':
    run_start_reindex_all()
