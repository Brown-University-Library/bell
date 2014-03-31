# -*- coding: utf-8 -*-

import datetime, json, os, pprint, sys, time
import requests

extra_path = os.path.abspath( u'./' )  # adds bell_code to path
sys.path.append( extra_path )
import bell_logger


class PidFinder( object ):
    """ Handles creation of an accession_number-to-pid-info dict, saved as a json file.
        Purpose: This is one of three essential files that should exist before doing almost any bell processing,
                 because the source bell data contains no pid info, and it is essential to know whether a bell item
                 needs to create or update bdr data.
        if __name__... at bottom indicates how to run this script. """

    def __init__( self ):
        self.logger = bell_logger.setup_logger()

    def make_dict( self,
        bdr_collection_pid,
        bell_dict_json_path,
        solr_root_url,
        output_json_path ):
        """ CONTROLLER.
            - Calls repo solr (500 rows at a time) to get all bell items
            - Creates an accession-number-to-pid dict from above
            - Creates an accesstion-number-to-pid dict from submitted data and saves to json file
            - Example produced data: { acc_num_1: {pid:bdr_123, title:abc}, acc_num_2: {pid:None, title:None}, etc. } """
        #
        #Run studio-solr query
        #Purpose: get raw child-pids data from _solr_, along with accession-number data
        #Example returned data: [ {pid:bdr123, identifier:[acc_num_a,other_num_b], mods_id_bell_accession_number_ssim:None_or_acc_num_a}, etc. ]
        solr_query_docs = self._run_studio_solr_query( bdr_collection_pid, solr_root_url )
        print u'- _run_studio_solr_query() done'
        #
        #Parse solr-results to accession_number:pid dict
        #Purpose: create dict for lookup by source accession-numbers
        #Example returned data: { acc_num_123:bdr_1, acc_num_456:bdr_2 }
        solr_accnum_to_pid_dict = self._make_solr_accnum_to_pid_dict( solr_query_docs )
        print u'- _make_solr_accnum_to_pid_dict() done'
        #
        #Get _bell_ accession numbers
        #Purpose: create list of bell accession numbers from _bell_ data
        #Example returned data: [ 'acc_num_1', 'acc_num_2', etc. ]
        bell_source_accession_numbers = self._load_bell_accession_numbers( bell_dict_json_path )
        print u'- _load_bell_accession_numbers() done'
        #
        #Make final accession-number dict
        #Purpose: go through bell accession-numbers, add any bdr-info, note lack of bdr-info
        #Example returned data: { acc_num_1: {pid:bdr_123, state:active}, acc_num_2: {pid:None, state:None} }
        final_accession_dict = self._make_final_accession_number_dict( bell_source_accession_numbers, solr_accnum_to_pid_dict )
        print u'- _make_final_accession_number_dict() done'
        #
        #Output json
        self._output_json( final_accession_dict, output_json_path )
        print u'- _output_json() done; all processing done'
        return

    def _run_studio_solr_query( self, bdr_collection_pid, solr_root_url ):
        """ Returns _solr_ doc list.
            Example solr url: 'https://solr-url/?q=rel_is_member_of_ssim:"collection:pid"&start=x&rows=y&fl=pid,mods_id_bell_accession_number_ssim,primary_title'
            Example result: [ {pid:bdr123, identifier:[acc_num_a,other_num_b], mods_id_bell_accession_number_ssim:None_or_acc_num_a, other:...}, etc. ] """
        doc_list = []
        for i in range( 100 ):  # would handle 50,000 records
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
            Called by self._run_studio_solr_query() """
        new_start = i * 500  # for solr start=i parameter (cool, eh?)
        params = {
            u'q': u'rel_is_member_of_ssim:"%s"' % bdr_collection_pid,
            u'fl': u'pid,accession_number_original,identifier,mods_id_bell_accession_number_ssim,primary_title',
            u'rows': 500, u'start': new_start, u'wt': u'json' }
        r = requests.get( solr_root_url, params=params, verify=False )
        self.logger.info( u'in __query_solr(); r.url, %s' % r.url )
        data_dict = json.loads( r.content.decode(u'utf-8', u'replace') )
        time.sleep( .1 )
        # self.logger.info( u'in __query_solr(); data_dict, %s' % pprint.pformat(data_dict) )
        return data_dict

    def _make_solr_accnum_to_pid_dict( self, solr_query_docs ):
        """ Returns accession_number:pid dict from _solr_ data. """
        solr_accnum_pid_dict = { u'errors':[] }
        for solr_doc in solr_query_docs:
            pid = solr_doc[u'pid']
            try:
                accession_number = solr_doc[u'mods_id_bell_accession_number_ssim'][0]  # accession-numbers are in solr as a single-item list
                solr_accnum_pid_dict[accession_number] = pid
            except:
                solr_accnum_pid_dict[u'errors'].append( pid )
                print u'-- missing accession-number in expected solr `mods_id_bell_accession_number_ssim` field --'; pprint.pprint( solr_doc ); print u'--'
        self.logger.info( u'in _make_solr_accnum_to_pid_dict(); errors, %s' % sorted(solr_accnum_pid_dict[u'errors']) )
        return solr_accnum_pid_dict

    def _load_bell_accession_numbers( self, bell_dict_json_path ):
        """ Returns sorted accession-number keys list from bell-json-dict.
            Example: [ 'acc_num_1', 'acc_num_2', etc. ] """
        with open( bell_dict_json_path ) as f:
            accession_dict = json.loads( f.read() )
        keys = sorted( accession_dict[u'items'].keys() )
        # pprint.pprint( keys )
        if len( keys ) < 5000:
            print u'- NOTE: accession_number_dict.json ONLY CONTAINS %s RECORDS' % len( keys )
        return keys

    def _make_final_accession_number_dict( self, bell_source_accession_numbers, solr_accnum_to_pid_dict ):
        """ Takes source accession-number list, and solr accession-number-to-pid dict
            Creates and returns an accession-number-to-pid dict from the source bell data.
            Example: { acc_num_1: bdr_123, acc_num_2: None } """
        final_accession_pid_dict = {}
        for accession_number in bell_source_accession_numbers:
            if accession_number in solr_accnum_to_pid_dict.keys():
                final_accession_pid_dict[accession_number] = solr_accnum_to_pid_dict[accession_number]
            else:
                final_accession_pid_dict[accession_number] = None
        # self.logger.info( u'in _make_final_accession_number_dict(); final_accession_pid_dict, %s' % pprint.pformat(final_accession_pid_dict) )
        return final_accession_pid_dict

    def _output_json( self, final_accession_pid_dict, output_json_path ):
        """ Saves to disk. """
        output_dict = {
            u'count': self.__run_output_counts( final_accession_pid_dict ),
            u'datetime': unicode( datetime.datetime.now() ),
            u'final_accession_pid_dict': final_accession_pid_dict }
        jstring = json.dumps( output_dict, sort_keys=True, indent=2 )
        with open( output_json_path, u'w' ) as f:
            f.write( jstring )
        return

    def __run_output_counts( self, final_accession_pid_dict ):
        """ Takes final dict.
            Creates and returns output counts dict.
            Called by self._output_json() """
        count_items = len( final_accession_pid_dict )
        count_pids = 0
        count_null = 0
        for (accession_number, pid) in final_accession_pid_dict.items():
            if pid == None:
                count_null += 1
            else:
                count_pids += 1
        return { u'count_items': count_items, u'count_pids': count_pids, u'count_null': count_null }

    def _print_settings( self, bdr_collection_pid, bell_dict_json_path, solr_root_url, output_json_path ):
        """ Prints variable values that are also sent to main controller function. """
        print u'- bdr_collection_pid: %s' % bdr_collection_pid
        print u'- bell_dict_json_path: %s' % bell_dict_json_path
        print u'- solr_root_url: %s' % solr_root_url
        print u'- output_json_path: %s' % output_json_path
        print u'---'
        return

    # end class PidFinder()




if __name__ == u'__main__':
    """ Assumes env is activated.
        ( 'ANTP' used as a namespace prefix for this 'acc_num_to_pid.py' file. ) """
    bdr_collection_pid=os.environ.get( u'BELL_ANTP__COLLECTION_PID' )
    bell_dict_json_path=os.environ.get( u'BELL_ANTP__BELL_DICT_JSON_PATH' )  # file of dict of bell-accession-number to metadata
    solr_root_url=os.environ.get( u'BELL_ANTP__SOLR_ROOT' )
    output_json_path=os.environ.get( u'BELL_ANTP__OUTPUT_JSON_PATH' )
    pid_finder = PidFinder()
    pid_finder._print_settings(
        bdr_collection_pid, bell_dict_json_path, solr_root_url, output_json_path )
    pid_finder.make_dict(
        bdr_collection_pid, bell_dict_json_path, solr_root_url, output_json_path )
