# -*- coding: utf-8 -*-

import json, pprint, time
import requests


class PidFinder( object ):

    def make_dict( self,
        bdr_collection_pid,
        bell_dict_json_path,
        fedora_risearch_url,
        output_json_path ):
        """ CONTROLLER.
            Returns accession-number dict showing bdr-pid & state.
            Example: { acc_num_1: {pid:bdr_123, state:active}, acc_num_2: {pid:None, state:None}, etc. }
            Gets pids from fedora, gets pids from solr, assesses any differences. """
        #Run itql query -- [ {'object': 'info:fedora/bdr:1234'}, {'object': 'info:fedora/bdr:5678'} ]
        itql_query_output = self._run_itql_query( fedora_risearch_url, bdr_collection_pid )
        #Parse results to fedora-pid list -- [ 'bdr:1234', 'bdr:5678' ]
        fedora_pid_list = self._parse_itql_search_results( itql_query_output )
        #Run studio-solr query for solr-pid list -- [ 'bdr:1', bdr:2' ]
        studio_solr_pid_list = self._run_studio_solr_query( bdr_collection_pid )
        print u'- studio_solr_pid_list...'; pprint.pprint(studio_solr_pid_list)
        #Make intersection pid-dict -- { bdr123: active, bdr234: active }
        intersection_pid_dict = self._make_intersection_pid_dict( fedora_pid_list, studio_solr_pid_list )
        # pprint.pprint( intersection_pid_dict )
        #Assign accession-numbers from bdr -- { acc_num_1: {pid:bdr_123, state:active}, acc_num_3: {pid:bdr_234, state:active}, etc. }
        initial_accession_dict = self._assign_bdr_accession_numbers( bdr_collection_pid, intersection_pid_dict )
        #Get _bell_ accession numbers -- [ 'acc_num_1', 'acc_num_2', etc. ]
        accession_numbers = self._load_bell_accession_numbers( bell_dict_json_path )
        #Make final accession-number dict -- { acc_num_1: {pid:bdr_123, state:active}, acc_num_2: {pid:None, state:None}, etc. }
        final_accesion_dict = self._make_final_accession_number_dict( accession_numbers, initial_accession_dict )
        #Output json
        print u'done'

    def _run_itql_query( self, fedora_risearch_url, bdr_collection_pid ):
        """ Returns output from fedora itql resource-index search. """
        itql_query = u'select $object from <#ri> where $object <fedora-rels-ext:isMemberOf> <info:fedora/%s>' % bdr_collection_pid
        params = { u'lang': u'itql', u'format': u'json', u'query': itql_query }
        try:
          r = requests.get( fedora_risearch_url, params=params, verify=False )
          d = json.loads( r.text )
          assert d.keys() == [ u'results' ]
          results_list = d[u'results']
          return results_list
        except Exception, e:
          message = u'problem running itql search: %s' % repr(e).decode(u'utf-8', u'replace')
          raise Exception( message )

    def _parse_itql_search_results(self, itql_query_output ):
        """ Returns list of pids from _fedora_. """
        assert type( itql_query_output ) == list
        parsed_pids = []
        for entry in itql_query_output:  # eg entry: { u'object': u'info:fedora/bdr:1234' }
          if entry[u'object'][0:4] == u'info':
            parsed_pids.append( entry[u'object'].split(u'/')[1] )
        sorted_fedora_pids = sorted( parsed_pids )
        return sorted_fedora_pids

    def _run_studio_solr_query( self, bdr_collection_pid ):
        """ Returns list of pids from _solr_. """
        def _set_params( bdr_collection_pid, new_start ):
            return {
                u'q': u'rel_is_member_of_ssim:"%s"' % bdr_collection_pid,
                u'fl': u'pid',
                u'rows': 500,
                u'start': new_start,
                u'wt': u'json'
                }
        def _query_solr( i, bdr_collection_pid ):
            search_api_url = u'https://repository.library.brown.edu/api/pub/search/'
            new_start = i * 500  # for solr start=i parameter (cool, eh?)
            params = _set_params( bdr_collection_pid, new_start )
            r = requests.get( search_api_url, params=params, verify=False )
            data_dict = json.loads( r.content.decode(u'utf-8', u'replace') )
            time.sleep( .1 )
            return data_dict
        ## work
        all_pids = []
        for i in range( 100 ):  # would handle 50,000 records; loop actually only run 11 times as of Nov-2013
            data_dict = _query_solr( i, bdr_collection_pid )
            docs = data_dict[u'response'][u'docs']
            for doc in docs:
                all_pids.append( doc[u'pid'] )
            if not len( docs ) > 0:
                break
        sorted_pids = sorted( all_pids )
        return sorted_pids

    def _make_intersection_pid_dict( self, fedora_pid_list, studio_solr_pid_list ):
        """ Returns accession-number dict showing active/inactive status
            Example: { 'bdr123': 'active', 'bdr234': 'inactive' } """
        set_fedora_pids, set_solr_pids = set( fedora_pid_list ), set( studio_solr_pid_list )
        only_in_fedora, only_in_solr = list( set_fedora_pids - set_solr_pids ), list( set_solr_pids - set_fedora_pids )
        print u'- len(fedora_pid_list)...'; print len(fedora_pid_list)
        print u'- len(studio_solr_pid_list)...'; print len(studio_solr_pid_list)
        print u'- only_in_fedora...'; pprint.pprint( only_in_fedora )
        if len( only_in_solr ) > 0:
            print u'WARNING: THE FOLLOWING PIDS WERE FOUND IN SOLR THAT ARE NOT IN FEDORA...'; pprint.pprint( sorted(only_in_solr) )
        intersection_dict = {}
        for entry in fedora_pid_list:
            if entry in only_in_fedora:
                intersection_dict[entry] = u'inactive'
            else:
                intersection_dict[entry] = u'active'
        return intersection_dict

    def _assign_bdr_accession_numbers( self, bdr_collection_pid, intersection_pid_dict ):
        """ Queries bdr_solr for all items with bdr_collection_pid; returns initial accession-number dict.
            Example: { acc_num_1: {pid:bdr_123, state:active}, acc_num_3: {pid:bdr_234, state:active}, etc. } """
        print u'TODO: _assign_bdr_accession_numbers()'
        pass

    def _load_bell_accession_numbers( self, bell_dict_json_path ):
        """ Returns sorted accession-number keys list from bell-json-dict.
            Example: [ 'acc_num_1', 'acc_num_2', etc. ] """
        with open( bell_dict_json_path ) as f:
            accession_dict = json.loads( f.read() )
        keys = sorted( accession_dict.keys() )
        # pprint.pprint( keys )
        if len( keys ) < 5000:
            print u'- NOTE: accession_number_dict.json ONLY CONTAINS %s RECORDS' % len( keys )
        return keys

    def _make_final_accession_number_dict( self, accession_numbers, fedora_pid_dict ):
        """ Adds accession-numbers with no bdr info; returns final accession-number dict.
            Example: { acc_num_1: {pid:bdr_123, state:active}, acc_num_2: {pid:None, state:None}, etc. } """
        print u'TODO: _make_final_accession_number_dict()'
        pass




if __name__ == u'__main__':
  import bell_ingest_settings as bs
  pid_finder = PidFinder()
  pid_finder.make_dict(
    bdr_collection_pid=bs.COLLECTION_PID,
    bell_dict_json_path=bs.BELL_DICT_JSON_PATH,  # file of dict of bell-accession-number to metadata
    fedora_risearch_url=bs.FEDORA_RISEARCH_URL,
    output_json_path=bs.OUTPUT_JSON_PATH
    )
