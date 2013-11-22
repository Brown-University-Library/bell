# -*- coding: utf-8 -*-

import json, pprint
import requests


class PidFinder( object ):

    def make_dict( self,
        bdr_collection_pid,
        bell_dict_json_path,
        fedora_risearch_url,
        output_json_path ):
        """ CONTROLLER.
            Returns accession-number dict showing bdr-pid & state.
            Example: { acc_num_1: {bdr:123, state:active}, acc_num_2: {bdr:234, state:active}, etc. }
            Gets pids from fedora, gets pids from solr, assesses any differences. """
        #Run itql query
        itql_query_output = self._run_itql_query( fedora_risearch_url, bdr_collection_pid )
        #Parse results to fedora-pid list
        fedora_pid_list = self._parse_itql_search_results( itql_query_output )
        #Run studio-solr query for solr-pid list
        studio_solr_pid_list = self._run_studio_solr_query( bdr_collection_pid )
        #Make fedora pid-dict... ( intersect for active/inactive ) { bdr123: active, bdr234: active }
        fedora_pid_dict = self.make_fedora_pid_dict( fedora_pid_list, studio_pid_dict )
        #Get accession numbers
        accession_numbers = sorted(json.loads(bell_dict_json_path).keys() )
        #Make accession-number-to-pidinfo-dict
        final_dict = self.make_accession_number_to_pid_dict( accession_numbers, fedora_pid_dict )
        #Output json
        pass

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
        collection_api_url = u'https://repository.library.brown.edu/api/pub/collections/%s/' % bdr_collection_pid
        r = requests.get( collection_api_url )
        d = json.loads( r.text )
        assert( sorted(d.keys()) ) == [ u'count', u'id', u'items' ]
        pid_list = []
        for entry in d[u'items'][u'docs']:  # entry: { u'pid': u'bdr:1234' }
            pid_list.append( entry[u'pid'] )
        sorted_solr_pids = sorted( pid_list )
        return sorted_solr_pids




if __name__ == u'__main__':
  import bell_ingest_settings as bs
  pid_finder = PidFinder()
  pid_finder.make_dict(
    bdr_collection_pid=bs.COLLECTION_PID,
    bell_dict_json_path=bs.BELL_DICT_JSON_PATH,
    fedora_risearch_url=bs.FEDORA_RISEARCH_URL,
    output_json_path=bs.OUTPUT_JSON_PATH
    )
