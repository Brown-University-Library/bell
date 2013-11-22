# -*- coding: utf-8 -*-

import json


class PidFinder( object ):

    def make_dict( self,
        bdr_collection_pid,
        bell_dict_json_path,
        fedora_risearch_url,
        output_json_path,
        studio_solr_root_url ):
        """ Controller.
            Returns accession-number dict showing bdr-pid & title.
            Example: { acc_num_1: {pid:123, state:active, title:cool_pic}, acc... etc. }
            Gets collection members from fedora, checks studio-solr for info. """
        #Run itql query
        itql_query_output = self.run_itql_query( fedora_risearch_url, bdr_collection_pid )
        #Parse results to fedora-pid list
        fedora_pid_list = self.parse_itql_search_results( itql_query_output )
        #Run studio-solr query
        studio_solr_output = self.run_studio_solr_query( studio_solr_root_url, bdr_collection_pid )
        #Parse results to solr-pid: title
        studio_pid_dict = self.parse_studio_solr_results( studio_solr_output )
        #Make fedora pid-dict...
        fedora_pid_dict = self.make_fedora_pid_dict( fedora_pid_list, studio_pid_dict )
        #Get accession numbers
        accession_numbers = sorted(json.loads(bell_dict_json_path).keys() )
        #Make accession-number-to-pidinfo-dict
        final_dict = self.make_accession_number_to_pid_dict( accession_numbers, fedora_pid_dict )
        #Output json
        pass


    def make_fedora_pid_dict( self, fedora_pid_list, studio_pid_dict ):
        """ Returns pid-dict
            Example: { pid1: {'state': 'active', 'title': 'abc'}, pid2: {etc} } """
        fedora_pid_dict = {}
        for fedora_pid in fedora_pid_list:
            #Set 'state' and 'title'
            if fedora_pid in studio_pid_dict.keys():
                final_dict[fedora_pid] = { u'state': u'active', u'title': studio_pid_dict[fedora_pid] }
            else:
                final_dict[fedora_pid] = { u'state': u'inactive', u'title': None }



        ## setup
        bh = BellHelper()
        output = {
          u'count': 0,
          u'accession_numbers': {}  # u'accession_number': { u'title': u'the_title', u'pid': u'the_pid' }
          }
        ## get collection children list
        unparsed_child_pids = bh.run_collection_member_search( self.FEDORA_RISEARCH_URL , self.COLLECTION_PID )
        ## parse list
        cleaned_child_pids = bh.parse_itql_search_results( unparsed_child_pids )
        # print u'- cleaned_child_pids...'; pprint.pprint( cleaned_child_pids )
        ## save
        t = Task.objects.get( name=u'list_previously_ingested_pids', project=u'BELL201305' )
        t.output = json.dumps( sorted(cleaned_child_pids), indent=2, sort_keys=True )
        t.save()
        return
        # end def list_previously_ingested_pids()


      # def run_collection_member_search( self, risearch_url, collection_pid ):
      #   """ Runs an itql query to get a list of member objects.
      #       Called by bell_2013_05.build_ingested_list() """
      #   itql_query = u'select $object from <#ri> where $object <fedora-rels-ext:isMemberOf> <info:fedora/%s>' % collection_pid
      #   params = {
      #     u'lang': u'itql',
      #     u'format': u'json',
      #     u'query': itql_query,
      #     }
      #   try:
      #     r = requests.get( risearch_url, params=params, verify=False )
      #     d = json.loads( r.text )
      #     assert d.keys() == [ u'results' ]
      #     results_list = d[u'results']
      #     return results_list
      #   except Exception, e:
      #     message = u'problem running itql search: %s' % repr(e).decode(u'utf-8', u'replace')
      #     log.error( message )
      #     raise Exception( message )




# def list_pids( self ):
# """ Returns accession-number dict showing bdr-pid & title.
#     Gets collection members from fedora, checks studio-solr for info. """
#     assert type(self.FEDORA_RISEARCH_URL) == unicode and len(self.FEDORA_RISEARCH_URL) > 0
#     assert type(self.COLLECTION_PID) == unicode and len(self.COLLECTION_PID) > 0
#     ## setup
#     bh = BellHelper()
#     output = {
#       u'count': 0,
#       u'accession_numbers': {}  # u'accession_number': { u'title': u'the_title', u'pid': u'the_pid' }
#       }
#     ## get collection children list
#     unparsed_child_pids = bh.run_collection_member_search( self.FEDORA_RISEARCH_URL , self.COLLECTION_PID )
#     ## parse list
#     cleaned_child_pids = bh.parse_itql_search_results( unparsed_child_pids )
#     # print u'- cleaned_child_pids...'; pprint.pprint( cleaned_child_pids )
#     ## save
#     t = Task.objects.get( name=u'list_previously_ingested_pids', project=u'BELL201305' )
#     t.output = json.dumps( sorted(cleaned_child_pids), indent=2, sort_keys=True )
#     t.save()
#     return
#     # end def list_previously_ingested_pids()




if __name__ == u'__main__':
  import bell_ingest_settings as bs
  pid_finder = PidFinder()
  pid_finder.make_dict(
    bdr_collection_pid=bs.COLLECTION_PID,
    bell_dict_json_path=bs.BELL_DICT_JSON_PATH,
    fedora_risearch_url=bs.FEDORA_RISEARCH_URL,
    output_json_path=bs.OUTPUT_JSON_PATH,
    studio_solr_root_url=bs.STUDIO_SOLR_ROOT_URL
    )
