# -*- coding: utf-8 -*-

from __future__ import unicode_literals

""" Reindexes -- in the bell custom index -- a list of pids.
    Iterates through them, updating the custom index.
        - Checks:
            - accession number in correct place.
            - bell_meta datastream exists.
            - (future) existence of both jp2 and master if either exists
            - (future) initial stream check on jp2 and master image
    If list contains string 'all',
        - all bell collection pids will be reindexed (in custom index).
        - any custom-index entries not in full-pid-list will be removed.
    Notes:
        - Assumes env is activated.
        - 'UCI' used as a namespace prefix for this 'update_custom_index.py' file.
    See `if __name__ == '__main__'` for examples of how to call this code. """

import argparse, json, os, pprint, sys, time
import redis, requests, rq
from bell_code import bell_logger
from bell_code.tasks.indexer import Indexer


queue_name = unicode( os.environ.get('BELL_QUEUE_NAME') )
q = rq.Queue( queue_name, connection=redis.Redis() )
r = redis.StrictRedis( host='localhost', port=6379, db=0 )


def parse_args():
    """ Parses arguments when module called via __main__. """
    parser = argparse.ArgumentParser( description='Required: list of pids.' )
    parser.add_argument( '--pids', '-p', help='pid-list in json format, eg \'[ "foo", "bar" ]\'', required=True )
    args_dict = vars( parser.parse_args() )
    return args_dict


class Reindexer( object ):

    def __init__( self, kwargs ):
        self.logger = bell_logger.setup_logger()
        self.solr_root_url = kwargs['solr_root_url']  # CUSTOM-solr index; TODO: rename so it's not confused with bdr-solr
        # print '- in Reindexer.__init_(); kwargs, `%s`' % kwargs
        # print '- in Reindexer.__init_(); self.solr_root_url, `%s`' % self.solr_root_url

    def make_jobs( self, pid_list ):
        print '- in Reindexer.make_jobs(); pid_list...'; pprint.pprint( pid_list )
        for pid in pid_list:
            q.enqueue_call( func='bell_code.utils.update_custom_index.run_reindexer', kwargs={'pid': pid}, timeout=600 )

    def reindex( self, pid ):
        ## access api
        api_root_url = unicode( os.environ['BELL_UCI__BDR_API_ROOT_URL'] )
        api_url = '%s/items/%s/' % ( api_root_url, pid )
        # self.logger.debug( '- in Reindexer.reindex(); api_url, %s' % api_url )
        r = requests.get( api_url )
        # self.logger.debug( '- in Reindexer.reindex(); partial response, %s' % r.content.decode('utf-8')[0:100] )
        ## validate accession_number
        ## validate bell_metadata
        ## get source data
        all_dict = r.json()
        # self.logger.debug( '- in Reindexer.reindex(); sorted(all_dict.keys()), %s' % sorted(all_dict.keys()) )
        bell_data_url = all_dict['links']['content_datastreams']['bell_metadata']
        self.logger.debug( '- in Reindexer.reindex(); bell_data_url, %s' % bell_data_url )
        r2 = requests.get( bell_data_url )
        bell_data_dict = r2.json()
        ## build initial post-dict
        bell_indexer = Indexer( self.logger )
        initial_solr_dict = bell_indexer.build_metadata_only_solr_dict( pid, bell_data_dict )
        # self.logger.debug( '- in Reindexer.reindex(); initial_solr_dict, %s' % pprint.pformat(initial_solr_dict) )
        ## update image data
        links_dict = all_dict['links']
        updated_solr_dict = bell_indexer.add_image_metadata( initial_solr_dict, links_dict )
        self.logger.debug( '- in Reindexer.reindex(); updated_solr_dict, %s' % pprint.pformat(updated_solr_dict) )
        ## validate dict
        validity = bell_indexer.validate_solr_dict( updated_solr_dict )
        self.logger.debug( '- in Reindexer.reindex(); validity for pid `%s`, %s' % (pid, validity) )
        ## update custom-solr
        if validity:
            post_result = bell_indexer.post_to_solr( updated_solr_dict )
            self.logger.debug( '- in Reindexer.reindex(); post_result for pid `%s`, %s' % (pid, post_result) )

    def grab_all_pids( self, collection_pid ):
        """ Creates a pid list from the given collection-pid.
            Called when __main__ detects the pid-list ['all']  """
        pid_list = []
        for i in range( 100 ):  # would handle 50,000 records
            data_dict = self._query_solr( i, collection_pid )
            docs = data_dict['response']['docs']
            for doc in docs:
                pid = doc['pid']
                pid_list.append( pid )
            if not len( docs ) > 0:
                break
        self.logger.debug( 'in utils.update_custom_index.grab_all_pids(); doc_list, %s' % pprint.pformat(pid_list[0:10]) )
        return pid_list

    def _query_solr( self, i, collection_pid ):
        """ Queries solr for iterating start-row.
            Helper for grab_all_pids()
            Returns results dict.
            Called by self._run_studio_solr_query() """
        search_api_root_url = '%s/search/' % unicode( os.environ['BELL_UCI__BDR_API_ROOT_URL'] )
        time.sleep( .5 )
        new_start = i * 500  # for solr start=i parameter (cool, eh?)
        params = {
            'q': 'rel_is_member_of_ssim:"%s"' % collection_pid,
            'fl': 'pid,mods_id_bell_accession_number_ssim,primary_title',
            'rows': 500, 'start': new_start, 'wt': 'json' }
        r = requests.get( search_api_root_url, params=params, verify=False )
        self.logger.info( 'in utils.update_custom_index._query_solr(); r.url, %s' % r.url )
        data_dict = json.loads( r.content.decode('utf-8', 'replace') )
        # self.logger.info( 'in __query_solr(); data_dict, %s' % pprint.pformat(data_dict) )
        return data_dict

    # end class Reindexer()


def run_reindexer( pid ):
    """ Caller for reindex function.
        Called by rq job. """
    # print '- in run_reindexer(); pid is, `%s`' % pid
    reindexer = Reindexer( kwargs={'solr_root_url': os.environ.get('BELL_I_SOLR_ROOT')} )
    reindexer.reindex( pid )




if __name__ == '__main__':
    """ Called manually.
        Note: pids string must be json.
        Example call for single pid:
            $ python ./utils/update_custom_index.py --pids '["abc"]'
        Example call for multiple pids:
            $ python ./utils/update_custom_index.py --pids '["abc", "def"]'
        Call to process _all_ pids:
            $ python ./utils/update_custom_index.py --pids '["all"]' """
    reindexer = Reindexer( kwargs={'solr_root_url': unicode(os.environ.get('BELL_I_SOLR_ROOT'))} )
    args = parse_args()
    pid_list = json.loads( args['pids'] )
    if pid_list == ['all']:  # get all collection pids
        pid_list = reindexer.grab_all_pids( unicode(os.environ.get('BELL_UCI__COLLECTION_PID')) )
    reindexer.make_jobs( pid_list )
