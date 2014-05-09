# -*- coding: utf-8 -*-

""" Reindexes a list of pids.
    Iterates through them, updating custom index.
        - Checks:
            - accession number in correct place.
            - bell_meta datastream exists.
            - (future) existence of both jp2 and master if either exists
            - (future) initial stream check on jp2 and master image
    If list contains string 'all',
        - all bell collection pids will be reindexed (custom index).
        - any custom-index entries not in full-pid-list will be removed. """

import argparse, json, os, pprint, sys, time
import redis, requests, rq
from bell_code import bell_logger
from bell_code.tasks.indexer import Indexer


queue_name = unicode( os.environ.get(u'BELL_QUEUE_NAME') )
q = rq.Queue( queue_name, connection=redis.Redis() )
r = redis.StrictRedis( host='localhost', port=6379, db=0 )


def parse_args():
    """ Parses arguments when module called via __main__. """
    parser = argparse.ArgumentParser( description=u'Required: list of pids.' )
    parser.add_argument( u'--pids', u'-p', help=u'pid-list in json format, eg \'[ "foo", "bar" ]\'', required=True )
    args_dict = vars( parser.parse_args() )
    return args_dict


class Reindexer( object ):

    def __init__( self, kwargs ):
        self.logger = bell_logger.setup_logger()
        self.solr_root_url = kwargs['solr_root_url']
        # print u'- in Reindexer.__init_(); kwargs, `%s`' % kwargs
        # print u'- in Reindexer.__init_(); self.solr_root_url, `%s`' % self.solr_root_url

    def make_jobs( self, pid_list ):
        print u'- in Reindexer.make_jobs(); pid_list...'; pprint.pprint( pid_list )
        for pid in pid_list:
            q.enqueue_call( func=u'bell_code.utils.update_custom_index.run_reindexer', kwargs={u'pid': pid}, timeout=600 )

    def reindex( self, pid ):
        ## access api
        api_root_url = unicode( os.environ[u'BELL_UCI__BDR_API_ROOT_URL'] )
        api_url = u'%s/items/%s/' % ( api_root_url, pid )
        # self.logger.debug( u'- in Reindexer.reindex(); api_url, %s' % api_url )
        r = requests.get( api_url )
        # self.logger.debug( u'- in Reindexer.reindex(); partial response, %s' % r.content.decode(u'utf-8')[0:100] )
        ## validate accession_number
        ## validate bell_metadata
        ## get source data
        all_dict = r.json()
        # self.logger.debug( u'- in Reindexer.reindex(); sorted(all_dict.keys()), %s' % sorted(all_dict.keys()) )
        bell_data_url = all_dict[u'links'][u'content_datastreams'][u'bell_metadata']
        self.logger.debug( u'- in Reindexer.reindex(); bell_data_url, %s' % bell_data_url )
        r2 = requests.get( bell_data_url )
        bell_data_dict = r2.json()
        ## build initial post-dict
        bell_indexer = Indexer( self.logger )
        initial_solr_dict = bell_indexer.build_metadata_only_solr_dict( pid, bell_data_dict )
        # self.logger.debug( u'- in Reindexer.reindex(); initial_solr_dict, %s' % pprint.pformat(initial_solr_dict) )
        ## update image data
        links_dict = all_dict[u'links']
        updated_solr_dict = bell_indexer.add_image_metadata( initial_solr_dict, links_dict )
        self.logger.debug( u'- in Reindexer.reindex(); updated_solr_dict, %s' % pprint.pformat(updated_solr_dict) )
        ## validate dict
        validity = bell_indexer.validate_solr_dict( updated_solr_dict )
        self.logger.debug( u'- in Reindexer.reindex(); validity for pid `%s`, %s' % (pid, validity) )
        ## update custom-solr
        if validity:
            post_result = bell_indexer.post_to_solr( updated_solr_dict )
            self.logger.debug( u'- in Reindexer.reindex(); post_result for pid `%s`, %s' % (pid, post_result) )

    # end class Reindexer()


def run_reindexer( pid ):
    """ Caller for reindex function.
        Called by rq job. """
    # print u'- in run_reindexer(); pid is, `%s`' % pid
    reindexer = Reindexer( kwargs={u'solr_root_url': os.environ.get(u'BELL_I_SOLR_ROOT')} )
    reindexer.reindex( pid )




if __name__ == u'__main__':
    """ Assumes env is activated.
        ( 'UCI' used as a namespace prefix for this 'updated_custom_index.py' file. ) """
    reindexer = Reindexer( kwargs={u'solr_root_url': os.environ.get(u'BELL_I_SOLR_ROOT')} )
    args = parse_args()
    pid_list = json.loads( args[u'pids'] )
    if pid_list == [u'all']:  # get all collection pids
        kwarg_data = { u'bdr_collection_pid': os.environ.get(u'BELL_UCI__COLLECTION_PID') }
        pid_list = reindexer.grab_all_collection_pids( kwargs=kwarg_data )
    reindexer.make_jobs( pid_list )
