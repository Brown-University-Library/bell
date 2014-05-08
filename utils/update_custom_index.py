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
        print u'- in Reindexer.__init_(); self.solr_root_url, `%s`' % self.solr_root_url

    def make_jobs( self, pid_list ):
        print u'- in Reindexer.make_jobs(); pid_list...'; pprint.pprint( pid_list )
        for pid in pid_list:
            q.enqueue_call( func=u'bell_code.utils.update_custom_index.run_reindexer', kwargs={u'pid': pid}, timeout=600 )

    def reindex( self, pid ):
        print u'- in Reindexer.reindex(); pid is, `%s`' % pid
        ## access api
        ## validate accession_number
        ## validate bell_metadata
        ## build post-dict
        ## update custom-solr

    # end class Reindexer()


def run_reindexer( pid ):
    """ Caller for reindex function.
        Called by rq job. """
    print u'- in run_reindexer(); pid is, `%s`' % pid
    reindexer = Reindexer( kwargs={u'solr_root_url': os.environ.get(u'BELL_UCI__SOLR_ROOT')} )
    reindexer.reindex( pid )




if __name__ == u'__main__':
    """ Assumes env is activated.
        ( 'UCI' used as a namespace prefix for this 'updated_custom_index.py' file. ) """
    reindexer = Reindexer( kwargs={u'solr_root_url': os.environ.get(u'BELL_UCI__SOLR_ROOT')} )
    args = parse_args()
    pid_list = json.loads( args[u'pids'] )
    if pid_list == [u'all']:  # get all collection pids
        pid_list = reindexer.grab_all_collection_pids( kwargs={u'bdr_collection_pid': os.environ.get(u'BELL_UCI__COLLECTION_PID')} )
    reindexer.make_jobs( pid_list )
