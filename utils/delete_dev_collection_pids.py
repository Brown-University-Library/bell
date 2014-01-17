# -*- coding: utf-8 -*-

""" Clears collection-pids from dev-fedora.
    Useful for testing full-ingestion script.
    Protections (ensuring dev): - checks for 'test' in collection-pid and pids-to-delete,
                                - ensures 'dev' in url env-setting
    Call from bell_code.
"""

import json, os, sys, time
import bell_logger, requests
from foundation.acc_num_to_pid import PidFinder
from redis import Redis
from rq import Queue


## get pids ##


def get_collection_pids():
    """ Gets list of pids to delete and puts each deletion-job on queue. """
    ( pid_finder, COLLECTION_PID, MEMBERSHIP_URL, DELETION_URL, queue_name,logger ) = _setup_getter_vars()
    logger.info( u'in delete_dev_collection_pids.get_collection_pids(); starting' )
    fedora_pid_list = _run_fedora_itql_query( queue_name, DELETION_URL )
    _populate_queue( fedora_pid_list )
    logger.info( u'in delete_dev_collection_pids.get_collection_pids(); done' )
    return


def _setup_getter_vars():
    """ Returns logger, and required parameters after setting and checking them. """
    ( COLLECTION_PID, MEMBERSHIP_URL, DELETION_URL, queue_name, logger ) = (
        unicode( os.environ.get(u'BELL_UTILS__COLLECTION_PID') ),
        unicode( os.environ.get(u'BELL_UTILS__FEDORA_RISEARCH_URL') ),
        unicode( os.environ.get(u'BELL_UTILS__FEDORA_ADMIN_URL') ),
        bell_logger.setup_logger() )
    assert u'test' in COLLECTION_PID, Exception( u'in delete_dev_collection_pids(); ERROR: problem validating collection-pid' )
    assert u'dev' in MEMBERSHIP_URL, Exception( u'in delete_dev_collection_pids(); ERROR: problem validating membership-url' )
    assert u'dev' in DELETION_URL, Exception( u'in delete_dev_collection_pids(); ERROR: problem validating deletion-url' )
    pid_finder = PidFinder()
    return ( pid_finder, COLLECTION_PID, MEMBERSHIP_URL, DELETION_URL, queue_name, logger )


def _run_fedora_itql_query( pid_finder, MEMBERSHIP_URL, COLLECTION_PID ):
    """ Returns list of pids from a fedora itql search. """
    itql_query_output = pid_finder._run_itql_query( fedora_risearch_url=MEMBERSHIP_URL, bdr_collection_pid=COLLECTION_PID )
    assert type( itql_query_output ) == list
    logger.info( u'in delete_dev_collection_pids(); len(itql_query_output): %s' % len(itql_query_output) )
    fedora_pid_list = pid_finder._parse_itql_search_results( itql_query_output )
    for pid in fedora_pid_list:
        assert u'test:' in pid, Exception( u'in delete_dev_collection_pids._run_fedora_itql_query(); ERROR: problem validating pid %s' % pid )
    return fedora_pid_list


def _populate_queue( fedora_pid_list ):
    """ Creates a queue deletion-job for each pid. """
    q = Queue( queue_name, connection=Redis() )
    for i, pid in enumerate( fedora_pid_list ):
        data = { u'pid': pid }
        q.enqueue_call ( func=u'utils.delete_dev_collection_pids.task__delete_item_from_fedora', args =(data,), timeout=30 )
        break
    logger.info( u'in delete_dev_collection_pids._populate_queue(); all deletion jobs put on queue.' )
    return


## delete them ##


def task__delete_item_from_fedora( data ):
    """ Deletes pid from dev-bdr.
        Task called via queue, which is populated by delete_dev_collection_pids.get_collection_pids(). """
    ( pid, fedora_url, fedora_username, fedora_password, logger ) = _setup_delete_vars( data )
    logger.info( u'in delete_dev_collection_pids.delete_item(); pid `%s`; starting' % data[u'pid'] )
    fedora_deletion_url = u'%s/%s?state=I' % ( fedora_url, pid )
    try:
        response = requests.put( fedora_deletion_url, auth=(fedora_username, fedora_password), verify=False )
        d = { u'pid': pid, u'response_status_code': response.status_code, u'response_reason': response.reason, u'response.content': response.content.decode(u'utf-8', u'replace') }
        logger.info( u'in delete_dev_collection_pids.delete_item_from_fedora(); result: %s' % unicode(repr(d)) )
    except Exception as e:
        logger.error( u'in delete_dev_collection_pids.delete_item_from_fedora(); ERROR: %s' % unicode(repr(e)) )
        raise Exception( u'ERROR deleting pid `%s` from fedora; logged' )
    return


def _setup_delete_vars( data ):
    """ Returns logger, and required parameters after setting and checking them. """
    ( pid, fedora_url, fedora_username, fedora_password, logger ) = (
        data[u'pid'],
        unicode( os.environ.get(u'BELL_UTILS__FEDORA_ADMIN_URL') ),
        unicode( os.environ.get(u'BELL_UTILS__FEDORA_ADMIN_USERNAME') ),
        unicode( os.environ.get(u'BELL_UTILS__FEDORA_ADMIN_PASSWORD') ),
        bell_logger.setup_logger() )
    check_list = [ pid, fedora_url, fedora_username, fedora_password ]
    for var in check_list:
        position = check_list.index(var)
        assert type( var ) == unicode, Exception( u'var at position `%s` is `%s`, not unicode' % (position, var) )
    return ( pid, fedora_url, fedora_username, fedora_password, logger )



if __name__ == u'__main__':
    get_collection_pids()  # will put deletion jobs on queue
    ## end ##
