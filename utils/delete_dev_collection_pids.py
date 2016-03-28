# -*- coding: utf-8 -*-

from __future__ import unicode_literals

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
    ( pid_finder, COLLECTION_PID, MEMBERSHIP_URL, QUEUE_NAME, logger ) = _setup_getter_vars()
    logger.info( 'in delete_dev_collection_pids.get_collection_pids(); starting' )
    fedora_pid_list = _run_fedora_itql_query( pid_finder, MEMBERSHIP_URL, COLLECTION_PID, logger )
    _populate_queue( fedora_pid_list, QUEUE_NAME, logger )
    logger.info( 'in delete_dev_collection_pids.get_collection_pids(); done' )
    return


def _setup_getter_vars():
    """ Returns logger, and required parameters after setting and checking them. """
    ( COLLECTION_PID, MEMBERSHIP_URL, QUEUE_NAME ) = (
        unicode( os.environ.get('BELL_UTILS__COLLECTION_PID') ),
        unicode( os.environ.get('BELL_UTILS__FEDORA_RISEARCH_URL') ),
        unicode( os.environ.get('BELL_QUEUE_NAME') )
        )
    assert 'test' in COLLECTION_PID, Exception( 'in delete_dev_collection_pids(); ERROR: problem validating collection-pid' )
    assert 'dev' in MEMBERSHIP_URL, Exception( 'in delete_dev_collection_pids(); ERROR: problem validating membership-url' )
    pid_finder = PidFinder()
    logger = bell_logger.setup_logger()
    return ( pid_finder, COLLECTION_PID, MEMBERSHIP_URL, QUEUE_NAME, logger )


def _run_fedora_itql_query( pid_finder, MEMBERSHIP_URL, COLLECTION_PID, logger ):
    """ Returns list of pids from a fedora itql search. """
    itql_query_output = pid_finder._run_itql_query( fedora_risearch_url=MEMBERSHIP_URL, bdr_collection_pid=COLLECTION_PID )
    assert type( itql_query_output ) == list
    logger.info( 'in delete_dev_collection_pids(); len(itql_query_output): %s' % len(itql_query_output) )
    fedora_pid_list = pid_finder._parse_itql_search_results( itql_query_output )
    for pid in fedora_pid_list:
        assert 'test:' in pid, Exception( 'in delete_dev_collection_pids._run_fedora_itql_query(); ERROR: problem validating pid %s' % pid )
    return fedora_pid_list


def _populate_queue( fedora_pid_list, QUEUE_NAME, logger ):
    """ Creates a queue deletion-job for each pid. """
    q = Queue( QUEUE_NAME, connection=Redis() )
    for i, pid in enumerate( fedora_pid_list ):
        data = { 'pid': pid }
        q.enqueue_call ( func='utils.delete_dev_collection_pids.task__delete_item_from_fedora', args =(data,), timeout=30 )
        break
    logger.info( 'in delete_dev_collection_pids._populate_queue(); all deletion jobs put on queue.' )
    return


## delete them ##


def task__delete_item_from_fedora( data ):
    """ Deletes pid from dev-bdr.
        Task called via queue, which is populated by delete_dev_collection_pids.get_collection_pids(). """
    ( pid, fedora_url, fedora_username, fedora_password, logger ) = _setup_delete_vars( data )
    logger.info( 'in delete_dev_collection_pids.delete_item(); pid `%s`; starting' % data['pid'] )
    fedora_deletion_url = '%s/%s?state=D' % ( fedora_url, pid )
    logger.info( 'in delete_dev_collection_pids.delete_item_from_fedora(); fedora_deletion_url: %s' % fedora_deletion_url )
    try:
        response = requests.put( fedora_deletion_url, auth=(fedora_username, fedora_password), verify=False )
        d = { 'pid': pid, 'response_status_code': response.status_code, 'response_reason': response.reason, 'response.content': response.content.decode('utf-8', 'replace') }
        logger.info( 'in delete_dev_collection_pids.delete_item_from_fedora(); result: %s' % unicode(repr(d)) )
    except Exception as e:
        logger.error( 'in delete_dev_collection_pids.delete_item_from_fedora(); ERROR: %s' % unicode(repr(e)) )
        raise Exception( 'ERROR deleting pid `%s` from fedora; logged' )
    return


def _setup_delete_vars( data ):
    """ Returns logger, and required parameters after setting and checking them. """
    ( pid, fedora_url, fedora_username, fedora_password ) = (
        data['pid'],
        unicode( os.environ.get('BELL_UTILS__FEDORA_ADMIN_URL') ),
        unicode( os.environ.get('BELL_UTILS__FEDORA_ADMIN_USERNAME') ),
        unicode( os.environ.get('BELL_UTILS__FEDORA_ADMIN_PASSWORD') ) )
    check_list = [ pid, fedora_url, fedora_username, fedora_password ]
    for var in check_list:
        position = check_list.index(var)
        assert type( var ) == unicode, Exception( 'var at position `%s` is `%s`, not unicode' % (position, var) )
    logger = bell_logger.setup_logger()
    return ( pid, fedora_url, fedora_username, fedora_password, logger )



if __name__ == '__main__':
    get_collection_pids()  # will put deletion jobs on queue
    ## end ##
