# -*- coding: utf-8 -*-

""" Clears collection-pids from dev-fedora.
    Useful for testing full-ingestion script.
    Protections (ensuring dev): - checks for 'test' in collection-pid and pids-to-delete,
                                - ensures 'dev' in url env-setting
    Call from bell_code.
"""

import json, os, sys, time
import bell_logger
import requests
from foundation.acc_num_to_pid import PidFinder
from redis import Redis
from rq import Queue


def delete_item_from_fedora( data ):
    """ Deletes pid from dev-bdr.
        Task called via queue. """
    logger = bell_logger.setup_logger()
    logger.info( u'in delete_dev_collection_pids.delete_item(); pid `%s`; starting' % data[u'pid'] )
    ( pid, fedora_url, fedora_username, fedora_password ) = (
        data[u'pid'],
        data[u'fedora_url'],
        unicode(os.environ.get(u'BELL_UTILS__FEDORA_ADMIN_USERNAME')),
        unicode(os.environ.get(u'BELL_UTILS__FEDORA_ADMIN_PASSWORD'))
        )
    check_list = [ pid, fedora_url, fedora_username, fedora_password ]
    for var in check_list:
        position = check_list.index(var)
        assert type( var ) == unicode, Exception( u'var at position `%s` is `%s`, not unicode' % (position, var) )
    fedora_deletion_url = u'%s/%s?state=I' % ( fedora_url, pid )
    try:
        response = requests.put( fedora_deletion_url, auth=(fedora_username, fedora_password), verify=False )
        d = { u'pid': pid, u'response_status_code': response.status_code, u'response_reason': response.reason, u'response.content': response.content.decode(u'utf-8', u'replace') }
        logger.info( u'in delete_dev_collection_pids.delete_item_from_fedora(); result: %s' % unicode(repr(d)) )
    except Exception as e:
        logger.error( u'in delete_dev_collection_pids.delete_item_from_fedora(); ERROR: %s' % unicode(repr(e)) )
        raise Exception( u'ERROR deleting pid `%s` from fedora; logged' )
    return


if __name__ == u'__main__':

    """ Gets list of pids to delete and puts each deletion-job on queueu. """

    ## set up logger
    logger = bell_logger.setup_logger()
    logger.info( u'in delete_dev_collection_pids(); starting' )

    ## settings
    COLLECTION_PID = unicode( os.environ.get(u'BELL_UTILS__COLLECTION_PID') )
    MEMBERSHIP_URL = unicode( os.environ.get(u'BELL_UTILS__FEDORA_RISEARCH_URL') )
    DELETION_URL = unicode( os.environ.get(u'BELL_UTILS__FEDORA_ADMIN_URL') )

    ## protection-checks
    try:
        assert u'test' in COLLECTION_PID, Exception( u'in delete_dev_collection_pids(); ERROR: problem validating collection-pid' )
        assert u'dev' in MEMBERSHIP_URL, Exception( u'in delete_dev_collection_pids(); ERROR: problem validating membership-url' )
        assert u'dev' in DELETION_URL, Exception( u'in delete_dev_collection_pids(); ERROR: problem validating deletion-url' )
        logger.info( u'in delete_dev_collection_pids(); protection-checks passed' )
    except Exception as e:
        logger.error( u'in delete_dev_collection_pids(); ERROR: %s' % unicode(repr(e)) )
        raise Exception( u'ERROR in protection-checks logged' )
    def validate_pid( pid ):
        """ Checks each pid before it's deleted. """
        assert u'test:' in pid, Exception( u'in delete_dev_collection_pids(); ERROR: problem validating pid %s' % pid )
        return

    ## get list of collection pids
    pid_finder = PidFinder()
    itql_query_output = pid_finder._run_itql_query( fedora_risearch_url=MEMBERSHIP_URL, bdr_collection_pid=COLLECTION_PID )
    assert type( itql_query_output ) == list
    logger.info( u'in delete_dev_collection_pids(); len(itql_query_output): %s' % len(itql_query_output) )
    fedora_pid_list = pid_finder._parse_itql_search_results( itql_query_output )
    logger.info( u'in delete_dev_collection_pids(); len(fedora_pid_list): %s' % len(fedora_pid_list) )

    ## validate each pid
    for pid in fedora_pid_list:
        validate_pid( pid )

    ## delete each pid
    queue_name = os.environ.get(u'BELL_QUEUE_NAME')
    q = Queue( queue_name, connection=Redis() )
    for i, pid in enumerate( fedora_pid_list ):
        data = { u'pid': pid, u'fedora_url': DELETION_URL }
        q.enqueue_call ( func=u'utils.delete_dev_collection_pids.delete_item_from_fedora', args =(data,), timeout=30 )
        break
    logger.info( u'in delete_dev_collection_pids(); all deletion jobs put on queue.' )

    ## end ##
