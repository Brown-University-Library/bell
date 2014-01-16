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


def delete_item( data ):
    """ Deletes pid from dev-bdr. """
    logger = bell_logger.setup_logger()
    logger.info( u'in delete_dev_collection_pids.delete_item(); pid `%s`; starting' % data[u'pid'] )
    ( pid, url, auth_id, auth_key ) = ( data[u'pid'], data[u'deletion_url'],
                                        unicode(os.environ.get(u'BELL_UTILS__DELETION_API_AUTHID')), unicode(os.environ.get(u'BELL_UTILS__DELETION_API_AUTHKEY')) )
    check_list = [ pid, url, auth_id, auth_key ]
    for var in check_list:
        position = check_list.index(var)
        assert type( var ) == unicode, Exception( u'var at position `%s` is `%s`, not unicode' % (position, var) )
    payload = {
      u'identity': auth_id, u'authorization_code': auth_key, u'pid': pid }
    r = requests.delete( url, data=payload, verify=False )
    print r.status_code
    print r.content
    jdict = json.loads( r.content.decode(u'utf-8', u'replace') )
    print u'- jdict'; pprint.pprint( jdict )
    assert sorted(jdict.keys()) == [u'info', u'request', u'response'], Exception( u'unexpected jdict.keys(); they are: %s' % sorted(jdict.keys()) )
    assert jdict[u'response'].keys() == [ u'status' ]
    logger.info( u'in delete_dev_collection_pids.delete_item(); about to delete pid: %s' % pid)
    return { u'status': jdict[u'response'][u'status'] }


# def deleteItem( pid, identifier ):
#   try:
#     var_list = [ pid, identifier, settings.ITEM_API_URL, settings.ITEM_API_IDENTITY, settings.ITEM_API_KEY ]
#     for v in var_list:
#       assert type(v) == unicode, u'- type %s is %s' % (v, type(v))
#     url = settings.ITEM_API_URL
#     payload = {
#       u'identity': settings.ITEM_API_IDENTITY,
#       u'authorization_code': settings.ITEM_API_KEY,
#       u'pid': pid }
#     r = requests.delete( url, data=payload, verify=False )  # to work around devbox's bad certificate
#     jdict = json.loads( r.content.decode(u'utf-8', u'replace') )
#     # print u'- jdict'; pprint.pprint( jdict )
#     assert sorted(jdict.keys()) == [u'info', u'request', u'response'], sorted(jdict.keys())
#     assert jdict[u'response'].keys() == [ u'status' ]
#     return { u'status': jdict[u'response'][u'status']}
#   except:
#     error_dict = makeErrorDict()
#     print u'in deleteItem(); exception is:'; pprint.pprint( error_dict )
#     updateLog( message=u'- in uc.deleteItem(); exception detail is: %s' % error_dict, message_importance='high', identifier=identifier )
#     return { u'status': u'FAILURE', u'data': error_dict }



if __name__ == u'__main__':

    """ Gets list of pids to delete and puts each deletion-job on queueu. """

    ## set up logger
    logger = bell_logger.setup_logger()
    logger.info( u'in delete_dev_collection_pids(); starting' )

    ## settings
    COLLECTION_PID = unicode( os.environ.get(u'BELL_UTILS__COLLECTION_PID') )
    MEMBERSHIP_URL = unicode( os.environ.get(u'BELL_UTILS__FEDORA_RISEARCH_URL') )
    DELETION_URL = unicode( os.environ.get(u'BELL_UTILS__DELETION_API_URL') )

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
    for pid in fedora_pid_list:
        data = { u'pid': pid, u'deletion_url': DELETION_URL }
        q.enqueue_call ( func=u'utils.delete_dev_collection_pids.delete_item', args =(data,), timeout=30 )
        break
    logger.info( u'in delete_dev_collection_pids(); all deletion jobs put on queue.' )

    ## end ##
