# -*- coding: utf-8 -*-

import json, os, sys
import bell_logger
import redis
from redis import Redis
from rq import Queue
# from tasks import fedora_metadata_only_builder


queue_name = os.environ.get(u'BELL_QUEUE_NAME')
q = Queue( queue_name, connection=Redis() )
r = redis.StrictRedis( host='localhost', port=6379, db=0 )


def determine_next_task( current_task, data=None ):
    """ Returns next task. """
    next_task = None
    if current_task == u'ensure_redis':
        next_task = u'tasks.check_environment.archive_previous_work'
    elif current_task == u'archive_previous_work':
        next_task = u'tasks.check_environment.ensure_redis_status_dict'
    elif current_task == u'ensure_redis_status_dict':
        next_task = u'tasks.check_environment.check_foundation_files'
    elif current_task == u'check_foundation_files':
        next_task = u'tasks.task_manager.populate_queue'
    ## concurrent-processing starts here ##
    elif current_task == u'populate_queue':
        next_task = u'tasks.task_manager.determine_situation'
    elif current_task == u'determine_situation' and data[u'situation'] == u'create_metadata_only_object':
        next_task = u'tasks.fedora_metadata_only_builder.run__create_fedora_metadata_object'
    return next_task


def populate_queue():
    """ Puts the bell items on the queue. """
    logger = bell_logger.setup_logger()
    with open( os.environ.get(u'BELL_CE__BELL_DICT_JSON_PATH') ) as f:
        all_items_dict = json.loads( f.read() )
    for i,(accnum_key, item_dict_value) in enumerate( sorted(all_items_dict[u'items'].items()) ):
        logger.info( u'accnum_key is: %s' % accnum_key )  # TEMP
        next = determine_next_task( sys._getframe().f_code.co_name )
        job = q.enqueue_call ( func=u'%s' % next, args = (item_dict_value,), timeout = 30 )
        if i > int( os.environ.get(u'BELL_TM__POPULATE_QUEUE_LIMIT') ):
            break
    update_tracker( key=u'GENERAL', message=u'queue populated' )
    logger.info( u'break occurred' ); return


def determine_situation( item_dict ):
    """ Examines item dict and updates next task. """
    logger = bell_logger.setup_logger(); logger.info( u'item_dict acc_num is: %s' % item_dict[u'calc_accession_id'] )  # TEMP
    acc_num = item_dict[u'calc_accession_id']
    situation = u'init'
    if _check_recently_processed:
        pass
    with open( os.environ.get(u'BELL_ANTP__BELL_DICT_JSON_PATH') ) as f:  # check for pid
        accnum_to_pid_dict = json.loads( f.read() )
    if not acc_num in accnum_to_pid_dict.keys():
        situation = u'create_metadata_only_object'
        update_tracker( key=acc_num, message=u'situation: %s' % situation )
        next = determine_next_task( sys._getframe().f_code.co_name, data={u'situation': situation} )
        job = q.enqueue_call ( func=u'%s' % next, args = (item_dict,), timeout = 30 )
    return


def _check_recently_processed( accession_number_key ):
    """ Checks redis bell:tracker to see if item has recently been successfully ingested.
        Called by determine_situation() """
    return_val = False
    if r.hexists( tracker_name, accession_number_key ):
        key_value_list = json.loads( r.hget(tracker_name, accession_number_key) )
        if u'ingestion_successful' in key_value_list:
            return_val = True
    return return_val


def update_tracker( key, message ):
    """ Updates redis bell:tracker hash.
        Note: the value for each key is a json-serializable list; makes it easy to add info. """
    tracker_name = u'bell:tracker'
    if r.hexists( tracker_name, key ):
        key_value = json.loads( r.hget(tracker_name, key) )
        if not message in key_value:  # prevents duplicated setup entries when re-running
            key_value.append( message )
    else:
        key_value = [ message ]
    r.hset( tracker_name, key, json.dumps(key_value) )
    return
