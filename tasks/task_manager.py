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


def determine_next_task( current_task, data=None, logger=None ):
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
    else:
        if logger:
            message = u'in task_manager.determine_next_task(); no next task selected for current_task, %s; data, %s' % (current_task, data)
            logger.info( message )
        next_task = None
    return next_task


def populate_queue():
    """ Puts the bell items on the queue. """
    logger = bell_logger.setup_logger()
    try:
        with open( os.environ.get(u'BELL_CE__BELL_DICT_JSON_PATH') ) as f:
            all_items_dict = json.loads( f.read() )
        for i,(accnum_key, item_dict_value) in enumerate( sorted(all_items_dict[u'items'].items()) ):
            next = determine_next_task( sys._getframe().f_code.co_name, logger=logger )
            job = q.enqueue_call ( func=u'%s' % next, args = (item_dict_value,), timeout = 30 )
            logger.info( u'in task_manager.populate_queue(); added accnum %s to queue' % accnum_key )
            if i > int( os.environ.get(u'BELL_TM__POPULATE_QUEUE_LIMIT') ):  # for development
                logger.debug( u'in task_manager.populate_queue(); breaking after %s' % accnum_key ); break
        update_tracker( key=u'GENERAL', message=u'queue populated' )
        logger.info( u'populate_queue ok' )
        return
    except Exception as e:
        message = u'Problem in populate_queue(); exception is: %s' % unicode(repr(e))
        logger.error( message )
        raise Exception( message )


def determine_situation( item_dict ):
    """ Examines item dict after populate_queue() and updates next task. """
    logger = bell_logger.setup_logger();
    try:
        acc_num = item_dict[u'calc_accession_id']
        situation = None
        if _check_recently_processed( acc_num, logger ): situation = u'skip__already_processed'
        if not situation:
            pid = _check_pid( acc_num, logger )
            situation = u'skip__pid_"%s"_exists' % pid if(pid) else u'create_metadata_only_object'
        update_tracker( key=acc_num, message=u'situation: %s' % situation )
        next = determine_next_task( sys._getframe().f_code.co_name, data={u'situation': situation}, logger=logger )
        if next: job = q.enqueue_call ( func=u'%s' % next, args = (item_dict,), timeout = 30 )
        logger.info( u'in task_manager.determine_situation(); done; acc_num, %s; situation, %s' % (acc_num, situation) )
        return
    except Exception as e:
        message = u'Problem in determine_situation(); exception is: %s' % unicode(repr(e))
        logger.error( message )
        raise Exception( message )


def _check_recently_processed( accession_number_key, logger=None ):
    """ Checks redis bell:tracker to see if item has recently been successfully ingested.
        Called by determine_situation() """
    return_val = False
    tracker_name = u'bell:tracker'
    if r.hexists( tracker_name, accession_number_key ):
        key_value_list = json.loads( r.hget(tracker_name, accession_number_key) )
        if u'save_successful' in key_value_list:
            return_val = True
    logger.info( u'in task_manager._check_recently_processed(); acc_num, %s; return_val, %s' % (accession_number_key, return_val) )
    return return_val


def _check_pid( acc_num, logger=None ):
    """ Checks if accession number has a pid and returns it if so.
        Called by determine_situation() """
    with open( os.environ.get(u'BELL_TM__PID_DICT_JSON_PATH') ) as f:
        full_pid_data_dict = json.loads( f.read() )
    pid = full_pid_data_dict[u'final_accession_pid_dict'][acc_num][u'pid']
    logger.info( u'in task_manager._check_pid(); pid, %s' % pid )
    return pid


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
