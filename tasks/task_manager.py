# -*- coding: utf-8 -*-

import json, pprint, os, sys
import bell_logger
import redis, rq
# import redis
# from redis import Redis
# from rq import Queue


queue_name = os.environ.get(u'BELL_QUEUE_NAME')
q = rq.Queue( queue_name, connection=redis.Redis() )
r = redis.StrictRedis( host='localhost', port=6379, db=0 )


def determine_next_task( current_task, data=None, logger=None ):
    """ Calls next task.
        Intended to eventually handle full flow of normal bell processing. """

    logger.info( u'in task_manager.determine_next_task(); %s' % pprint.pformat({u'current_task': current_task, u'data': data}) )
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
        assert sorted(data.keys()) == [u'item_dict', u'situation']
        next_task = u'tasks.fedora_metadata_only_builder.run__create_fedora_metadata_object'

    elif current_task == u'create_fedora_metadata_object':
        assert sorted( data.keys() ) == [ u'item_data', u'pid' ]
        next_task = u'tasks.indexer.build_metadata_only_solr_dict'

    elif current_task == u'build_metadata_only_solr_dict':
        assert data.keys() == [ u'solr_dict' ]
        next_task = u'tasks.indexer.post_to_solr'

    else:
        message = u'in task_manager.determine_next_task(); no next task selected for current_task, %s; data, %s' % (current_task, data)
        logger.info( message )
        next_task = None

    ## TODO: have this make _all_ enqueue calls!
    logger.info( u'in task_manager.determine_next_task(); %s' % pprint.pformat({u'next_task': next_task, u'data': data}) )
    if next_task:
        if data:
            job = q.enqueue_call( func=u'%s' % next_task, args=(data,), timeout=30 )
        else:
            job = q.enqueue_call( func=u'%s' % next_task, args=(), timeout=30 )
    return next_task


def populate_queue():
    """ Puts individual bell items on the queue. """
    logger = bell_logger.setup_logger()
    try:
        with open( os.environ.get(u'BELL_CE__BELL_DICT_JSON_PATH') ) as f:
            all_items_dict = json.loads( f.read() )
        for i,(accnum_key, item_dict_value) in enumerate( sorted(all_items_dict[u'items'].items()) ):
            determine_next_task( current_task=sys._getframe().f_code.co_name, data=item_dict_value, logger=logger )
            if i+2 > int( os.environ.get(u'BELL_TM__POPULATE_QUEUE_LIMIT') ):  # for development
                logger.debug( u'in task_manager.populate_queue(); breaking after %s' % accnum_key ); break
        update_tracker( key=u'GENERAL', message=u'queue populated' )
        logger.info( u'in task_manager.populate_queue(); populate_queue ok' )
        return
    except Exception as e:
        message = u'in task_manager.populate_queue(); problem in populate_queue(); exception is: %s' % unicode(repr(e))
        logger.error( message )
        raise Exception( message )


def determine_situation( item_dict ):
    """ Examines item dict after populate_queue() and updates next task. """
    logger = bell_logger.setup_logger();
    acc_num = item_dict[u'calc_accession_id']
    situation = None
    if _check_recently_processed( acc_num, logger ): situation = u'skip__already_processed'
    if situation == None:
        pid = _check_pid( acc_num, logger )
        situation = u'skip__pid_"%s"_exists' % pid if(pid) else u'create_metadata_only_object'
    update_tracker( key=acc_num, message=u'situation: %s' % situation )
    determine_next_task( sys._getframe().f_code.co_name, data={u'item_dict': item_dict, u'situation': situation}, logger=logger )
    logger.info( u'in task_manager.determine_situation(); done; acc_num, %s; situation, %s' % (acc_num, situation) )
    return


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
    FILE_PATH = os.environ.get(u'BELL_TM__PID_DICT_JSON_PATH')
    with open( FILE_PATH ) as f:
        full_pid_data_dict = json.loads( f.read() )
        # logger.info( u'in task_manager._check_pid(); full_pid_data_dict, %s' % pprint.pformat(full_pid_data_dict) )
    pid = full_pid_data_dict[u'final_accession_pid_dict'][acc_num]
    # logger.info( u'in task_manager._check_pid(); pid, %s' % pid )
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
