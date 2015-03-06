# -*- coding: utf-8 -*-

import json, pprint, os, sys
import redis, requests, rq
from bell_code import bell_logger


queue_name = os.environ.get(u'BELL_QUEUE_NAME')
q = rq.Queue( queue_name, connection=redis.Redis() )
r = redis.StrictRedis( host='localhost', port=6379, db=0 )


def determine_next_task( current_task=None, data=None, logger=None ):
    """ Calls next task.
        Intended to eventually handle full flow of normal bell processing. """

    # logger.info( u'in task_manager.determine_next_task(); %s' % pprint.pformat({u'current_task': current_task, u'data': data}) )
    logger.info( u'in task_manager.determine_next_task(); current task, `%s`' % current_task )
    logger.info( u'in task_manager.determine_next_task(); current data, `%s`' % pprint.pformat(data) )

    next_task = None

    if current_task == u'ensure_redis':
        next_task = u'bell_code.tasks.check_environment.archive_previous_work'

    elif current_task == u'archive_previous_work':
        next_task = u'bell_code.tasks.check_environment.ensure_redis_status_dict'

    elif current_task == u'ensure_redis_status_dict':
        next_task = u'bell_code.tasks.check_environment.check_foundation_files'

    elif current_task == u'check_foundation_files':
        next_task = u'bell_code.tasks.task_manager.populate_queue'

    ## concurrent-processing starts here ##

    elif current_task == u'populate_queue':
        next_task = u'bell_code.tasks.metadata.run_check_create_metadata'

    elif current_task == u'check_create_metadata' and data[u'create_metadata'] == False:
        next_task = u'bell_code.tasks.metadata.run_check_update_metadata'
        data = { u'item_data': data[u'item_data'], u'pid': data[u'pid'] }

    elif current_task == u'check_update_metadata' and data[u'update_metadata'] == False:
        next_task = u'bell_code.tasks.image.run_check_create_image'
        data = { u'item_data': data[u'item_data'], u'pid': data[u'pid'] }

    elif current_task == u'run_check_create_image' and data[u'create_image'] == False:
        next_task = u'bell_code.tasks.image.run_check_update_image'
        data = { u'item_data': data[u'item_data'], u'pid': data[u'pid'] }

    elif current_task == u'run_check_update_image' and data[u'update_image'] == True:
        next_task = u'bell_code.tasks.image.run_make_jp2'
        data = { u'item_data': data[u'item_data'], u'pid': data[u'pid'], u'update_image': data[u'update_image'] }  # update_image not used by make_jp2(), but will be passed to next task: add_image_datastream().

    elif current_task == u'run_make_jp2':
        next_task = u'bell_code.tasks.image.run_add_image_datastream'  # will look at the data[u'update_image'] value to determine whether to add 'overwrite' flag to api call.
        data = { u'item_data': data[u'item_data'], u'pid': data[u'pid'], u'update_image': data[u'update_image'] }

    elif current_task == u'run_add_image_datastream':
        next_task = u'bell_code.tasks.indexer.run_update_custom_index'
        data = { u'item_data': data[u'item_data'], u'pid': data[u'pid'] }

    # elif current_task == u'populate_queue':
    #     next_task = u'tasks.task_manager.determine_handler'

    # elif current_task == u'determine_handler':
    #     assert sorted(data.keys()) == [ u'handler', u'item_dict', u'pid' ]  # pid could be None
    #     if data[u'handler'] == u'add_new_metadata_only_item':
    #         next_task = u'tasks.fedora_metadata_only_builder.run__create_fedora_metadata_object'  # built
    #     elif data[u'handler'] == u'add_new_item_with_image':
    #         next_task = u'tasks.fedora_metadata_and_image_builder.run__add_metadata_and_image'  # built
    #     elif data[u'handler'] == u'update_existing_metadata':
    #         next_task = u'tasks.fedora_metadata_only_updater.run__update_existing_metadata_object'  # TODO
    #         next_task = None
    #     elif data[u'handler'] == u'update_existing_metadata_and_create_image':
    #         next_task = u'tasks.fedora_metadata_updater_and_image_builder.run__update_existing_metadata_and_create_image'  # built
    #     elif data[u'handler'] == u'update_existing_metadata_and_update_image':
    #         next_task = u'tasks.fedora_metadata_updater_and_image_updater.run__update_existing_metadata_and_update_image'  # TODO
    #         next_task = None

    elif current_task == u'create_fedora_metadata_object':
        assert sorted( data.keys() ) == [ u'item_data', u'pid' ]
        next_task = u'tasks.indexer.build_metadata_only_solr_dict'

    elif current_task == u'add_metadata_and_image' or current_task == 'update_existing_metadata_and_create_image':
        assert sorted( data.keys() ) == [ u'item_data', u'jp2_path', u'pid' ]
        next_task = u'tasks.cleanup.delete_jp2'

    # elif current_task == u'add_metadata_and_image' or current_task == 'update_existing_metadata_and_create_image':
    #     assert sorted( data.keys() ) == [ u'item_data', u'pid' ]
    #     next_task = u'tasks.indexer.build_metadata_and_image_solr_dict'

    elif current_task == u'delete_jp2':
        assert sorted( data.keys() ) == [ u'item_data', u'pid' ]
        next_task = u'tasks.indexer.build_metadata_and_image_solr_dict'

    elif current_task == u'build_metadata_only_solr_dict' or current_task == u'build_metadata_and_image_solr_dict':
        assert data.keys() == [ u'solr_dict' ]
        next_task = u'tasks.indexer.post_to_solr'

    else:
        message = u'in task_manager.determine_next_task(); no next task selected for current_task'
        logger.info( message )
        next_task = None

    # logger.info( u'in task_manager.determine_next_task(); %s' % pprint.pformat({u'next_task': next_task, u'data': data}) )
    logger.info( u'in task_manager.determine_next_task(); new task, `%s`' % next_task )
    logger.info( u'in task_manager.determine_next_task(); new data, `%s`' % pprint.pformat(data) )
    if next_task:
        if data:
            job = q.enqueue_call( func=u'%s' % next_task, args=(data,), timeout=600 )
        else:
            job = q.enqueue_call( func=u'%s' % next_task, args=(), timeout=600 )
    return next_task

    # end def determine_next_task()


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
