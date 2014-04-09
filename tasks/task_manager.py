# -*- coding: utf-8 -*-

import json, pprint, os, sys
import redis, requests, rq
from bell_code import bell_logger


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
        next_task = u'tasks.task_manager.determine_handler'

    elif current_task == u'determine_handler':
        assert sorted(data.keys()) == [ u'handler', u'item_dict', u'pid' ]  # pid could be None
        if data[u'handler'] == u'add_new_metadata_only_item':
            next_task = u'tasks.fedora_metadata_only_builder.run__create_fedora_metadata_object'  # built
        elif data[u'handler'] == u'add_new_item_with_image':
            next_task = u'tasks.fedora_metadata_and_image_builder.run__add_metadata_and_image'  # built
        elif data[u'handler'] == u'update_existing_metadata':
            next_task = u'tasks.fedora_metadata_only_updater.run__update_existing_metadata_object'  # TODO
        elif data[u'handler'] == u'update_existing_metadata_and_create_image':
            next_task = u'tasks.fedora_metadata_updater_and_image_builder.run__update_existing_metadata_and_create_image'  # built
        elif data[u'handler'] == u'update_existing_metadata_and_update_image':
            next_task = u'tasks.fedora_metadata_updater_and_image_updater.run__update_existing_metadata_and_update_image'  # TODO

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
        message = u'in task_manager.determine_next_task(); no next task selected for current_task, %s; data, %s' % (current_task, data)
        logger.info( message )
        next_task = None

    logger.info( u'in task_manager.determine_next_task(); %s' % pprint.pformat({u'next_task': next_task, u'data': data}) )
    if next_task:
        if data:
            job = q.enqueue_call( func=u'%s' % next_task, args=(data,), timeout=120 )
        else:
            job = q.enqueue_call( func=u'%s' % next_task, args=(), timeout=120 )
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


def determine_handler( item_dict ):
    """ Examines item dict after populate_queue() and updates next task.
        Situations:
        - accession_number has no pid, & has no image_filename
            handler == ‘add_new_metadata_only_item’
        - accession_number has no pid, & has image_filename, & image_file _not_ in image_dir
            handler == ‘add_new_metadata_only_item’
        - accession_number has no pid, & has image_filename, & image_file _found_ in image_dir
            handler == ‘add_new_item_with_image’
        - accession_number has pid, & has no image_filename
            handler == ‘update_existing_metadata’
        - accession_number has pid, & has image_filename, & image_file _not_ in image_dir
            handler == ‘update_existing_metadata’
        - accession_number has pid, & has image_filename, & image_file _found_ in image_dir
            handler == 'update_existing_metadata_and_create_image' _or_ 'update_existing_metadata_and_update_image'
            """
    IMAGE_DIR = os.environ.get(u'BELL_TM__IMAGES_DIR_PATH')
    logger = bell_logger.setup_logger()
    handler = None
    acc_num = item_dict[u'calc_accession_id']
    filename = item_dict[u'object_image_scan_filename']
    filepath = _check_filepath( filename, IMAGE_DIR, logger )
    pid = _check_pid( acc_num, logger )
    if _check_recently_processed( acc_num, logger ):
        handler = u'skip__already_processed'
        update_tracker( key=acc_num, message=u'handler: %s' % handler )
        return
    if not pid and not filename:
        handler = u'add_new_metadata_only_item'
    elif not pid and filename and not filepath:
        handler = u'add_new_metadata_only_item'
    elif not pid and filename and filepath:
        handler = u'add_new_item_with_image'
    elif pid and not filepath:
        handler = u'update_existing_metadata'
    elif pid and filename and not filepath:
        handler = u'update_existing_metadata'
    elif pid and filename and filepath:
        if _image_already_ingested( pid, logger ):
            handler = u'update_existing_metadata_and_update_image'
        else:
            handler = u'update_existing_metadata_and_create_image'
    else:
        raise Exception( u'in task_manager.determine_handler(); unhandled case' )
    update_tracker( key=acc_num, message=u'handler: %s' % handler )
    determine_next_task(
        current_task=sys._getframe().f_code.co_name,
        data={ u'item_dict': item_dict, u'handler': handler, u'pid': pid },
        logger=logger )
    logger.info( u'in task_manager.determine_handler(); done; acc_num, %s; filepath, %s; pid, %s; handler, %s' % (acc_num, filepath, pid, handler) )
    return

    # end def determine_handler()


def _check_filepath( filename, IMAGE_DIR, logger ):
    """ Checks to see if file exists.
        Returns filepath string or None.
        Called by determine_handler(). """
    filepath = None
    temp_filepath = None
    if filename:
        temp_filepath = u'%s/%s' % ( IMAGE_DIR, filename )
        if os.path.isfile( temp_filepath ):
            filepath = temp_filepath
    logger.info( u'in task_manager._check_filepath(); temp_filepath, %s; filepath, %s' % (temp_filepath, filepath) )
    return filepath
    #
def _check_pid( acc_num, logger=None ):
    """ Checks if accession number has a pid and returns it if so.
        Called by determine_handler() """
    FILE_PATH = os.environ.get(u'BELL_TM__PID_DICT_JSON_PATH')
    with open( FILE_PATH ) as f:
        full_pid_data_dict = json.loads( f.read() )
        # logger.info( u'in task_manager._check_pid(); full_pid_data_dict, %s' % pprint.pformat(full_pid_data_dict) )
    pid = full_pid_data_dict[u'final_accession_pid_dict'][acc_num]
    logger.info( u'in task_manager._check_pid(); pid, %s' % pid )
    return pid
    #
def _check_recently_processed( accession_number_key, logger=None ):
    """ Checks redis bell:tracker to see if item has recently been successfully ingested.
        Called by determine_handler() """
    return_val = False
    tracker_name = u'bell:tracker'
    if r.hexists( tracker_name, accession_number_key ):
        key_value_list = json.loads( r.hget(tracker_name, accession_number_key) )
        if u'save_successful' in key_value_list:
            return_val = True
    logger.info( u'in task_manager._check_recently_processed(); acc_num, %s; return_val, %s' % (accession_number_key, return_val) )
    return return_val
    #
def _image_already_ingested( pid, logger ):
    """ Checks repo api to see if an image has previously been ingested.
        Returns boolean.
        Called by determine_handler() """
    ITEM_API_ROOT = os.environ.get(u'BELL_TM__ITEM_API_ROOT')
    image_already_ingested = True
    item_api_url = u'%s/%s/' % ( ITEM_API_ROOT, pid )
    logger.debug( u'in task_manager._image_already_ingested(); item_api_url, %s' % item_api_url )
    r = requests.get( item_api_url, verify=False )
    d = r.json()
    if u'JP2' in d[u'links'][u'content_datastreams'].keys()  or  u'jp2' in d[u'rel_content_models_ssim']:
        pass
    else:
        image_already_ingested = False
    return image_already_ingested


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
