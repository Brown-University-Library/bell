# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import datetime, json, os, pprint, sys
import redis, rq
from bell_code import bell_logger
from bell_code.tasks import task_manager


r = redis.StrictRedis( host='localhost', port=6379, db=0 )
queue_name = os.environ.get( 'BELL_QUEUE_NAME' )
q = rq.Queue( queue_name, connection=redis.Redis() )


def ensure_redis():
    """ Checks that redis is running. """
    logger = bell_logger.setup_logger()
    logger.info( 'STARTING_PROCESSING...' )
    try:
        assert len(r.keys()) > -1  # if redis isn't running this will generate an error
        next = task_manager.determine_next_task( sys._getframe().f_code.co_name, logger=logger )  # passes current function name
        # job = q.enqueue_call ( func='%s' % next, args=(), timeout=30 )
        logger.info( 'in check_environment.ensure_redis(); redis-check ok' )
        return
    except Exception as e:
        message = 'in check_environment.ensure_redis(); redis does not appear to be running; exception: %s' % unicode(repr(e))
        logger.error( message )
        raise Exception( message )


def archive_previous_work():
    """ Archives previous redis data. """
    logger = bell_logger.setup_logger()
    try:
        bell_dir = unicode( os.environ.get('BELL_LOG_DIR') )
        now_string = unicode( datetime.datetime.now() ).replace( ' ', '_' )
        archive_file_path = '%s/%s.archive' % ( bell_dir, now_string )
        jstring = _convert_tracker_to_dict()
        with open( archive_file_path, 'w' ) as f:
            f.write( jstring )
        next = task_manager.determine_next_task( sys._getframe().f_code.co_name, logger=logger )
        # job = q.enqueue_call ( func='%s' % next, args=(), timeout=30 )
        logger.info( 'in check_environment.archive_previous_work(); archive_previous_work ok' )
        return
    except Exception as e:
        message = 'in check_environment.archive_previous_work(); problem archiving previous work; exception: %s' % unicode(repr(e))
        logger.error( message )
        raise Exception( message )


def _convert_tracker_to_dict():
    """ Returns json from bell:tracker. """
    dct = r.hgetall( 'bell:tracker' )
    dct2 = {}
    for key, value in dct.items():
        value_data = json.loads( value )
        dct2[key] = value_data
    jstring = json.dumps( dct2, sort_keys=True, indent=2 )
    return jstring


def ensure_redis_status_dict():
    """ Ensures the status dict exists. Resets it if required.
        Each key's value is a json-serializable list. """
    logger = bell_logger.setup_logger()
    try:
        OVERWRITE = unicode( os.environ.get('BELL_CE__TRACKER_OVERWRITE') )
        tracker_key = 'bell:tracker'
        if OVERWRITE == 'TRUE':
            r.delete( tracker_key )
        if not r.exists( tracker_key ):
            message = '%s initialized %s' % ( tracker_key, unicode(datetime.datetime.now()) )
            r.hset( tracker_key, 'GENERAL', json.dumps([message]) )
        next = task_manager.determine_next_task( sys._getframe().f_code.co_name, logger=logger )
        # job = q.enqueue_call ( func='%s' % next, args=(), timeout=30 )
        logger.info( 'in check_environment.ensure_redis_status_dict(); bell_status ok' )
        return
    except Exception as e:
        message = 'in check_environment.ensure_redis_status_dict(); redis bell_status not set; exception: %s' % unicode(repr(e))
        logger.error( message )
        raise Exception( message )


def check_foundation_files():
    """ Checks that foundation-files exist. """
    logger = bell_logger.setup_logger()
    try:
        for filepath in [ os.environ.get('BELL_CE__BELL_DICT_JSON_PATH'), os.environ.get('BELL_CE__AccToPidDict_JSON_PATH') ]:
            try:
                assert os.path.exists( filepath )
            except Exception as e:
                message = 'Problem finding filepath %s; exception: %s' % ( filepath, unicode(repr(e)) )
                logger.error( message ); raise Exception( message )
        task_manager.update_tracker( key='GENERAL', message='foundation files ok' )
        next = task_manager.determine_next_task( sys._getframe().f_code.co_name, logger=logger )
        # job = q.enqueue_call ( func='%s' % next, args=(), timeout=30 )
        logger.info( 'in check_environment.check_foundation_files(); files ok' )
        return
    except Exception as e:
        message = 'in check_environment.check_foundation_files(); problem checking foundation files; exception: %s' % unicode(repr(e))
        logger.error( message )
        raise Exception( message )
