# -*- coding: utf-8 -*-

import datetime, json, os, pprint, sys
import bell_logger
import redis
from redis import Redis
from rq import Queue
from tasks import task_manager


r = redis.StrictRedis( host='localhost', port=6379, db=0 )
queue_name = os.environ.get( u'BELL_QUEUE_NAME' )
q = Queue( queue_name, connection=Redis() )


def ensure_redis():
    """ Checks that redis is running. """
    logger = bell_logger.setup_logger()
    logger.info( u'STARTING_PROCESSING...' )
    try:
        assert len(r.keys()) > -1  # if redis isn't running this will generate an error
        next = task_manager.determine_next_task( sys._getframe().f_code.co_name, logger=logger )  # passes current function name
        # job = q.enqueue_call ( func=u'%s' % next, args=(), timeout=30 )
        logger.info( u'in check_environment.ensure_redis(); redis-check ok' )
        return
    except Exception as e:
        message = u'in check_environment.ensure_redis(); redis does not appear to be running; exception: %s' % unicode(repr(e))
        logger.error( message )
        raise Exception( message )


def archive_previous_work():
    """ Archives previous redis data. """
    logger = bell_logger.setup_logger()
    try:
        bell_dir = unicode( os.environ.get(u'BELL_LOG_DIR') )
        now_string = unicode( datetime.datetime.now() ).replace( u' ', u'_' )
        archive_file_path = u'%s/%s.archive' % ( bell_dir, now_string )
        jstring = _convert_tracker_to_dict()
        with open( archive_file_path, u'w' ) as f:
            f.write( jstring )
        next = task_manager.determine_next_task( sys._getframe().f_code.co_name, logger=logger )
        # job = q.enqueue_call ( func=u'%s' % next, args=(), timeout=30 )
        logger.info( u'in check_environment.archive_previous_work(); archive_previous_work ok' )
        return
    except Exception as e:
        message = u'in check_environment.archive_previous_work(); problem archiving previous work; exception: %s' % unicode(repr(e))
        logger.error( message )
        raise Exception( message )


def _convert_tracker_to_dict():
    """ Returns json from bell:tracker. """
    dct = r.hgetall( u'bell:tracker' )
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
        OVERWRITE = unicode( os.environ.get(u'BELL_TRACKER_OVERWRITE') )
        tracker_key = u'bell:tracker'
        if OVERWRITE == u'TRUE':
            r.delete( tracker_key )
        if not r.exists( tracker_key ):
            message = u'%s initialized %s' % ( tracker_key, unicode(datetime.datetime.now()) )
            r.hset( tracker_key, u'GENERAL', json.dumps([message]) )
        next = task_manager.determine_next_task( sys._getframe().f_code.co_name, logger=logger )
        # job = q.enqueue_call ( func=u'%s' % next, args=(), timeout=30 )
        logger.info( u'in check_environment.ensure_redis_status_dict(); bell_status ok' )
        return
    except Exception as e:
        message = u'in check_environment.ensure_redis_status_dict(); redis bell_status not set; exception: %s' % unicode(repr(e))
        logger.error( message )
        raise Exception( message )


def check_foundation_files():
    """ Checks that foundation-files exist. """
    logger = bell_logger.setup_logger()
    try:
        for filepath in [ os.environ.get(u'BELL_CE__BELL_DICT_JSON_PATH'), os.environ.get(u'BELL_CE__AccToPidDict_JSON_PATH') ]:
            try:
                assert os.path.exists( filepath )
            except Exception as e:
                message = u'Problem finding filepath %s; exception: %s' % ( filepath, unicode(repr(e)) )
                logger.error( message ); raise Exception( message )
        task_manager.update_tracker( key=u'GENERAL', message=u'foundation files ok' )
        next = task_manager.determine_next_task( sys._getframe().f_code.co_name, logger=logger )
        # job = q.enqueue_call ( func=u'%s' % next, args=(), timeout=30 )
        logger.info( u'in check_environment.check_foundation_files(); files ok' )
        return
    except Exception as e:
        message = u'in check_environment.check_foundation_files(); problem checking foundation files; exception: %s' % unicode(repr(e))
        logger.error( message )
        raise Exception( message )
