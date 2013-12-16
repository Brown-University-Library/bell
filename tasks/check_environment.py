# -*- coding: utf-8 -*-

import datetime, json, os, pprint, sys
import bell_logger
import redis
from redis import Redis
from rq import Queue
from tasks import task_manager


r = redis.StrictRedis( host='localhost', port=6379, db=0 )
queue_name = os.environ.get(u'BELL_QUEUE_NAME')
q = Queue( queue_name, connection=Redis() )


def ensure_redis():
    """ Checks that redis is running. """
    logger = bell_logger.setup_logger()
    logger.info( u'STARTING_PROCESSING...' )
    try:
        assert len(r.keys()) > -1  # if redis isn't running this will generate an error
        next = task_manager.determine_next_task( sys._getframe().f_code.co_name )  # passes current function name
        job = q.enqueue_call ( func=u'%s' % next, args = (), timeout = 30 )
        return
    except Exception as e:
        message = u'Redis does not appear to be running; exception: %s' % unicode(repr(e))
        logger.error( message )
        raise Exception( message )


def archive_previous_work():
    """ Archives previous redis data. """
    try:
        bell_dir = unicode( os.environ.get(u'BELL_LOG_DIR') )
        now_string = unicode( datetime.datetime.now() ).replace( u' ', u'_' )
        archive_file_path = u'%s/%s.archive' % ( bell_dir, now_string )
        d = r.hgetall( u'bell_work_tracker' )
        jstring = json.dumps( d, sort_keys=True, indent=2 )
        with open( archive_file_path, u'w' ) as f:
            f.write( jstring )
        next = task_manager.determine_next_task( sys._getframe().f_code.co_name )
        job = q.enqueue_call ( func=u'%s' % next, args = (), timeout = 30 )
        return
    except Exception as e:
        raise Exception( unicode(repr(e)) )


def ensure_redis_status_dict():
    """ Ensures the status dict exists. """
    logger = bell_logger.setup_logger()
    try:
        if not r.exists( u'bell_work_tracker' ):
            r.hset( u'bell_work_tracker', 'initialized', unicode(datetime.datetime.now()) )
        next = task_manager.determine_next_task( sys._getframe().f_code.co_name )  # passes current function name
        job = q.enqueue_call ( func=u'%s' % next, args = (), timeout = 30 )
        logger.info( u'bell_work_tracker ready' )
        return
    except Exception as e:
        message = u'Redis bell_status not set; exception: %s' % unicode(repr(e))
        logger.error( message )
        raise Exception( message )


def check_foundation_files():
    """ Checks that foundation-files exist. """
    logger = bell_logger.setup_logger()
    for filepath in [ os.environ.get(u'BELL_CE__BELL_DICT_JSON_PATH'), os.environ.get(u'BELL_CE__AccToPidDict_JSON_PATH') ]:
        try:
            assert os.path.exists( filepath )
        except Exception as e:
            message = u'Problem finding filepath %s; exception: %s' % ( filepath, unicode(repr(e)) )
            logger.error( message )
            raise Exception( message )
    next = task_manager.determine_next_task( sys._getframe().f_code.co_name )
    job = q.enqueue_call ( func=u'%s' % next, args = (), timeout = 30 )
    return
