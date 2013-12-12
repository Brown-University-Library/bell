# -*- coding: utf-8 -*-

import datetime, json, os, pprint, sys
import bell_logger
import redis

from redis import Redis
from rq import Queue
from tasks import next_task


r = redis.StrictRedis( host='localhost', port=6379, db=0 )
q = Queue( u'bell_work', connection=Redis() )


def check_redis_status_dict():
    """ Checks that redis is running. """
    logger = bell_logger.setup_logger()
    logger.info( u'STARTING_PROCESSING...' )
    try:
        assert len(r.keys()) > -1  # if redis isn't running this will generate an error
        next = next_task.determine_next_task( sys._getframe().f_code.co_name )  # passes current function name
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
        next = next_task.determine_next_task( sys._getframe().f_code.co_name )
        job = q.enqueue_call ( func=u'%s' % next, args = (), timeout = 30 )
        return
    except Exception as e:
        raise Exception( unicode(repr(e)) )


def check_main_accession_number_dict():
    """ Checks that 'accession_number_to_data_dict.json' exists.
        Possible TODO: load it to ensure it's a dict. """
    logger = bell_logger.setup_logger()
    for filepath in [ os.environ.get(u'BELL_CE__BELL_DICT_JSON_PATH'), os.environ.get(u'BELL_CE__AccToPidDict_JSON_PATH') ]:
        try:
            assert os.path.exists( filepath )
            logger.info( u'- filepath %s ok' % filepath )
            logger.info( u'abc' )
        except Exception as e:
            message = u'Problem finding filepath %s; exception: %s' % ( filepath, unicode(repr(e)) )
            logger.error( message )
            raise Exception( message )
    return
