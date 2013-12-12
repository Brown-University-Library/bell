# -*- coding: utf-8 -*-

import datetime, json, os, pprint, sys
import redis

from redis import Redis
from rq import Queue
from tasks import next_task


r = redis.StrictRedis( host='localhost', port=6379, db=0 )
q = Queue( u'bell_work', connection=Redis() )


def check_redis_status_dict():
    """ Checks that redis is running. """
    try:
        assert len(r.keys()) > -1  # if redis isn't running this will generate an error
        next = next_task.determine_next_task( sys._getframe().f_code.co_name )  # passes current function name
        job = q.enqueue_call ( func=u'%s' % next, args = (), timeout = 30 )
        return
    except Exception as e:
        raise Exception( u'Redis does not appear to be running; exception: %s' % unicode(repr(e)) )


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
    core_accession_number_file_path = u'init'
    try:
        core_accession_number_file_path = unicode( os.environ.get(u'BELL_CE__BELL_DICT_JSON_PATH') )
        assert os.path.exists( core_accession_number_file_path )
    except Exception as e:
        message = u'Problem finding "accession_number_to_data_dict.json" at %s; exception: %s; environ: %s' % (
            core_accession_number_file_path, unicode(repr(e)), os.environ )
        raise Exception( message )
