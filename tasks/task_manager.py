# -*- coding: utf-8 -*-

import json, os, sys
import bell_logger
from redis import Redis
from rq import Queue


queue_name = os.environ.get(u'BELL_QUEUE_NAME')
q = Queue( queue_name, connection=Redis() )


def determine_next_task( current_task ):
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
    elif current_task == u'populate_queue':
        next_task = u'tasks.task_manager.determine_situation'
    return next_task


def populate_queue():
    """ Puts the bell items on the queue. """
    logger = bell_logger.setup_logger()
    LIMIT = 2
    with open( os.environ.get(u'BELL_CE__BELL_DICT_JSON_PATH') ) as f:
        all_items_dict = json.loads( f.read() )
    for i,(accnum_key, item_dict_value) in enumerate( all_items_dict[u'items'].items() ):
      logger.info( u'accnum_key is: %s' % accnum_key )  # TEMP
      next = determine_next_task( sys._getframe().f_code.co_name )
      job = q.enqueue_call ( func=u'%s' % next, args = ( item_dict_value, ), timeout = 30 )
      if i > LIMIT:  # TEMP
        break
    logger.info( u'break occurred' )
    return


def determine_situation( item_dict_value ):
    """ Examines item dict and updates next task. """
    logger = bell_logger.setup_logger()
    logger.info( u'item_dict_value acc_num is: %s' % item_dict_value[u'calc_accession_id'] )  # TEMP
    return
