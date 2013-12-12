# -*- coding: utf-8 -*-

import json, os
import bell_logger


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
    return next_task


def populate_queue():
    """ Puts the bell items on the queue. """
    logger = bell_logger.setup_logger()
    LIMIT = 2
    main_json_path = os.environ.get( u'BELL_CE__BELL_DICT_JSON_PATH' )
    with open( main_json_path ) as f:
        all_items_dict = json.loads( f.read() )
    for i,(accnum_key, item_dict_value) in enumerate( all_items_dict[u'items'].items() ):
      ## register with redis tracker
      logger.info( u'accnum_key is: %s' % accnum_key )
      # next = task_manager.determine_next_task( sys._getframe().f_code.co_name )
      # job = q.enqueue_call ( func=u'%s' % next, args = (), timeout = 30 )
      if i > LIMIT:
        break
    logger.info( u'break occurred' )
    return
