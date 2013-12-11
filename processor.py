# -*- coding: utf-8 -*-

from redis import Redis
from rq import Queue

""" Starts bell processing
    Assumes rq worker(s) started from bell_code directory.
    Sample bash code to start a worker daemon...
    #
    WORKER_NAME=bell_worker_$RANDOM       # $RANDOM is a built-in; doesn't have to be set
    LOG_FILENAME=$BELL_LOG_DIR/$WORKER_NAME.log
    BELL_LOG_DIR="/path/to/bell_log_dir"  # if not already set as an env variable
    QUEUE_NAME="bell_work"
    rqworker --name $WORKER_NAME $QUEUE_NAME >> $LOG_FILENAME 2>&1 &
    #
    """


q = Queue( u'bell_work', connection=Redis() )
job = q.enqueue_call (
  func=u'tasks.check_environment.check_redis_status_dict',
  args = (),
  timeout = 30
  )

