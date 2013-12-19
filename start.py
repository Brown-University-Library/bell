# -*- coding: utf-8 -*-

import os
from redis import Redis
from rq import Queue

""" Starts bell processing
    Assumes rq worker(s) started from bell_code directory.
    """


queue_name = os.environ.get(u'BELL_QUEUE_NAME')
q = Queue( queue_name, connection=Redis() )
job = q.enqueue_call (
  func=u'tasks.check_environment.ensure_redis',
  args=(),
  timeout=30
  )

