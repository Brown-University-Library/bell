# -*- coding: utf-8 -*-

""" Requeues bell failed-queue jobs. """

import os, sys
import redis, rq


failed_queue = rq.queue.get_failed_queue( connection=redis.Redis(u'localhost') )
BELL_QUEUE = os.environ.get( u'BELL_QUEUE_NAME' )

for job in failed_queue.jobs:
    if not job.origin == BELL_QUEUE:
        print( u'skipping function call: %s' % job.func_name )
        continue
    else:
        print( u'requeueing function call: %s' % job.func_name )
        # failed_queue.requeue( job.id )

sys.exit()
