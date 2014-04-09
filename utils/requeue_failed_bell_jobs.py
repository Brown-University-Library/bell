# -*- coding: utf-8 -*-

""" Requeues bell failed-queue jobs. """

import os, sys
import redis, rq


failed_queue = rq.queue.get_failed_queue( connection=redis.Redis(u'localhost') )
BELL_QUEUE = os.environ.get( u'BELL_QUEUE_NAME' )

for job in failed_queue.jobs:
    if not job.origin == BELL_QUEUE:
        print( u'skipping non-bell function call: %s' % job.func_name )
        continue
    else:
        print( u'function call: %s' % job.func_name )
        action_val = raw_input( u'Action (use first letter from the following, default is nothing): [Nothing/Requeue/Delete] ' )
        if action_val.lower() == u'r':
            print('requeuing job...')
            # failed_queue.requeue( job.id )
        elif action_val.lower() == 'd':
            print('deleting job...')
            # failed_queue.remove( job.id )
        else:
            print('skipping job...')
            pass

sys.exit()
