# -*- coding: utf-8 -*-

""" Outputs failed-queue jobs for given queue. """

import os, pprint
import redis, rq


TARGET_QUEUE = os.environ.get( u'BELL_QUEUE_NAME' )
queue_name = u'failed'
q = rq.Queue( queue_name, connection=redis.Redis() )

d = { u'failed_target_count': None, u'jobs': [] }
failed_count = 0
for job in q.jobs:
    if not job.origin == TARGET_QUEUE:
        continue
    failed_count += 1
    job_d = {
        u'args': job._args,
        u'kwargs': job._kwargs,
        u'function': job._func_name,
        u'dt_created': job.created_at,
        u'dt_enqueued': job.enqueued_at,
        u'dt_ended': job.ended_at,
        u'origin': job.origin,
        u'id': job._id,
        u'traceback': job.exc_info
        }
    d[u'jobs'].append( job_d )
d[u'failed_target_count'] = failed_count
pprint.pprint( d )
