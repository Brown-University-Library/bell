# -*- coding: utf-8 -*-

from __future__ import unicode_literals

""" Outputs failed-queue jobs for given queue. """

import os, pprint
import redis, rq


TARGET_QUEUE = os.environ.get( 'BELL_QUEUE_NAME' )
queue_name = 'failed'
q = rq.Queue( queue_name, connection=redis.Redis() )

d = { 'failed_target_count': None, 'jobs': [] }
failed_count = 0
for job in q.jobs:
    if not job.origin == TARGET_QUEUE:
        continue
    failed_count += 1
    job_d = {
        'args': job._args,
        'kwargs': job._kwargs,
        'function': job._func_name,
        'dt_created': job.created_at,
        'dt_enqueued': job.enqueued_at,
        'dt_ended': job.ended_at,
        'origin': job.origin,
        'id': job._id,
        'traceback': job.exc_info
        }
    d['jobs'].append( job_d )
d['failed_target_count'] = failed_count
pprint.pprint( d )
