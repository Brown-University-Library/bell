# -*- coding: utf-8 -*-

from __future__ import unicode_literals

""" Shows queued Bell jobs. """

import pprint
import redis, rq


queue_name = 'bell:job_queue'
q = rq.Queue( queue_name, connection=redis.Redis() )

###
# at this point, q.jobs would just show something like:
# [Job(u'6cd95926-59d7-4825-9a87-814e4b527556', enqueued_at=datetime.datetime(2016, 5, 3, 14, 33, 56))]
# ...but I want to see the details of each job.
###

d = { 'count': None, 'jobs': [] }
count = 0

for job in q.jobs:
    count += 1
    job_d = {
        'args': job.args,
        'kwargs': job.kwargs,
        'function': job.func_name,
        'dt_created': job.created_at,
        'dt_enqueued': job.enqueued_at,
        'dt_ended': job.ended_at,
        'origin': job.origin,
        'id': job.id,
        'traceback': job.exc_info
        }
    d['jobs'].append( job_d )

d['count'] = count
pprint.pprint( d )

###
# the above print will show, for each job, a dct like:
#   {
#   u'args': (),
#   u'dt_created': datetime.datetime(2016, 5, 3, 14, 33, 56),
#   u'dt_ended': None,
#   u'dt_enqueued': datetime.datetime(2016, 5, 3, 14, 33, 56),
#   u'function': u'bell_code.tasks.metadata.run_create_metadata_only_object',
#   u'id': u'6cd95926-59d7-4825-9a87-814e4b527556',
#   u'kwargs': {u'accession_number': u'2015.11.45'},
#   u'origin': u'bell:job_queue',
#   u'traceback': None
#   }
###
