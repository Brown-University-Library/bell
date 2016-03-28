# -*- coding: utf-8 -*-

from __future__ import unicode_literals

"""
- Old code that assumed I'd run this start command and everything would automagically happen.
- Revising approach March 2015. Since I run this so infrequently, I'm changing to the approach
    of manually running a series of focused scripts, as outlined in the readme.md flow
"""

# import os
# from redis import Redis
# from rq import Queue

# """ Starts bell processing
#     Assumes rq worker(s) started from bell_code directory.
#     """


# queue_name = os.environ.get('BELL_QUEUE_NAME')
# q = Queue( queue_name, connection=Redis() )
# job = q.enqueue_call (
#   func='bell_code.tasks.check_environment.ensure_redis',
#   args=(),
#   timeout=30
#   )
