# -*- coding: utf-8 -*-

from __future__ import unicode_literals

""" Clear out target job queue.
    Useful for experimenting with rq & redis, starting queue from scratch.
    TODO: functionize to take target-queue as parameter
    """

import os, pprint, sys
import redis


rds = redis.Redis( 'localhost' )
TARGET_QUEUE = os.environ.get( 'BELL_QUEUE_NAME' )
print '- target-queue is: %s' % TARGET_QUEUE; print '--'
FULL_QUEUE_NAME = 'rq:queue:%s' % TARGET_QUEUE
print '- full-queue-name is: %s' % FULL_QUEUE_NAME; print '--'


# check whether queue exists
if rds.type( FULL_QUEUE_NAME ) == 'none':
  print '- queue `%s` does not exist' % FULL_QUEUE_NAME; print '--'
  sys.exit()

# get members
assert rds.type( FULL_QUEUE_NAME ) == 'list'
print '- length of target-queue starts at: %s' % rds.llen( FULL_QUEUE_NAME )
members = rds.lrange( FULL_QUEUE_NAME, 0, -1 )
print '- target-queue members...'; pprint.pprint( members ); print '--'

# inspect jobs
for entry in members:
  assert type(entry) == str
  print '- entry is: %s' % unicode( entry )
  job_id = 'rq:job:%s' % unicode( entry )
  print '- job_id is: %s' % job_id

  # ensure target-queue-job really exists
  if rds.type( job_id ) == 'none':  # job was already deleted (i.e. interactive redis-cli experimentation), so remove it from target-queue-list
    print '- job_id  was not found; entry still removed from queue'
  else:
    # remove the job
    rds.delete( job_id )
    print '- job id removed'

  # remove the queue's reference
  rds.lrem( FULL_QUEUE_NAME, entry, num=0 )  # note count and value-name are reversed from redis-cli syntax... (redis-cli) > lrem "rq:queue:NAME" 0 "06d0a46e-21ec-4fd3-92f8-f941f32101c4"
  print '- job reference removed from queue'

  print '- length of target-queue is now: %s' % rds.llen( FULL_QUEUE_NAME ); print '--'

# end
