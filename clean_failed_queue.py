# -*- coding: utf-8 -*-

""" Cleans up default rq failed-queue.
    Only cleans up jobs from a target queue.
    Useful for experimenting with rq & redis, while also indulging ocd tendencies.
    TODO: functionize to take target-queue as parameter
    """

import os, pprint, sys
import redis


rds = redis.Redis( u'localhost' )
FAILED_QUEUE_NAME = u'rq:queue:failed'
TARGET_QUEUE = os.environ.get( u'BELL_QUEUE_NAME' )  # only removing failed-queue jobs for target project
# TARGET_QUEUE = u'bell_work'  # only removing failed-queue jobs for target project


# check that failed-queue exists
if rds.type( FAILED_QUEUE_NAME ) == u'none':  # interesting: failed-queue will disappear if all it's members are deleted
  sys.exit()

# get members
assert rds.type( FAILED_QUEUE_NAME ) == u'list'
print u'- length of failed-queue starts at: %s' % rds.llen( FAILED_QUEUE_NAME )
members = rds.lrange( FAILED_QUEUE_NAME, 0, -1 )
print u'- failed-queue members...'; pprint.pprint( members ); print u'--'

# inspect failed jobs
for entry in members:
  assert type(entry) == str
  print u'- entry is: %s' % unicode( entry )
  job_id = u'rq:job:%s' % unicode( entry )
  print u'- job_id is: %s' % job_id

  # ensure failed-job really exists
  if rds.type( job_id ) == u'none':  # job was already deleted (i.e. interactive redis-cli experimentation), so remove it from failed-queue-list
    rds.lrem( FAILED_QUEUE_NAME, entry, num=0 )  # note count and value-name are reversed from redis-cli syntax... (redis-cli) > lrem "rq:queue:failed" 0 "06d0a46e-21ec-4fd3-92f8-f941f32101c4"

  # failed-job exists, but is it from our target-queue?
  elif rds.type( job_id ) == u'hash':
    info_dict = rds.hgetall( job_id )
    if info_dict[u'origin'] == TARGET_QUEUE:  # ok, delete the job, _and_ remove it from the failed-queue-list
      print u'- to delete...'; pprint.pprint( info_dict )
      rds.delete( job_id )
      rds.lrem( FAILED_QUEUE_NAME, entry, num=0 )
    else:
      print u'- job_id "%s" not mine; skipping it' % job_id

  print u'- length of failed-queue is now: %s' % rds.llen( FAILED_QUEUE_NAME ); print u'--'

# end
