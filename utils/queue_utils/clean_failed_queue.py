""" Cleans up default rq failed-queue.
    Only cleans up jobs from a target queue.
    Useful for experimenting with rq & redis, while also indulging ocd tendencies.
    TODO: functionize to take target-queue as parameter
    """

import os, pprint, sys
import redis


rds = redis.Redis( 'localhost' )
FAILED_QUEUE_NAME = 'rq:queue:failed'
TARGET_QUEUE = os.environ.get( 'BELL_QUEUE_NAME' )  # only removing failed-queue jobs for target project


# check that failed-queue exists
if rds.type( FAILED_QUEUE_NAME ) == b'none':  # interesting: failed-queue will disappear if all it's members are deleted
  sys.exit()

# get members
assert rds.type( FAILED_QUEUE_NAME ) == b'list'
print('- length of failed-queue starts at: %s' % rds.llen( FAILED_QUEUE_NAME ))
members = rds.lrange( FAILED_QUEUE_NAME, 0, -1 )
print('- failed-queue members...'); pprint.pprint( members ); print('--')

# inspect failed jobs
for entry in members:
  assert type(entry) == bytes
  print('- entry is: %s' % entry)
  job_id = 'rq:job:%s' % entry
  print('- job_id is: %s' % job_id)

  # ensure failed-job really exists
  if rds.type( job_id ) == b'none':  # job was already deleted (i.e. interactive redis-cli experimentation), so remove it from failed-queue-list
    rds.lrem( FAILED_QUEUE_NAME, entry, num=0 )  # note count and value-name are reversed from redis-cli syntax... (redis-cli) > lrem "rq:queue:failed" 0 "06d0a46e-21ec-4fd3-92f8-f941f32101c4"

  # failed-job exists, but is it from our target-queue?
  elif rds.type( job_id ) == b'hash':
    info_dict = rds.hgetall( job_id )
    if info_dict['origin'] == TARGET_QUEUE:  # ok, delete the job, _and_ remove it from the failed-queue-list
      print('- to delete...'); pprint.pprint( info_dict )
      rds.delete( job_id )
      rds.lrem( FAILED_QUEUE_NAME, entry, num=0 )
    else:
      print('- job_id "%s" not mine; skipping it' % job_id)

  print('- length of failed-queue is now: %s' % rds.llen( FAILED_QUEUE_NAME )); print('--')

# end
