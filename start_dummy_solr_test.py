# -*- coding: utf-8 -*-

import json, os
from redis import Redis
from rq import Queue

""" Starts bell processing just to test indexing
    Assumes rq worker(s) started from bell_code directory.
    """


queue_name = os.environ.get(u'BELL_QUEUE_NAME')
q = Queue( queue_name, connection=Redis() )


with open( os.path.abspath(u'./tasks/test_data/raw_source_single_artist.json') ) as f:
    item_data_dict = json.loads( f.read() )
data = { u'item_data': item_data_dict, u'pid': u'bdr:10977' }


job = q.enqueue_call (
  func=u'tasks.indexer.build_metadata_only_solr_dict',
  args=( data, ),
  timeout=30
  )

