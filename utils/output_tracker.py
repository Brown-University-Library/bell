# -*- coding: utf-8 -*-

import json, pprint
import redis

""" Pretty-prints bell:tracker data. """


rds = redis.StrictRedis( host='localhost', port=6379, db=0 )
dct = rds.hgetall( u'bell:tracker' )

dct2 = {}
for key, value in dct.items():
  value_data = json.loads( value )
  dct2[key] = value_data

pprint.pprint( dct2 )