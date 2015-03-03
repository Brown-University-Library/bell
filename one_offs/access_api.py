# -*- coding: utf-8 -*-

""" Accesses authenticated api. """

import os, pprint
import requests
from bell_code import bell_logger

logger = bell_logger.setup_logger()


class Accessor( object ):

    def __init__( self ):
        self.API_URL = unicode( os.environ[u'BELL_ONEOFF_ACCESS_API__API_URL'] )
        self.AUTH_IDENTITY = unicode( os.environ[u'BELL_ONEOFF_ACCESS_API__AUTH_IDENTITY'] )
        self.AUTH_KEY = unicode( os.environ[u'BELL_ONEOFF_ACCESS_API__AUTH_KEY'] )
        self.PID = unicode( os.environ[u'BELL_ONEOFF_ACCESS_API__PID'] )

    def run_GET( self ):
        """ Explores request. """
        params = {
            u'pid': self.PID,
            u'identity': self.AUTH_IDENTITY,
            u'authentication_key': self.AUTH_KEY }
        r = requests.get( self.API_URL, params=params )
        pprint.pprint( r.json )




if __name__ == u'__main__':
    accr = Accessor()
    accr.run_GET()
