# -*- coding: utf-8 -*-

""" Overwrites jp2 datastream. """

import json, logging, os, pprint, urllib
import logging.handlers
import requests


class Jp2Resaver( object ):

    def __init__( self ):
        self.PID = os.environ[u'BELL_ONEOFF__JP2_RESAVE_PID']
        self.JP2_TEMP_SAVE_DIR_PATH = os.environ[u'BELL_ONEOFF__JP2_TEMP_SAVE_DIR_PATH']
        self.PRIVATE_API_URL = os.environ[u'BELL_ONEOFF__PRIVATE_API_URL']

    def resave_jp2( self ):
        """ z """
        print u'foo'




if __name__ == u'__main__':
    resaver = Jp2Resaver()
    resaver.resave_jp2()
