# -*- coding: utf-8 -*-

""" Overwrites jp2 datastream. """

import json, logging, os, pprint, urllib
import logging.handlers
import requests

""" Overwrites bad jp2 with new, good jp2. """


class Jp2Resaver( object ):

    def __init__( self ):
        # self.PIDS = json.loads( os.environ[u'BELL_ONEOFF__JP2_RESAVE_PIDS'] )
        self.PID = unicode( os.environ[u'BELL_ONEOFF__JP2_RESAVE_PID'] )
        self.JP2_TEMP_SAVE_DIR_PATH = unicode( os.environ[u'BELL_ONEOFF__JP2_TEMP_SAVE_DIR_PATH'] )
        self.PRIVATE_API_URL = unicode( os.environ[u'BELL_ONEOFF__PRIVATE_API_URL'] )
        self.MASTER_IMAGE_URL_PATTERN = unicode( os.environ[u'BELL_ONEOFF__MASTER_IMAGE_URL_PATTERN'] )

    def resave_jp2( self ):
        """ Controls making and overwriting """
        self._save_master_to_file( self.PID )
        self._make_new_jp2_from_ingested_master()
        self._validate_new_jp2()
        self._overwrite_datastream()
        print u'- pid `%s` done' % self.PID
        return

    def _save_master_to_file( self, pid ):
        """ Accesses fedora master and saves it to a file. """
        url = self.MASTER_IMAGE_URL_PATTERN % pid
        print u'- url, %s' % url
        save_path = u'%s/%s' % ( self.JP2_TEMP_SAVE_DIR_PATH, u'temp_' + pid + u'_.tif' )
        print u'- save_path, %s' % save_path
        r = requests.get( url, stream=True )
        if r.status_code == 200:
            with open( save_path, 'wb' ) as f:
                for chunk in r.iter_content(1024):
                    f.write(chunk)
        return




if __name__ == u'__main__':
    resaver = Jp2Resaver()
    resaver.resave_jp2()
