# -*- coding: utf-8 -*-

""" Overwrites bad jp2 datastream with new, good jp2. """

import imghdr, json, logging, os, pprint, urllib
import logging.handlers
import requests
from bell_code import bell_logger
from bell_code.tasks.image import ImageBuilder

logger = bell_logger.setup_logger()


class Jp2Resaver( object ):

    def __init__( self ):
        # self.PIDS = json.loads( os.environ[u'BELL_ONEOFF__JP2_RESAVE_PIDS'] )
        self.PID = unicode( os.environ[u'BELL_ONEOFF__JP2_RESAVE_PID'] )
        self.JP2_TEMP_SAVE_DIR_PATH = unicode( os.environ[u'BELL_ONEOFF__JP2_TEMP_SAVE_DIR_PATH'] )
        self.PRIVATE_API_URL = unicode( os.environ[u'BELL_ONEOFF__PRIVATE_API_URL'] )
        self.MASTER_IMAGE_URL_PATTERN = unicode( os.environ[u'BELL_ONEOFF__MASTER_IMAGE_URL_PATTERN'] )

    def resave_jp2( self ):
        """ Controls making and overwriting """
        temp_master_filepath = self._save_master_to_file( self.PID )
        master_filepath = self._fix_master_filename( temp_master_filepath )
        self._make_new_jp2_from_master( master_filepath )
        self._validate_new_jp2()
        self._overwrite_datastream()
        print u'- pid `%s` done' % self.PID
        return

    def _save_master_to_file( self, pid ):
        """ Accesses fedora master and saves it to a file. """
        url = self.MASTER_IMAGE_URL_PATTERN % pid
        print u'- url, %s' % url
        save_path = u'%s/%s' % ( self.JP2_TEMP_SAVE_DIR_PATH, u'temp_' + pid + u'_.tmp' )
        print u'- save_path, %s' % save_path
        r = requests.get( url, stream=True )
        if r.status_code == 200:
            with open( save_path, 'wb' ) as f:
                for chunk in r.iter_content(1024):
                    f.write(chunk)
        return save_path

    def _fix_master_filename( self, temp_filepath ):
        """ Examines file type and renames it with the proper extension."""
        image_type = imghdr.what( temp_filepath )
        print u'- image_type, `%s`' % image_type
        if image_type == u'jpeg':
            new_filepath = temp_filepath.replace( u'.tmp', u'.jpg' )
        else:
            new_filepath = temp_filepath.replace( u'.tmp', u'.tif' )
        os.rename( temp_filepath, new_filepath )
        return new_filepath

    def _make_new_jp2_from_master( self, master_filepath ):
        """ Creates jp2 and returns jp2_filepath and url. """
        if u'.jpg' in master_filepath:
            jp2_filepath = master_filepath.replace( u'.jpg', u'.jp2' )
        else:
            jp2_filepath = master_filepath.replace( u'.tif', u'.jp2' )
        image_builder = ImageBuilder( logger )
        image_builder.create_jp2( master_filepath, jp2_filepath )
        return

    ## end class Jp2Resaver()




if __name__ == u'__main__':
    resaver = Jp2Resaver()
    resaver.resave_jp2()
