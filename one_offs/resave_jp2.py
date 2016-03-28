# -*- coding: utf-8 -*-

from __future__ import unicode_literals

""" Overwrites bad jp2 datastream with new, good jp2. """

import imghdr, json, logging, os, pprint, urllib
import logging.handlers
import requests
from bell_code import bell_logger
from bell_code.tasks.image import ImageBuilder

logger = bell_logger.setup_logger()


class Jp2Resaver( object ):

    def __init__( self ):
        # self.PIDS = json.loads( os.environ['BELL_ONEOFF__JP2_RESAVE_PIDS'] )
        self.PID = unicode( os.environ['BELL_ONEOFF__JP2_RESAVE_PID'] )
        self.TEMP_IMAGE_DIR_PATH = unicode( os.environ['BELL_ONEOFF__TEMP_IMAGE_DIR_PATH'] )
        self.TEMP_IMAGE_DIR_URL = unicode( os.environ['BELL_ONEOFF__TEMP_IMAGE_DIR_URL'] )
        self.MASTER_IMAGE_URL_PATTERN = unicode( os.environ['BELL_ONEOFF__MASTER_IMAGE_URL_PATTERN'] )
        self.PRIVATE_API_URL = unicode( os.environ['BELL_ONEOFF__PRIVATE_API_URL'] )

    def resave_jp2( self ):
        """ Controls making and overwriting """
        temp_master_filepath = self._save_master_to_file( self.PID )
        master_filepath = self._fix_master_filename( temp_master_filepath )
        jp2_filepath = self._make_new_jp2_from_master( master_filepath )
        self._overwrite_datastream( jp2_filepath )
        print '- pid `%s` done' % self.PID
        return

    def _save_master_to_file( self, pid ):
        """ Accesses fedora master and saves it to a file. """
        url = self.MASTER_IMAGE_URL_PATTERN % pid
        print '- url, %s' % url
        save_path = '%s/%s' % ( self.TEMP_IMAGE_DIR_PATH, 'temp_' + pid + '_.tmp' )
        print '- save_path, %s' % save_path
        r = requests.get( url, stream=True )
        if r.status_code == 200:
            with open( save_path, 'wb' ) as f:
                for chunk in r.iter_content(1024):
                    f.write(chunk)
        return save_path

    def _fix_master_filename( self, temp_filepath ):
        """ Examines file type and renames it with the proper extension."""
        image_type = imghdr.what( temp_filepath )
        print '- image_type, `%s`' % image_type
        if image_type == 'jpeg':
            new_filepath = temp_filepath.replace( '.tmp', '.jpg' )
        else:
            new_filepath = temp_filepath.replace( '.tmp', '.tif' )
        os.rename( temp_filepath, new_filepath )
        return new_filepath

    def _make_new_jp2_from_master( self, master_filepath ):
        """ Creates jp2 and returns jp2_filepath. """
        if '.jpg' in master_filepath:
            jp2_filepath = master_filepath.replace( '.jpg', '.jp2' )
        else:
            jp2_filepath = master_filepath.replace( '.tif', '.jp2' )
        image_builder = ImageBuilder( logger )
        image_builder.create_jp2( master_filepath, jp2_filepath )
        bytes = os.path.getsize( jp2_filepath )
        if bytes < 1000:
            raise Exception( 'Problem creating jp2' )
        return jp2_filepath

    def _overwrite_datastream( self, jp2_filepath ):
        """ Hits api. """
        jp2_filename = jp2_filepath.split('/')[-1]
        jp2_url = '%s/%s' % ( self.TEMP_IMAGE_DIR_URL, jp2_filename )
        logger.debug( 'in _overwrite_datastream(); jp2_url, `%s`' % jp2_url )
        logger.debug( 'in _overwrite_datastream(); self.PRIVATE_API_URL, `%s`' % self.PRIVATE_API_URL )
        params = { 'pid': self.PID, 'overwrite_content': 'yes' }
        params['content_streams'] = json.dumps([
            {'dsID': 'JP2', 'url': jp2_url} ])
        r = requests.put( self.PRIVATE_API_URL, data=params, verify=False )
        response_dict = {
            'r.url': self.PRIVATE_API_URL, 'r.status_code': r.status_code, 'r.content': r.content.decode('utf-8', 'replace') }
        print 'response_dict...'; pprint.pprint( response_dict )

    ## end class Jp2Resaver()




if __name__ == '__main__':
    resaver = Jp2Resaver()
    resaver.resave_jp2()
