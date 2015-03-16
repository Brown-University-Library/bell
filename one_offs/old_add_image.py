# -*- coding: utf-8 -*-

""" Assumes pid to metadata-only item.
    Assumes path to web-accessible master image.
    Creates jp2.
    Adds jp2 and master to given bdr-pid.
    Does _not_ update custom index. """

import imghdr, json, logging, os, pprint
import logging.handlers
import requests
from bell_code import bell_logger
from bell_code.tasks.image import ImageBuilder

logger = bell_logger.setup_logger()


class ImageAdder( object ):

    def __init__( self ):
        self.PID = unicode( os.environ[u'BELL_ONEOFF_ADD_IMAGE__PID'] )
        self.RAW_MASTER_FILENAME = unicode( os.environ[u'BELL_ONEOFF_ADD_IMAGE__RAW_MASTER_FILENAME'] )
        self.TEMP_IMAGE_DIR_PATH = unicode( os.environ[u'BELL_ONEOFF_ADD_IMAGE__TEMP_IMAGE_DIR_PATH'] )
        self.TEMP_IMAGE_DIR_URL = unicode( os.environ[u'BELL_ONEOFF_ADD_IMAGE__TEMP_IMAGE_DIR_URL'] )
        self.PRIVATE_API_URL = unicode( os.environ[u'BELL_ONEOFF_ADD_IMAGE__PRIVATE_API_URL'] )

    def add_image( self ):
        """ Controls making and overwriting """
        temp_master_filepath = u'%s/%s' % ( self.TEMP_IMAGE_DIR_PATH, self.RAW_MASTER_FILENAME )
        master_filepath = self._fix_master_filename( temp_master_filepath )
        jp2_filepath = self._make_new_jp2_from_master( master_filepath )
        ( jp2_url, master_url ) = self._make_file_urls( jp2_filepath, master_filepath )
        self._overwrite_datastream( jp2_url, master_url )
        print u'- pid `%s` added to bdr' % self.PID
        return

    def _fix_master_filename( self, temp_filepath ):
        """ Examines file type and renames it with the proper extension.
            Replaces spaces with underscores.
            Returns new filepath. """
        temp_filepath_b = temp_filepath.replace( u' ', u'_' )
        os.rename( temp_filepath, temp_filepath_b )
        image_type = imghdr.what( temp_filepath_b )
        print u'- image_type, `%s`' % image_type
        if image_type == u'jpeg':
            new_filepath = temp_filepath_b.replace( u'.tmp', u'.jpg' )
        else:
            new_filepath = temp_filepath_b.replace( u'.tmp', u'.tif' )
        os.rename( temp_filepath_b, new_filepath )
        return new_filepath

    def _make_new_jp2_from_master( self, master_filepath ):
        """ Creates jp2 and returns jp2_filepath. """
        if u'.jpg' in master_filepath:
            jp2_filepath = master_filepath.replace( u'.jpg', u'.jp2' )
        else:
            jp2_filepath = master_filepath.replace( u'.tif', u'.jp2' )
        image_builder = ImageBuilder( logger )
        image_builder.create_jp2( master_filepath, jp2_filepath )
        bytes = os.path.getsize( jp2_filepath )
        if bytes < 1000:
            raise Exception( u'Problem creating jp2' )
        return jp2_filepath

    def _make_file_urls( self, jp2_filepath, master_filepath ):
        """ Returns urls for jp2 and master files. """
        jp2_filename = jp2_filepath.split(u'/')[-1]
        jp2_url = u'%s/%s' % ( self.TEMP_IMAGE_DIR_URL, jp2_filename )
        master_filename = master_filepath.split(u'/')[-1]
        master_url = u'%s/%s' % ( self.TEMP_IMAGE_DIR_URL, master_filename )
        return ( jp2_url, master_url )

    def _overwrite_datastream( self, jp2_url, master_url ):
        """ Hits api. """
        params = { u'pid': self.PID, u'overwrite_content': u'yes' }
        params[u'content_streams'] = json.dumps([
            { u'dsID': u'MASTER', u'url': master_url },
            { u'dsID': u'JP2', u'url': jp2_url }
            ])
        r = requests.put( self.PRIVATE_API_URL, data=params, verify=False )
        response_dict = { u'r.url': self.PRIVATE_API_URL, u'r.status_code': r.status_code, u'r.content': r.content.decode(u'utf-8', u'replace') }
        print u'response_dict...'; pprint.pprint( response_dict )
        return response_dict

    ## end class ImageAdder()




if __name__ == u'__main__':
    adder = ImageAdder()
    adder.add_image()
