# -*- coding: utf-8 -*-

""" Assumes pid to metadata-only item.
    Assumes path to web-accessible master image.
    Creates jp2.
    Adds jp2 and master to given bdr-pid. """

import imghdr, json, logging, os, pprint, urllib
import logging.handlers
import requests
from bell_code import bell_logger
from bell_code.tasks.image import ImageBuilder

logger = bell_logger.setup_logger()


class ImageAdder( object ):

    def __init__( self ):
        self.PID = unicode( os.environ[u'BELL_ONEOFF_ADD_IMAGE__PID'] )
        self.TEMP_IMAGE_DIR_PATH = unicode( os.environ[u'BELL_ONEOFF_ADD_IMAGE__TEMP_IMAGE_DIR_PATH'] )
        self.TEMP_IMAGE_DIR_URL = unicode( os.environ[u'BELL_ONEOFF_ADD_IMAGE__TEMP_IMAGE_DIR_URL'] )
        self.MASTER_IMAGE_URL_PATTERN = unicode( os.environ[u'BELL_ONEOFF_ADD_IMAGE__MASTER_IMAGE_URL_PATTERN'] )
        self.PRIVATE_API_URL = unicode( os.environ[u'BELL_ONEOFF_ADD_IMAGE__PRIVATE_API_URL'] )

    def add_image( self ):
        """ Controls making and overwriting """
        temp_master_filepath = self._save_master_to_file( self.PID )
        master_filepath = self._fix_master_filename( temp_master_filepath )
        jp2_filepath = self._make_new_jp2_from_master( master_filepath )
        self._overwrite_datastream( jp2_filepath )
        print u'- pid `%s` added to bdr' % self.PID
        self._update_index()
        return

    def _save_master_to_file( self, pid ):
        """ Accesses fedora master and saves it to a file. """
        url = self.MASTER_IMAGE_URL_PATTERN % pid
        print u'- url, %s' % url
        save_path = u'%s/%s' % ( self.TEMP_IMAGE_DIR_PATH, u'temp_' + pid + u'_.tmp' )
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

    def _overwrite_datastream( self, jp2_filepath ):
        """ Hits api. """
        jp2_filename = jp2_filepath.split(u'/')[-1]
        jp2_url = u'%s/%s' % ( self.TEMP_IMAGE_DIR_URL, jp2_filename )
        logger.debug( u'in _overwrite_datastream(); jp2_url, `%s`' % jp2_url )
        logger.debug( u'in _overwrite_datastream(); self.PRIVATE_API_URL, `%s`' % self.PRIVATE_API_URL )
        params = { u'pid': self.PID, u'overwrite_content': u'yes' }
        params[u'content_streams'] = json.dumps([
            {u'dsID': u'JP2', u'url': jp2_url} ])
        r = requests.put( self.PRIVATE_API_URL, data=params, verify=False )
        response_dict = {
            u'r.url': self.PRIVATE_API_URL, u'r.status_code': r.status_code, u'r.content': r.content.decode(u'utf-8', u'replace') }
        print u'response_dict...'; pprint.pprint( response_dict )

    def _update_index( self ):
        """ Updates custom-index. """
        # next_task = u'tasks.indexer.build_metadata_and_image_solr_dict'
        # next_task = u'tasks.indexer.post_to_solr'
        1/0
        return

    ## end class ImageAdder()




if __name__ == u'__main__':
    adder = ImageAdder()
    adder.add_image()
