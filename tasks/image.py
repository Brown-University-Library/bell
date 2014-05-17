# -*- coding: utf-8 -*-

""" Handles image-related tasks. """

import json, os, sys
import requests
from bell_code import bell_logger
from bell_code.tasks import task_manager
from bell_code.tasks.fedora_parts_builder import ImageBuilder


class ImageHandler( object ):
    """ Handles image-related tasks. """

    def __init__( self, data, logger ):
        self.data = data
        self.logger = logger

    def check_create_image( self ):
        """ Determines whether an image will need to be ingested.
            Returns boolean.
            Called by runner. """
        create_image = False
        master_filename_raw = self.data[u'item_data'][u'object_image_scan_filename']
        if len( master_filename_raw.strip() ) > 0:
            if self._image_already_ingested() == False:
                if self._image_filepath_good( master_filename_raw ) == True:
                    create_image = True
        return create_image

    def check_update_image( self ):
        """ Determines whether an image needs to be updated.
            Returns boolean.
            Called by runner.
            TODO: let's say for an image to be updated,
                  the source item-data dict json will have to have both
                  a filename, and an entry: "custom_field_UPDATE_IMAGE": "true",
                  and, of course, the filepath has to be valid.
                  """
        update_image = True
        return update_image

    def make_jp2( self ):
        """ Makes jp2.
            Called by runner. """
        MASTER_IMAGES_DIR_PATH = unicode( os.environ.get(u'BELL_IMAGE__MASTER_IMAGES_DIR_PATH') )
        JP2_IMAGES_DIR_PATH = unicode( os.environ.get(u'BELL_IMAGE__JP2_IMAGES_DIR_PATH') )
        master_filename_raw = self.data[u'item_data'][u'object_image_scan_filename']
        master_source_filepath = self._make_source_filepath( MASTER_IMAGES_DIR_PATH, master_filename_raw )
        jp2_destination_filepath = self._make_destination_filepath( JP2_IMAGES_DIR_PATH, master_filename_raw )
        image_builder = ImageBuilder()
        image_builder.create_jp2( source_filepath, destination_filepath )
        return

    ## helpers ##

    def _image_already_ingested( self ):
        """ Checks api to see if an image already exists.
            Returns boolean.
            Called by check_create_image(). """
        ITEM_API_ROOT = os.environ.get(u'BELL_IMAGE__ITEM_API_ROOT')
        image_already_ingested = True
        item_api_url = u'%s/%s/' % ( ITEM_API_ROOT, self.data[u'pid'] )
        self.logger.debug( u'in tasks.image._image_already_ingested(); item_api_url, %s' % item_api_url )
        r = requests.get( item_api_url, verify=False )
        d = r.json()
        if u'JP2' in d[u'links'][u'content_datastreams'].keys() or  u'jp2' in d[u'rel_content_models_ssim']:
            pass
        else:
            image_already_ingested = False
        return image_already_ingested

    def _image_filepath_good( self, master_filename_raw ):
        """ Checks to see if we actually have an image file to ingest.
            Returns boolean.
            Called by check_create_image(). """
        MASTER_IMAGES_DIR_PATH = unicode( os.environ.get(u'BELL_IMAGE__MASTER_IMAGES_DIR_PATH') )
        filepath_check = False
        possible_path_a = u'%s/%s' % ( MASTER_IMAGES_DIR_PATH, master_filename_raw )
        if os.path.isfile( possible_path_a ):
            filepath_check = True
        else:
            possible_path_b = u'%s/%s' % ( MASTER_IMAGES_DIR_PATH, master_filename_raw.strip() )
            if os.path.isfile( possible_path_b ):
                filepath_check = True
        return filepath_check

    def _make_source_filepath( self, MASTER_IMAGES_DIR_PATH, master_filename_raw ):
        """ Creates and returns master image source filepath.
            Called by make_jp2().
            TODO: merge with _image_filepath_good() """
        source_path = None
        possible_path_a = u'%s/%s' % ( MASTER_IMAGES_DIR_PATH, master_filename_raw )
        if os.path.isfile( possible_path_a ):
            source_path = possible_path_a
        else:
            possible_path_b = u'%s/%s' % ( MASTER_IMAGES_DIR_PATH, master_filename_raw.strip() )
            if os.path.isfile( possible_path_b ):
                source_path = possible_path_b
        self.logger.info( u'in tasks.image._make_source_filepath(); master source filepath, %s' % source_path )
        return source_path

    def _make_destination_filepath( JP2_IMAGES_DIR_PATH, master_filename_raw ):
        """ Creates and returns jp2 destination filepath.
            Called by make_jp2(). """
        temp_jp2_filename = master_filename_raw.strip().replace( u' ', u'_' )
        jp2_filename = temp_jp2_filename[0:-4] + u'.jp2'
        destination_filepath = u'%s/%s' % ( JP2_IMAGES_DIR_PATH, jp2_filename )
        self.logger.info( u'in tasks.image._make_destination_filepath(); jp2 destination_filepath, %s' % destination_filepath )
        return destination_filepath

    # end class ImageHandler()


## runners ##

logger = bell_logger.setup_logger()

def run_check_create_image( data ):
    """ Runner for check_create_image().
        Called by queue-job triggered by tasks.task_manager.determine_next_task(). """
    assert sorted( data.keys() ) == [u'item_data', u'pid'], sorted( data.keys() )
    ih = ImageHandler( data, logger )
    create_image = ih.check_create_image()
    task_manager.determine_next_task( current_task=unicode(sys._getframe().f_code.co_name), logger=logger,
        data={ u'item_data': data[u'item_data'], u'pid': data[u'pid'], u'create_image': create_image } )
    return

def run_check_update_image( data ):
    """ Runner for check_update_image().
        Called by queue-job triggered by tasks.task_manager.determine_next_task(). """
    assert sorted( data.keys() ) == [u'item_data', u'pid'], sorted( data.keys() )
    ih = ImageHandler( data, logger )
    update_image = ih.check_update_image()
    task_manager.determine_next_task( current_task=unicode(sys._getframe().f_code.co_name), logger=logger,
        data={ u'item_data': data[u'item_data'], u'pid': data[u'pid'], u'update_image': update_image } )
    return

def run_make_jp2( data ):
    """ Runner for create_jp2().
        Called by queue-job triggered by tasks.task_manager.determine_next_task(). """
    assert sorted( data.keys() ) == [u'item_data', u'pid', u'update_image'], sorted( data.keys() )
    ih = ImageHandler( data, logger )
    task_manager.determine_next_task( current_task=unicode(sys._getframe().f_code.co_name), logger=logger,
        data=data )
    return
