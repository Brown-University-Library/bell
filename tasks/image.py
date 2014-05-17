# -*- coding: utf-8 -*-

""" Handles image-related tasks. """

import json, os, sys
import requests
from bell_code import bell_logger
from bell_code.tasks import task_manager


class ImageHandler( object ):
    """ Handles image-related tasks. """

    def __init__( self, data, logger ):
        self.data = data
        self.logger = logger

    def check_create_image( self ):
        """ Determines whether an image will need to be ingested. """
        create_image = False
        master_filename_raw = self.data[u'item_data'][u'object_image_scan_filename']
        if len( master_filename_raw.strip() ) > 0:
            if self._image_already_ingested() == False:
                if self._image_filepath_good( master_filename_raw ) == True:
                    create_image = True
        task_manager.determine_next_task( current_task=unicode(sys._getframe().f_code.co_name), logger=self.logger,
            data={ u'item_data': self.data[u'item_data'], u'pid': self.data[u'pid'], u'create_image': create_image } )
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

    # end class ImageHandler()


## runners ##

logger = bell_logger.setup_logger()

def run_check_create_image( data ):
    """ Runner for check_create_image().
        Called by queue-job triggered by tasks.task_manager.determine_next_task(). """
    assert sorted( data.keys() ) == [u'item_data', u'pid'], sorted( data.keys() )
    ih = ImageHandler( data, logger )
    ih.check_create_image()
    return
