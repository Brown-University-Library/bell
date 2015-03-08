# -*- coding: utf-8 -*-

""" Handles image-related tasks. """

import json, os, pprint, sys, urllib
import envoy, requests
from bell_code import bell_logger

class ImageLister( object ):
    """ Lists images that need to be added, and those that need to be updated. """

    def __init__( self, logger ):
        self.logger = logger
        self.IMAGE_LIST_PATH = unicode( os.environ[u'BELL_TASKS_IMGS__IMAGES_LIST_PATH'] )
        self.IMAGES_TO_PROCESS_OUTPUT_PATH = unicode( os.environ[u'BELL_TASKS_IMGS__IMAGES_TO_PROCESS_OUTPUT_PATH'] )

    def make_image_lists( self ):
        """ Saves two json list of accession_numbers, one for images to be added, and one for images to be updated.
            Called manuallly per readme. """
        logger.debug( u'in tasks.images.ImageLister.make_image_lists(); starting' )
        ( images_lst, images_to_add, images_to_update ) = self.setup()
        for image in images_lst:
            self.check_image( images_to_add, images_to_update )
        data = {
            u'datetime': unicode( datetime.datetime.now() ),
            u'count_images_to_add': len( images_to_add ),
            u'count_images_to_update': len( images_to_update ),
            u'lst_images_to_add': images_to_add,
            u'lst_images_to_update': images_to_update
            }
        self.output_listing( data )

    def setup( self ):
        """ Sets initial vars.
            Called by make_image_lists() """
        images_to_add = []
        images_to_update = []
        images_lst = []
        with open( self.IMAGE_LIST_PATH ) as f:
            dct = json.loads( f.read() )
        images_lst = dct[u'filelist']
        return ( images_lst, images_to_add, images_to_update )

    def check_image( self ):
        return u'foo'


## runners

logger = bell_logger.setup_logger()

def run_make_image_lists():
    """ Runner for make_image_lists()
        Called manually as per readme """
    lister = ImageLister( logger )
    lister.make_image_lists()
    return
