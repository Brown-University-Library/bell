# -*- coding: utf-8 -*-

""" Handles image-related tasks. """

import datetime, json, os, pprint, sys, time, urllib
import envoy, requests
from bell_code import bell_logger

class ImageLister( object ):
    """ Lists images that need to be added, and those that need to be updated. """

    def __init__( self, logger ):
        self.logger = logger
        self.IMAGE_LIST_PATH = unicode( os.environ[u'BELL_TASKS_IMGS__IMAGES_LIST_PATH'] )
        self.IMAGES_TO_PROCESS_OUTPUT_PATH = unicode( os.environ[u'BELL_TASKS_IMGS__IMAGES_TO_PROCESS_OUTPUT_PATH'] )
        self.DATA_DCT_PATH = unicode( os.environ[u'BELL_TASKS_IMGS__ACCESSION_NUMBER_TO_DATA_DICT_PATH'] )
        self.PID_DCT_PATH = unicode( os.environ[u'BELL_TASKS_IMGS__ACCESSION_NUMBER_TO_PID_DICT_PATH'] )
        self.API_URL = unicode( os.environ[u'BELL_TASKS_IMGS__API_ROOT_URL'] )

    def make_image_lists( self ):
        """ Saves two json list of accession_numbers, one for images to be added, and one for images to be updated.
            Called manuallly per readme. """
        logger.debug( u'in tasks.images.ImageLister.make_image_lists(); starting' )
        ( accession_data_dct, accession_pid_dct, images_lst, images_to_add, images_to_update ) = self.setup()
        filename_to_data_dct = self.make_filename_to_data_dct( accession_data_dct, accession_pid_dct, images_lst )
        for ( i, image_filename ) in enumerate(sorted( filename_to_data_dct.keys()) ):
            api_dct = self.get_api_data( image_filename, filename_to_data_dct )
            self.check_api_data( api_dct, image_filename, images_to_add, images_to_update )
            if i+1 >= 500:
                break
        self.output_listing( images_to_add, images_to_update )
        return

    def setup( self ):
        """ Sets initial vars.
            Called by make_image_lists() """
        ( images_to_add, images_to_update, images_lst ) = ( [], [], [] )
        with open( self.IMAGE_LIST_PATH ) as f:
            dct = json.loads( f.read() )
        images_lst = dct[u'filelist']
        with open( self.DATA_DCT_PATH ) as f2:
            dct2 = json.loads( f2.read() )
        accession_data_dct = dct2[u'items']
        with open( self.PID_DCT_PATH ) as f3:
            dct3 = json.loads( f3.read() )
        accession_pid_dct = dct3[u'final_accession_pid_dict']
        return ( accession_data_dct, accession_pid_dct, images_lst, images_to_add, images_to_update )

    def make_filename_to_data_dct( self, accession_data_dct, accession_pid_dct, images_lst ):
        """ Returns dct of filename keys to data-dct of accession-number and pid.
            Called by: make_image_lists() """
        filename_to_data_dct = {}
        for image_filename in images_lst:
            extension_idx = image_filename.rfind( u'.' )
            if not u'.tif' in image_filename[extension_idx:]:  # removes a few non-image entries
                continue
            non_extension_filename = image_filename[0:extension_idx]
            for ( accession_number_key, data_dct_value ) in accession_data_dct.items():
                if image_filename == data_dct_value[u'object_image_scan_filename'] or non_extension_filename == data_dct_value[u'object_image_scan_filename']:
                    filename_to_data_dct[image_filename] = { u'accession_number': accession_number_key, u'pid': accession_pid_dct[accession_number_key] }
        logger.debug( u'in tasks.images.ImageLister.make_filename_to_data_dct(); filename_to_data_dct, `%s`' % pprint.pformat(filename_to_data_dct) )
        return filename_to_data_dct

    def get_api_data( self, image_filename, filename_to_data_dct ):
        """ Makes api call.
            Called by make_image_lists() """
        pid = filename_to_data_dct[image_filename][u'pid']
        item_api_url = u'%s/%s/' % ( self.API_URL, pid )
        self.logger.debug( u'in tasks.images.ImageLister.check_image(); item_api_url, %s' % item_api_url )
        time.sleep( 2 )
        r = requests.get( item_api_url, verify=False )
        api_dct = r.json()
        return api_dct

    def check_api_data( self, api_dct, image_filename, images_to_add, images_to_update ):
        """ Looks up image-filename via public api; stores whether image exists or not.
            Called by make_image_lists() """
        image_already_ingested = True
        if u'JP2' in api_dct[u'links'][u'content_datastreams'].keys() or  u'jp2' in api_dct[u'rel_content_models_ssim']:
            pass
        else:
            image_already_ingested = False
        if image_already_ingested:
            images_to_update.append( image_filename )
        else:
            images_to_add.append( image_filename )
        return

    def output_listing( self, images_to_add, images_to_update ):
        """ Saves json file.
            Called by make_image_lists() """
        data = {
            u'datetime': unicode( datetime.datetime.now() ),
            u'count_images': len( images_to_add ) + len( images_to_update ),
            u'count_images_to_add': len( images_to_add ),
            u'count_images_to_update': len( images_to_update ),
            u'lst_images_to_add': images_to_add,
            u'lst_images_to_update': images_to_update }
        jsn = json.dumps( data, indent=2, sort_keys=True )
        with open( self.IMAGES_TO_PROCESS_OUTPUT_PATH, u'w' ) as f:
            f.write( jsn )
        return


## runners

logger = bell_logger.setup_logger()

def run_make_image_lists():
    """ Runner for make_image_lists()
        Called manually as per readme """
    lister = ImageLister( logger )
    lister.make_image_lists()
    return
