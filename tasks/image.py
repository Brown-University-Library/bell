# -*- coding: utf-8 -*-

""" Handles image-related tasks. """

import json, os, pprint, sys, urllib
import envoy, requests
from bell_code import bell_logger
from bell_code.tasks import task_manager
# from bell_code.tasks.fedora_parts_builder import ImageBuilder


class ImageHandler( object ):
    """ Handles image-related tasks. """

    def __init__( self, data, logger ):
        self.data = data
        self.logger = logger

    ## check_create_image()

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

    ## check_update_image()

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

    ## make_jp2()

    def make_jp2( self ):
        """ Makes jp2.
            Called by runner. """
        MASTER_IMAGES_DIR_PATH = unicode( os.environ.get(u'BELL_IMAGE__MASTER_IMAGES_DIR_PATH') )
        JP2_IMAGES_DIR_PATH = unicode( os.environ.get(u'BELL_IMAGE__JP2_IMAGES_DIR_PATH') )
        master_filename_raw = self.data[u'item_data'][u'object_image_scan_filename']
        master_source_filepath = self._make_source_filepath( MASTER_IMAGES_DIR_PATH, master_filename_raw )
        jp2_destination_filepath = self._make_destination_filepath( JP2_IMAGES_DIR_PATH, master_filename_raw )
        image_builder = ImageBuilder( self.logger )
        image_builder.create_jp2( master_source_filepath, jp2_destination_filepath )
        return

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

    def _make_destination_filepath( self, JP2_IMAGES_DIR_PATH, master_filename_raw ):
        """ Creates and returns jp2 destination filepath.
            Called by make_jp2(). """
        temp_jp2_filename = master_filename_raw.strip().replace( u' ', u'_' )
        jp2_filename = temp_jp2_filename[0:-4] + u'.jp2'
        destination_filepath = u'%s/%s' % ( JP2_IMAGES_DIR_PATH, jp2_filename )
        self.logger.info( u'in tasks.image._make_destination_filepath(); jp2 destination_filepath, %s' % destination_filepath )
        return destination_filepath

    ## add_image_datastream()

    def add_image_datastream( self ):
        """ Assembles params and hits api.
            Called by runner. """
        ( PRIVATE_ITEMS_API_URL, MASTER_IMAGES_DIR_URL, JP2_IMAGES_DIR_URL ) = self._get_image_datastream_settings()
        ( master_url, jp2_url ) = self._prep_image_urls( MASTER_IMAGES_DIR_URL, JP2_IMAGES_DIR_URL )
        params_dict = self._prep_image_datastream_params( master_url, jp2_url )
        r = requests.put( PRIVATE_ITEMS_API_URL, data=params_dict, verify=False )
        self.logger.debug( u'in image.add_image_datastream(); r.status_code, `%s`; r.content, `%s`' % (r.status_code, r.content.decode(u'utf-8')) )
        return

    def _get_image_datastream_settings( self ):
        """ Grabs environmental variables for api call.
            Returns tuple.
            Called by add_image_datastream() """
        PRIVATE_ITEMS_API_URL = unicode( os.environ.get(u'BELL_IMAGE__PRIVATE_API_URL') )
        MASTER_IMAGES_DIR_URL = unicode( os.environ.get(u'BELL_IMAGE__MASTER_IMAGES_DIR_URL') )
        JP2_IMAGES_DIR_URL = unicode( os.environ.get(u'BELL_IMAGE__JP2_IMAGES_DIR_URL') )
        return_tuple = ( PRIVATE_ITEMS_API_URL, MASTER_IMAGES_DIR_URL, JP2_IMAGES_DIR_URL )
        self.logger.debug( u'in image._get_image_datastream_settings(); return_tuple, `%s`' % unicode(return_tuple) )
        return return_tuple

    def _prep_image_urls( self, MASTER_IMAGES_DIR_URL, JP2_IMAGES_DIR_URL ):
        """ Prepars master and jp2 image_urls for the api (for fedora to grab).
            Returns tuple of master_url and jp2_url.
            Called by add_image_datastream(). """
        master_filename_raw = self.data[u'item_data'][u'object_image_scan_filename']  # can include spaces
        master_filename_utf8 = master_filename_raw.encode( u'utf-8' )  # for urllib
        master_filename_urllib_encoded = urllib.quote( master_filename_utf8 ).decode( u'utf-8' )
        master_url = u'%s/%s' % ( MASTER_IMAGES_DIR_URL, master_filename_urllib_encoded )
        temp_jp2_filename = master_filename_raw.replace( u' ', u'_' )
        jp2_filename = temp_jp2_filename[0:-4] + u'.jp2'
        jp2_url = u'%s/%s' % ( JP2_IMAGES_DIR_URL, jp2_filename )
        return ( master_url, jp2_url )

    def _prep_image_datastream_params( self, master_url, jp2_url ):
        """ Prepares and returns params_dict for api call.
            Called by add_image_datastream(). """
        params = { u'pid': self.data[u'pid'] }
        params[u'content_streams'] = json.dumps([
            { u'dsID': u'MASTER', u'url': master_url },
            { u'dsID': u'JP2', u'url': jp2_url }
            ])
        if self.data[u'update_image'] == True:
            params[u'overwrite'] = True
        self.logger.debug( u'in image._prep_image_datastream_params(); params, `%s`' % pprint.pformat(params) )
        return params

    # end class ImageHandler()


class ImageBuilder( object ):
    """ Handles repo new-object datastream assignment.
        Handles repo new-object rels-int assignment.
        Creates jp2. """

    def __init__( self, logger ):
        self.logger = logger

    ## create jp2 ##

    def create_jp2( self, source_filepath, destination_filepath ):
        """ Creates jp2.
            Called by tasks.ImageHandler.make_jp2()
            TODO: consider merging this into that class. """
        self.logger.debug( u'in image._create_jp2(); source_filepath, %s' % source_filepath )
        self.logger.debug( u'in image._create_jp2(); destination_filepath, %s' % destination_filepath )
        KAKADU_COMMAND_PATH = unicode( os.environ.get(u'BELL_IMAGE__KAKADU_COMMAND_PATH') )
        CONVERT_COMMAND_PATH = unicode( os.environ.get(u'BELL_IMAGE__CONVERT_COMMAND_PATH') )
        if source_filepath.split( u'.' )[-1] == u'tif':
            self.logger.debug( u'in image._create_jp2(); in `tif` handling' )
            self._create_jp2_from_tif( KAKADU_COMMAND_PATH, source_filepath, destination_filepath )
        elif source_filepath.split( u'.' )[-1] == u'jpg':
            self.logger.debug( u'in image._create_jp2(); in `jpg` handling' )
            self._create_jp2_from_jpg( CONVERT_COMMAND_PATH, KAKADU_COMMAND_PATH, source_filepath, destination_filepath )
        return

    def _create_jp2_from_tif( self, KAKADU_COMMAND_PATH, source_filepath, destination_filepath ):
        """ Creates jp2 directly. """
        cleaned_source_filepath = source_filepath.replace( u' ', u'\ ' )
        cmd = u'%s -i "%s" -o "%s" Creversible=yes -rate -,1,0.5,0.25 Clevels=12' % (
            KAKADU_COMMAND_PATH, cleaned_source_filepath, destination_filepath )
        self.logger.info( u'in image._create_jp2_from_tif(); cmd, %s' % cmd )
        r = envoy.run( cmd.encode(u'utf-8', u'replace') )  # envoy requires a non-unicode string
        self.logger.info( u'in image._create_jp2_from_tif(); r.std_out, %s' % r.std_out )
        self.logger.info( u'in image._create_jp2_from_tif(); r.std_err, %s' % r.std_err )
        return

    def _create_jp2_from_jpg( self, CONVERT_COMMAND_PATH, KAKADU_COMMAND_PATH, source_filepath, destination_filepath ):
        """ Creates jp2 after first converting jpg to tif (due to server limitation). """
        cleaned_source_filepath = source_filepath.replace( u' ', u'\ ' )
        self.logger.debug( u'in image._create_jp2_from_jpg(); cleaned_source_filepath, %s' % cleaned_source_filepath )
        tif_destination_filepath = destination_filepath[0:-4] + u'.tif'
        self.logger.debug( u'in image._create_jp2_from_jpg(); tif_destination_filepath, %s' % tif_destination_filepath )
        cmd = u'%s -compress None "%s" %s' % (
            CONVERT_COMMAND_PATH, cleaned_source_filepath, tif_destination_filepath )  # source-filepath quotes needed for filename containing spaces
        self.logger.debug( u'in image._create_jp2_from_jpg(); cmd, %s' % cmd )
        r = envoy.run( cmd.encode(u'utf-8', u'replace') )
        self.logger.info( u'in image._create_jp2_from_jpg(); r.std_out, %s; type(r.std_out), %s' % (r.std_out, type(r.std_out)) )
        self.logger.info( u'in image._create_jp2_from_jpg(); r.std_err, %s; type(r.std_err), %s' % (r.std_err, type(r.std_err)) )
        if len( r.std_err ) > 0:
            raise Exception( u'Problem making intermediate .tif from .jpg' )
        source_filepath = tif_destination_filepath
        cmd = u'%s -i "%s" -o "%s" Creversible=yes -rate -,1,0.5,0.25 Clevels=12' % (
            KAKADU_COMMAND_PATH, source_filepath, destination_filepath )
        r = envoy.run( cmd.encode(u'utf-8', u'replace') )
        os.remove( tif_destination_filepath )
        return

    # end class ImageBuilder()


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
    assert sorted( data.keys() ) == [u'item_data', u'pid', u'update_image'], sorted( data.keys() )  # 'update_image' needed for next task
    ih = ImageHandler( data, logger )
    ih.make_jp2()
    task_manager.determine_next_task( current_task=unicode(sys._getframe().f_code.co_name), logger=logger,
        data=data )
    return

def run_add_image_datastream( data ):
    """ Runner for add_image_datastream().
        Called by queue-job triggered by tasks.task_manager.determine_next_task(). """
    assert sorted( data.keys() ) == [u'item_data', u'pid', u'update_image'], sorted( data.keys() )
    ih = ImageHandler( data, logger )
    ih.add_image_datastream()
    task_manager.determine_next_task( current_task=unicode(sys._getframe().f_code.co_name), logger=logger,
        data=data )
    return

