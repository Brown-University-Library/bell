# -*- coding: utf-8 -*-

""" Handles image-related tasks. """

import datetime, json, os, pprint, sys, time, urllib
import envoy, redis, requests, rq
from bell_code import bell_logger


queue_name = unicode( os.environ.get(u'BELL_QUEUE_NAME') )
q = rq.Queue( queue_name, connection=redis.Redis() )
r = redis.StrictRedis( host='localhost', port=6379, db=0 )


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
            ( pid, accession_number ) = ( filename_to_data_dct[image_filename][u'pid'], filename_to_data_dct[image_filename][u'accession_number'] )
            api_dct = self.get_api_data( pid )
            self.check_api_data( api_dct, image_filename, pid, accession_number, images_to_add, images_to_update )
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

    def get_api_data( self, pid ):
        """ Makes api call.
            Called by make_image_lists() """
        item_api_url = u'%s/%s/' % ( self.API_URL, pid )
        self.logger.debug( u'in tasks.images.ImageLister.check_image(); item_api_url, %s' % item_api_url )
        time.sleep( 2 )
        r = requests.get( item_api_url, verify=False )
        api_dct = r.json()
        return api_dct

    def check_api_data( self, api_dct, image_filename, pid, accession_number, images_to_add, images_to_update ):
        """ Looks up image-filename via public api; stores whether image exists or not.
            Called by make_image_lists() """
        image_already_ingested = True
        if u'JP2' in api_dct[u'links'][u'content_datastreams'].keys() or  u'jp2' in api_dct[u'rel_content_models_ssim']:
            pass
        else:
            image_already_ingested = False
        if image_already_ingested:
            # images_to_update.append( image_filename )
            images_to_update.append( {image_filename: {u'pid': pid, u'accession_number': accession_number}} )
        else:
            images_to_add.append( {image_filename: {u'pid': pid, u'accession_number': accession_number}} )
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

    # end class ImageLister


class ImageAdder( object ):
    """ Adds image to metadata object. """

    def __init__( self, logger ):
        self.logger = logger
        self.MASTER_IMAGES_DIR_PATH = unicode( os.environ[u'BELL_TASKS_IMGS__MASTER_IMAGES_DIR_PATH'] )
        self.JP2_IMAGES_DIR_PATH = unicode( os.environ[u'BELL_TASKS_IMGS__JP2_IMAGES_DIR_PATH'] )
        self.KAKADU_COMMAND_PATH = unicode( os.environ[u'BELL_TASKS_IMGS__KAKADU_COMMAND_PATH'] )
        self.CONVERT_COMMAND_PATH = unicode( os.environ[u'BELL_TASKS_IMGS__CONVERT_COMMAND_PATH'] )
        self.MASTER_IMAGES_DIR_URL = unicode( os.environ[u'BELL_TASKS_IMGS__MASTER_IMAGES_DIR_URL'] )
        self.JP2_IMAGES_DIR_URL = unicode( os.environ[u'BELL_TASKS_IMGS__JP2_IMAGES_DIR_URL'] )
        self.AUTH_API_URL = unicode( os.environ[u'BELL_TASKS_IMGS__AUTH_API_URL'] )
        self.AUTH_API_IDENTITY = unicode( os.environ[u'BELL_TASKS_IMGS__AUTH_API_IDENTITY'] )
        self.AUTH_API_KEY = unicode( os.environ[u'BELL_TASKS_IMGS__AUTH_API_KEY'] )

    def add_image( self, filename_dct ):
        """ Manages: creates jp2, hits api, & cleans up.
            Called by run_add_image() """
        logger.debug( u'in tasks.images.ImageAdder.add_image(); starting; filename_dct, `%s`' % pprint.pformat(filename_dct) )
        image_filename = filename_dct.keys()[0]
        ( source_filepath, destination_filepath, master_filename_encoded, jp2_filename ) = self.create_temp_filenames( image_filename )
        self.create_jp2( source_filepath, destination_filepath )
        pid = filename_dct[image_filename][u'pid']
        params = self.prep_params( master_filename_encoded, jp2_filename, pid )
        resp = self.hit_api( params )
        self.track_response( resp )
        os.remove( destination_filepath )
        return

    def create_temp_filenames( self, image_filename ):
        """ Creates filenames for subsequent jp2 creation.
            Called by add_image() """
        master_filename_raw = image_filename  # includes spaces
        master_filename_utf8 = master_filename_raw.encode( u'utf-8' )
        master_filename_encoded = urllib.quote( master_filename_utf8 ).decode( u'utf-8' )  # used for api call
        source_filepath = u'%s/%s' % ( self.MASTER_IMAGES_DIR_PATH, master_filename_raw )
        temp_jp2_filename = master_filename_raw.replace( u' ', u'_' )
        extension_idx = temp_jp2_filename.rfind( u'.' )
        non_extension_filename = temp_jp2_filename[0:extension_idx]
        jp2_filename = non_extension_filename + u'.jp2'
        destination_filepath = u'%s/%s' % ( self.JP2_IMAGES_DIR_PATH, jp2_filename )
        logger.debug( u'in tasks.images.ImageAdder.create_temp_filenames(); source_filepath, `%s`; destination_filepath, `%s`' % ( source_filepath, destination_filepath ) )
        return ( source_filepath, destination_filepath, master_filename_encoded, jp2_filename )

    # def create_temp_filenames( self, image_filename ):
    #     """ Creates filenames for subsequent jp2 creation.
    #         Called by add_image() """
    #     master_filename_raw = image_filename  # includes spaces
    #     master_filename_utf8 = master_filename_raw.encode( u'utf-8' )
    #     master_filename_encoded = urllib.quote( master_filename_utf8 ).decode( u'utf-8' )  # used for api call
    #     source_filepath = u'%s/%s' % ( self.MASTER_IMAGES_DIR_PATH, master_filename_raw )
    #     temp_jp2_filename = master_filename_raw.replace( u' ', u'_' )
    #     extension_idx = temp_jp2_filename.rfind( u'.' )
    #     non_extension_filename = temp_jp2_filename[0:extension_idx]
    #     jp2_filename = temp_jp2_filename + u'.jp2'
    #     destination_filepath = u'%s/%s' % ( self.JP2_IMAGES_DIR_PATH, jp2_filename )
    #     logger.debug( u'in tasks.images.ImageAdder.create_temp_filenames(); source_filepath, `%s`; destination_filepath, `%s`' % ( source_filepath, destination_filepath ) )
    #     return ( source_filepath, destination_filepath, master_filename_encoded, jp2_filename )

    def create_jp2( self, source_filepath, destination_filepath ):
        """ Creates jp2.
            Called by add_image() """
        if u'tif' in source_filepath.split( u'.' )[-1]:
            self._create_jp2_from_tif( source_filepath, destination_filepath )
        elif u'jp' in source_filepath.split( u'.' )[-1]:
            self._create_jp2_from_jpg( source_filepath, destination_filepath )
        return

    def _create_jp2_from_tif( self, source_filepath, destination_filepath ):
        """ Creates jp2 directly.
            Called by create_jp2() """
        cleaned_source_filepath = source_filepath.replace( u' ', u'\ ' )
        cmd = u'%s -i "%s" -o "%s" Creversible=yes -rate -,1,0.5,0.25 Clevels=12' % (
            self.KAKADU_COMMAND_PATH, cleaned_source_filepath, destination_filepath )
        self.logger.info( u'in tasks.images.ImageAdder._create_jp2_from_tif(); cmd, %s' % cmd )
        r = envoy.run( cmd.encode(u'utf-8', u'replace') )  # envoy requires a non-unicode string
        self.logger.info( u'in tasks.images.ImageAdder._create_jp2_from_tif(); r.std_out, %s' % r.std_out )
        self.logger.info( u'in tasks.images.ImageAdder._create_jp2_from_tif(); r.std_err, %s' % r.std_err )
        return

    def _create_jp2_from_jpg( self, source_filepath, destination_filepath ):
        """ Creates jp2 after first converting jpg to tif (due to server limitation).
            Called by create_jp2() """
        cleaned_source_filepath = source_filepath.replace( u' ', u'\ ' )
        self.logger.debug( u'in tasks.images.ImageAdder._create_jp2_from_jpg(); cleaned_source_filepath, %s' % cleaned_source_filepath )
        tif_destination_filepath = destination_filepath[0:-4] + u'.tif'
        self.logger.debug( u'in tasks.images.ImageAdder._create_jp2_from_jpg(); tif_destination_filepath, %s' % tif_destination_filepath )
        cmd = u'%s -compress None "%s" %s' % (
            self.CONVERT_COMMAND_PATH, cleaned_source_filepath, tif_destination_filepath )  # source-filepath quotes needed for filename containing spaces
        self.logger.debug( u'in tasks.images.ImageAdder._create_jp2_from_jpg(); cmd, %s' % cmd )
        r = envoy.run( cmd.encode(u'utf-8', u'replace') )
        self.logger.info( u'in tasks.images.ImageAdder._create_jp2_from_jpg(); r.std_out, %s; type(r.std_out), %s' % (r.std_out, type(r.std_out)) )
        self.logger.info( u'in tasks.images.ImageAdder._create_jp2_from_jpg(); r.std_err, %s; type(r.std_err), %s' % (r.std_err, type(r.std_err)) )
        if len( r.std_err ) > 0:
            raise Exception( u'Problem making intermediate .tif from .jpg' )
        source_filepath = tif_destination_filepath
        cmd = u'%s -i "%s" -o "%s" Creversible=yes -rate -,1,0.5,0.25 Clevels=12' % (
            self.KAKADU_COMMAND_PATH, source_filepath, destination_filepath )
        r = envoy.run( cmd.encode(u'utf-8', u'replace') )
        os.remove( tif_destination_filepath )
        return

    def prep_params( self, master_filename_encoded, jp2_filename, pid ):
        """ Sets params.
            Called by add_image() """
        master_url = u'%s/%s' % ( self.MASTER_IMAGES_DIR_URL, master_filename_encoded )
        jp2_url = u'%s/%s' % ( self.JP2_IMAGES_DIR_URL, jp2_filename )
        params = { u'pid': pid, u'identity': self.AUTH_API_IDENTITY, u'authorization_code': self.AUTH_API_KEY }
        params[u'content_streams'] = json.dumps([
            { u'dsID': u'MASTER', u'url': master_url },
            { u'dsID': u'JP2', u'url': jp2_url }
            ])
        self.logger.info( u'in tasks.images.ImageAdder.prep_params(); params/content_streams, `%s`' % params[u'content_streams'] )
        return params

    def hit_api( self, params ):
        """ Hits auth-api.
            Called by add_image() """
        try:
            self.logger.info( u'in tasks.images.ImageAdder.hit_api(); url, `%s`; identity, `%s`; pid, `%s`' % (self.AUTH_API_URL, params[u'identity'], params[u'pid']) )
            r = requests.put( self.AUTH_API_URL, data=params, verify=False )
            return r
        except Exception as e:
            err = unicode(repr(e))
            self.logger.error( u'in tasks.images.ImageAdder.hit_api(); error, `%s`' % err )
            raise Exception( err )

    def track_response( self, resp ):
        """ Outputs put result.
            Called by add_image() """
        resp_txt = resp.content.decode( u'utf-8' )
        self.logger.info( u'in tasks.images.ImageAdder.track_response(); resp_txt, `%s`; status_code, `%s`' % (resp_txt, resp.status_code) )
        print u'resp_txt, `%s`' % resp_txt
        if not resp.status_code == 200:
            raise Exception( u'Bad http status code detected.' )
        return

    # end class ImageAdder


## runners

logger = bell_logger.setup_logger()

def run_make_image_lists():
    """ Runner for make_image_lists()
        Called manually as per readme """
    lister = ImageLister( logger )
    lister.make_image_lists()
    return

def run_enqueue_add_image_jobs():
    """ Prepares list of images-to-add and enqueues jobs.
        Called manually.
        Suggestion: run on ingestion-server. """
    IMAGES_TO_PROCESS_JSON_PATH = unicode( os.environ[u'BELL_TASKS_IMGS__IMAGES_TO_PROCESS_JSON_PATH'] )
    with open( IMAGES_TO_PROCESS_JSON_PATH ) as f:
        dct = json.loads( f.read() )
    images_to_add = dct[u'lst_images_to_add']  # each lst entry is like: { "Agam PR_1981.1694.tif": {"accession_number": "PR 1981.1694", "pid": "bdr:300120"} }
    for (i, filename_dct) in enumerate( images_to_add ):
        print u'i is, `%s`' % i
        if i+1 > 500:
            break
        q.enqueue_call(
            func=u'bell_code.tasks.images.run_add_image',
            kwargs={ u'filename_dct': filename_dct },
            timeout=600 )
    print u'done'
    return

def run_add_image( filename_dct ):
    """ Runner for add_image()
        Called manually as per readme.
        Suggestion: run on ingestion-server. """
    adder = ImageAdder( logger )
    adder.add_image( filename_dct )
    return
