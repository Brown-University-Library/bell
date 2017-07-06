# -*- coding: utf-8 -*-
""" Handles image-related tasks. """
import datetime, json, logging, os, pprint, sys, time, urllib
import envoy, redis, requests, rq


LOG_FILENAME = os.environ['BELL_LOG_FILENAME']
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG,
                    format='[%(asctime)s] %(levelname)s [%(module)s-%(funcName)s()::%(lineno)d] %(message)s',
                    datefmt='%d/%b/%Y %H:%M:%S',
                    filename=LOG_FILENAME)


queue_name = os.environ.get('BELL_QUEUE_NAME')
q = rq.Queue( queue_name, connection=redis.Redis() )


class ImageDctMaker:
    """ Creats a dct, and then json file, of image-filename to accession-number and pid info.
        Example:
            {
            u'Zorn PR_1971.705.tif': {u'accession_number': u'PR 1971.705',
                                      u'pid': u'bdr:301594'},
            u'Zorn PR_1971.709.tif': {u'accession_number': u'PR 1971.709',
                                      u'pid': u'bdr:301595'}
            } """

    def __init__( self ):
        self.IMAGE_LIST_PATH = os.environ['BELL_TASKS_IMGS__IMAGES_LIST_PATH']
        self.DATA_DCT_PATH = os.environ['BELL_TASKS_IMGS__ACCESSION_NUMBER_TO_DATA_DICT_PATH']
        self.PID_DCT_PATH = os.environ['BELL_TASKS_IMGS__ACCESSION_NUMBER_TO_PID_DICT_PATH']
        self.IMAGES_FILENAME_DCT_JSON_OUTPUT_PATH = os.environ['BELL_TASKS_IMGS__IMAGES_FILENAME_DCT_JSON_OUTPUT_PATH']
        self.files_excluded = []

    def make_json_file( self ):
        """ Manages creation of dct and then json file.
            Called manually. """
        logger.debug( 'in tasks.images.ImageDctMaker.make_json_file(); starting' )
        ( images_lst, accession_data_dct, accession_pid_dct ) = self.setup()
        filename_to_data_dct = self.make_filename_to_data_dct( images_lst, accession_data_dct, accession_pid_dct )
        self.output_listing( filename_to_data_dct, images_lst )
        return

    def setup( self ):
        """ Sets initial vars.
            Called by make_json_file() """
        with open( self.IMAGE_LIST_PATH ) as f:
            dct = json.loads( f.read() )
        images_lst = dct['filelist']
        logger.debug( 'in tasks.images.ImageDctMaker.setup(); len(images_lst), `{}`'.format(len(images_lst)) )
        with open( self.DATA_DCT_PATH ) as f2:
            dct2 = json.loads( f2.read() )
        accession_data_dct = dct2['items']
        with open( self.PID_DCT_PATH ) as f3:
            dct3 = json.loads( f3.read() )
        accession_pid_dct = dct3['final_accession_pid_dict']
        return ( images_lst, accession_data_dct, accession_pid_dct )

    def make_filename_to_data_dct( self, images_lst, accession_data_dct, accession_pid_dct ):
        """ Returns dct of filename keys to data-dct of accession-number and pid.
            Called by: make_image_lists() """
        filename_to_data_dct = {}
        for image_filename in images_lst:
            extension_idx = image_filename.rfind( '.' )
            self._check_image_filename( image_filename, accession_data_dct, filename_to_data_dct, extension_idx, accession_pid_dct )
        logger.debug( 'in tasks.images.ImageLister.make_filename_to_data_dct(); filename_to_data_dct, `%s`' % pprint.pformat(filename_to_data_dct) )
        return filename_to_data_dct

    def _check_image_filename( self, image_filename, accession_data_dct, filename_to_data_dct, extension_idx, accession_pid_dct ):
        """ Checks image_filename and variant against data['object_image_scan_filename'].
            If found, adds filename_to_data_dct entry; otherwise logs skip.
            Called by make_filename_to_data_dct() """
        found = False
        non_extension_filename = image_filename[0:extension_idx]
        for ( accession_number_key, data_dct_value ) in accession_data_dct.items():
            if image_filename == data_dct_value['object_image_scan_filename'] or non_extension_filename == data_dct_value['object_image_scan_filename']:
                filename_to_data_dct[image_filename] = { 'accession_number': accession_number_key, 'pid': accession_pid_dct[accession_number_key] }
                found = True
        if found is False:
            self.files_excluded.append( image_filename )
            logger.debug( 'in tasks.images.ImageLister._check_image_filename(); no data entry found on image_filename, `{image_filename}` or non_extension_filename, {non_extension_filename}'.format(image_filename=image_filename, non_extension_filename=non_extension_filename) )
        return

    def output_listing( self, filename_to_data_dct, images_lst ):
        """ Saves json file.
            Called by make_image_lists() """
        data = {
            'datetime': str( datetime.datetime.now() ),
            'count_dir_images': len( images_lst ),
            'count_dct_images': len( filename_to_data_dct.keys() ),
            'files_excluded': self.files_excluded,
            'filename_to_data_dct': filename_to_data_dct }
        jsn = json.dumps( data, indent=2, sort_keys=True )
        with open( self.IMAGES_FILENAME_DCT_JSON_OUTPUT_PATH, 'wt', encoding='utf8' ) as f:
            f.write( jsn )
        return

    # end class ImageDctMaker()


class ImageLister:
    """ Lists images that need to be added, and those that need to be updated. """

    def __init__( self ):
        self.IMAGES_FILENAME_DCT_JSON_PATH = os.environ['BELL_TASKS_IMGS__IMAGES_FILENAME_DCT_JSON_OUTPUT_PATH']
        self.IMAGES_TO_PROCESS_OUTPUT_PATH = os.environ['BELL_TASKS_IMGS__IMAGES_TO_PROCESS_OUTPUT_PATH']
        #only running against PROD, since it's read-only and wouldn't really work against dev.
        self.PROD_API_URL = os.environ['BELL_TASKS_IMGS__PROD_API_ROOT_URL']
        self.images_to_add = []
        self.images_to_update = []

    def make_image_lists( self ):
        """ Saves, in one json file, two lists of accession_numbers, one for images to be added, and one for images to be updated.
            Called manuallly per readme. """
        logger.debug( 'in tasks.images.ImageLister.make_image_lists(); starting' )
        filename_to_data_dct = self.setup()
        for ( i, image_filename ) in enumerate(sorted( filename_to_data_dct.keys()) ):
            ( pid, accession_number ) = ( filename_to_data_dct[image_filename]['pid'], filename_to_data_dct[image_filename]['accession_number'] )
            api_dct = self.get_api_data( pid )
            self.check_api_data( api_dct, image_filename, pid, accession_number )
            print(i)
            if i+1 >= 1000:
                break
        self.output_listing( self.images_to_add, self.images_to_update, filename_to_data_dct )
        return

    def setup( self ):
        """ Sets initial vars.
            Called by make_image_lists() """
        with open( self.IMAGES_FILENAME_DCT_JSON_PATH, encoding='utf8' ) as f:
            dct = json.loads( f.read() )
        filename_to_data_dct = dct['filename_to_data_dct']
        return filename_to_data_dct

    def get_api_data( self, pid ):
        """ Makes api call.
            Called by make_image_lists() """
        item_api_url = '%s/%s/' % ( self.PROD_API_URL, pid )
        logger.debug( 'in tasks.images.ImageLister.check_image(); item_api_url, %s' % item_api_url )
        time.sleep( .3 )
        r = requests.get( item_api_url )
        if r.ok:
            api_dct = r.json()
            return api_dct
        else:
            msg = '%s - %s' % (r.status_code, r.text)
            logger.error(msg)
            raise Exception(msg)

    def check_api_data( self, api_dct, image_filename, pid, accession_number ):
        """ Looks up image-filename via public api; stores whether image exists or not.
            Called by make_image_lists() """
        image_already_ingested = True
        if 'JP2' in api_dct['links']['content_datastreams'].keys() or 'jp2' in api_dct['rel_content_models_ssim']:
            pass
        else:
            image_already_ingested = False
        if image_already_ingested:
            self.images_to_update.append( {image_filename: {'pid': pid, 'accession_number': accession_number}} )
        else:
            self.images_to_add.append( {image_filename: {'pid': pid, 'accession_number': accession_number}} )
        return

    def output_listing( self, images_to_add, images_to_update, filename_to_data_dct ):
        """ Saves json file.
            Called by make_image_lists() """
        data = {
            'datetime': str( datetime.datetime.now() ),
            'count_images': len( filename_to_data_dct.keys() ),  # number of images from ImageDctMaker()
            'count_images_processed': len( images_to_add ) + len( images_to_update ),  # should match above count
            'count_images_to_add': len( images_to_add ),
            'count_images_to_update': len( images_to_update ),
            'lst_images_to_add': images_to_add,
            'lst_images_to_update': images_to_update }
        jsn = json.dumps( data, indent=2, sort_keys=True )
        with open( self.IMAGES_TO_PROCESS_OUTPUT_PATH, 'wt', encoding='utf8' ) as f:
            f.write( jsn )
        return

    # end class ImageLister


class ImageAdder:
    """ Adds image to object. """

    def __init__( self, logger, env='dev' ):
        self.logger = logger
        self.MASTER_IMAGES_DIR_PATH = os.environ['BELL_TASKS_IMGS__MASTER_IMAGES_DIR_PATH']  # no trailing slash
        self.JP2_IMAGES_DIR_PATH = os.environ['BELL_TASKS_IMGS__JP2_IMAGES_DIR_PATH']  # no trailing slash
        self.KAKADU_COMMAND_PATH = os.environ['BELL_TASKS_IMGS__KAKADU_COMMAND_PATH']
        self.CONVERT_COMMAND_PATH = os.environ['BELL_TASKS_IMGS__CONVERT_COMMAND_PATH']
        self.MASTER_IMAGES_DIR_URL = os.environ['BELL_TASKS_IMGS__MASTER_IMAGES_DIR_URL']  # no trailing slash
        self.JP2_IMAGES_DIR_URL = os.environ['BELL_TASKS_IMGS__JP2_IMAGES_DIR_URL']  # no trailing slash
        if env == 'prod':
            self.AUTH_API_URL = os.environ['BELL_TASKS_IMGS__PROD_AUTH_API_URL']
            self.AUTH_API_IDENTITY = os.environ['BELL_TASKS_IMGS__PROD_AUTH_API_IDENTITY']
            self.AUTH_API_KEY = os.environ['BELL_TASKS_IMGS__PROD_AUTH_API_KEY']
        else:
            self.AUTH_API_URL = os.environ['BELL_TASKS_IMGS__DEV_AUTH_API_URL']
            self.AUTH_API_IDENTITY = os.environ['BELL_TASKS_IMGS__DEV_AUTH_API_IDENTITY']
            self.AUTH_API_KEY = os.environ['BELL_TASKS_IMGS__DEV_AUTH_API_KEY']

    def add_image( self, filename_dct ):
        """ Manages: creates jp2, hits api, & cleans up.
            Called by run_add_image() """
        logger.debug( 'in tasks.images.ImageAdder.add_image(); starting; filename_dct, `%s`' % pprint.pformat(filename_dct) )
        image_filename = list(filename_dct.keys())[0]
        ( source_filepath, destination_filepath, master_filename_encoded, jp2_filename ) = self.create_temp_filenames( image_filename )
        self.create_jp2( source_filepath, destination_filepath )
        pid = filename_dct[image_filename]['pid']
        params = self.prep_params( master_filename_encoded, jp2_filename, pid )
        resp = self.hit_api( params )
        self.track_response( resp )
        os.remove( destination_filepath )
        return

    def create_temp_filenames( self, master_filename_raw ):
        """ Creates filenames for subsequent jp2 creation.
            Called by add_image()
            Note, master_filename_raw likely includes spaces and may include apostrophes,
              and self.MASTER_IMAGES_DIR_PATH may include spaces. """
        master_filename_encoded = urllib.parse.quote( master_filename_raw )  # used for api call
        source_filepath = '%s/%s' % ( self.MASTER_IMAGES_DIR_PATH, master_filename_raw )
        temp_jp2_filename = master_filename_raw.replace( ' ', '_' )
        temp_jp2_filename2 = temp_jp2_filename.replace( u"'", '_' )
        extension_idx = temp_jp2_filename2.rfind( '.' )
        non_extension_filename = temp_jp2_filename2[0:extension_idx]
        jp2_filename = non_extension_filename + '.jp2'
        destination_filepath = '%s/%s' % ( self.JP2_IMAGES_DIR_PATH, jp2_filename )
        logger.debug( 'in tasks.images.ImageAdder.create_temp_filenames(); source_filepath, `%s`; destination_filepath, `%s`' % ( source_filepath, destination_filepath ) )
        return ( source_filepath, destination_filepath, master_filename_encoded, jp2_filename )

    def create_jp2( self, source_filepath, destination_filepath ):
        """ Creates jp2.
            Called by add_image() """
        if 'tif' in source_filepath.split( '.' )[-1]:
            self._create_jp2_from_tif( source_filepath, destination_filepath )
        elif 'jp' in source_filepath.split( '.' )[-1]:
            self._create_jp2_from_jpg( source_filepath, destination_filepath )
        return

    def _create_jp2_from_tif( self, source_filepath, destination_filepath ):
        """ Creates jp2 directly.
            Called by create_jp2() """
        source_filepath2 = source_filepath.replace( "'", "\\'" )
        cmd = '{command_path} -i "{source_path}" -o "{destination_path}" Creversible=yes -rate -,1,0.5,0.25 Clevels=12'.format(
            command_path=self.KAKADU_COMMAND_PATH, source_path=source_filepath2, destination_path=destination_filepath
            )
        self.logger.info( 'in tasks.images.ImageAdder._create_jp2_from_tif(); cmd, %s' % cmd )
        #TODO replace envoy with subprocess? Check that same information is in output.
        r = envoy.run( cmd )
        self.logger.info( 'in tasks.images.ImageAdder._create_jp2_from_tif(); r.std_out, %s' % r.std_out )
        self.logger.info( 'in tasks.images.ImageAdder._create_jp2_from_tif(); r.std_err, %s' % r.std_err )
        return

    def _create_jp2_from_jpg( self, source_filepath, destination_filepath ):
        """ Creates jp2 after first converting jpg to tif (due to server limitation).
            Called by create_jp2() """
        # cleaned_source_filepath = source_filepath.replace( ' ', '\ ' )
        cleaned_source_filepath = source_filepath  # graphicsmagic doesn't need spaces escaped
        self.logger.debug( 'in tasks.images.ImageAdder._create_jp2_from_jpg(); cleaned_source_filepath, %s' % cleaned_source_filepath )
        tif_destination_filepath = destination_filepath[0:-4] + '.tif'
        self.logger.debug( 'in tasks.images.ImageAdder._create_jp2_from_jpg(); tif_destination_filepath, %s' % tif_destination_filepath )
        cmd = '%s -compress None "%s" %s' % (
            self.CONVERT_COMMAND_PATH, cleaned_source_filepath, tif_destination_filepath )  # source-filepath quotes needed for filename containing spaces
        self.logger.debug( 'in tasks.images.ImageAdder._create_jp2_from_jpg(); cmd, %s' % cmd )
        r = envoy.run( cmd.encode('utf-8', 'replace') )
        self.logger.info( 'in tasks.images.ImageAdder._create_jp2_from_jpg(); r.std_out, %s; type(r.std_out), %s' % (r.std_out, type(r.std_out)) )
        self.logger.info( 'in tasks.images.ImageAdder._create_jp2_from_jpg(); r.std_err, %s; type(r.std_err), %s' % (r.std_err, type(r.std_err)) )
        if len( r.std_err ) > 0:
            raise Exception( 'Problem making intermediate .tif from .jpg' )
        source_filepath = tif_destination_filepath
        cmd = '%s -i "%s" -o "%s" -rate -,1,0.5,0.25 Creversible=yes Clevels=8 Stiles={1024,1024}' % (
            self.KAKADU_COMMAND_PATH, source_filepath, destination_filepath )
        r = envoy.run( cmd.encode('utf-8', 'replace') )
        os.remove( tif_destination_filepath )
        return

    def prep_params( self, master_filename_encoded, jp2_filename, pid ):
        """ Sets params.
            Called by add_image() """
        params = { 'pid': pid, 'identity': self.AUTH_API_IDENTITY, 'authorization_code': self.AUTH_API_KEY }
        #Note: we used to check a setting for whether or not to send this param, but seems like we can just
        #   always send it.
        params['overwrite_content'] = 'yes'
        master_url = '%s/%s' % ( self.MASTER_IMAGES_DIR_URL, master_filename_encoded )
        jp2_url = '%s/%s' % ( self.JP2_IMAGES_DIR_URL, jp2_filename )
        params['content_streams'] = json.dumps([
            { 'dsID': 'MASTER', 'url': master_url },
            { 'dsID': 'JP2', 'url': jp2_url }
            ])
        self.logger.info( 'in tasks.images.ImageAdder.prep_params(); params/content_streams, `%s`' % params['content_streams'] )
        return params

    def hit_api( self, params ):
        """ Hits auth-api.
            Called by add_image() """
        try:
            self.logger.info( 'in tasks.images.ImageAdder.hit_api(); url, `%s`; identity, `%s`; pid, `%s`' % (self.AUTH_API_URL, params['identity'], params['pid']) )
            r = requests.put( self.AUTH_API_URL, data=params )
            return r
        except Exception as e:
            err = unicode(repr(e))
            self.logger.error( 'in tasks.images.ImageAdder.hit_api(); error, `%s`' % err )
            raise Exception( err )

    def track_response( self, resp ):
        """ Outputs put result.
            Called by add_image() """
        resp_txt = resp.content.decode( 'utf-8' )
        self.logger.info( 'in tasks.images.ImageAdder.track_response(); resp_txt, `%s`; status_code, `%s`' % (resp_txt, resp.status_code) )
        print('resp_txt, `%s`' % resp_txt)
        if not resp.status_code == 200:
            raise Exception( 'Bad http status code detected.' )
        return

    # end class ImageAdder


## runners

def run_make_image_filename_dct():
    """ Runner for ImageDctMaker.make_json_file()
        Called manually as per readme. """
    mkr = ImageDctMaker()
    mkr.make_json_file()
    return


def run_make_image_lists():
    """ Runner for make_image_lists()
        Called manually as per readme. """
    lister = ImageLister()
    lister.make_image_lists()
    return


def run_enqueue_add_image_jobs(env='dev'):
    """ Grabs list of images-to-add and enqueues jobs.
        Called manually.
        Suggestion: run on ingestion-server. """
    IMAGES_TO_PROCESS_JSON_PATH = os.environ['BELL_TASKS_IMGS__IMAGES_TO_PROCESS_JSON_PATH']
    with open( IMAGES_TO_PROCESS_JSON_PATH ) as f:
        dct = json.loads( f.read() )
    images_to_add = dct['lst_images_to_add']  # each lst entry is like: { "Agam PR_1981.1694.tif": {"accession_number": "PR 1981.1694", "pid": "bdr:300120"} }
    for (i, filename_dct) in enumerate( images_to_add ):
        print('i is, `%s`' % i)
        if i+1 > 1:
            break
        q.enqueue_call(
            func='tasks.images.run_add_image',
            kwargs={ 'env': env, 'filename_dct': filename_dct },
            timeout=600 )
    print('done')
    return

def run_enqueue_update_image_jobs():
    """ Grabs list of images-to-update and enqueues jobs.
        Called manually.
        Suggestion: run on ingestion-server. """
    IMAGES_TO_PROCESS_JSON_PATH = os.environ['BELL_TASKS_IMGS__IMAGES_TO_PROCESS_JSON_PATH']
    with open( IMAGES_TO_PROCESS_JSON_PATH ) as f:
        dct = json.loads( f.read() )
    images_to_add = dct['lst_images_to_update']  # each lst entry is like: { "Agam PR_1981.1694.tif": {"accession_number": "PR 1981.1694", "pid": "bdr:300120"} }
    for (i, filename_dct) in enumerate( images_to_add ):
        print('i is, `%s`' % i)
        if i+1 > 1:
            break
        q.enqueue_call(
            func='bell_code.tasks.images.run_add_image',
            kwargs={ 'filename_dct': filename_dct },
            timeout=600 )
    print('done')
    return

def run_add_image( filename_dct, env='dev' ):
    """ Runner for add_image()
        Called manually as per readme.
        Suggestion: run on ingestion-server. """
    adder = ImageAdder( logger, env )
    adder.add_image( filename_dct )
    return

