""" Handles metadata-related tasks. """
import datetime, io, json, os, logging, pprint, sys, time
#from StringIO import StringIO as SIO
#import filelike, redis, requests, rq
import redis, requests, rq
from utils import mods_builder
from tasks.bell_utils import DATA_DIR


LOG_FILENAME = os.environ['BELL_LOG_FILENAME']
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG,
                    format='[%(asctime)s] %(levelname)s [%(module)s-%(funcName)s()::%(lineno)d] %(message)s',
                    datefmt='%d/%b/%Y %H:%M:%S',
                    filename=LOG_FILENAME)


ACCESSION_NUMBER_TO_DATA_PATH = os.path.join(DATA_DIR, 'c__accession_number_to_data_dict.json')
METADATA_ONLY_JSON = os.path.join(DATA_DIR, 'f1__metadata_only_accession_numbers.json')
TRACKER_PATH = os.path.join(DATA_DIR, 'f2__metadata_obj_tracker.json')
MODS_SCHEMA_PATH = os.environ['BELL_TASKS_META__MODS_XSD_PATH']

PROD_STORAGE_URL = os.environ['BELL_TASKS_META__PROD_STORAGE_URL']
PROD_API_URL = os.environ['BELL_TASKS_META__PROD_AUTH_API_URL']
PROD_API_IDENTITY = os.environ['BELL_TASKS_META__PROD_AUTH_API_IDENTITY']
PROD_API_KEY = os.environ['BELL_TASKS_META__PROD_AUTH_API_KEY']


class MetadataOnlyLister:
    """ Creates json file of accession numbers for which new metatdata-only objects will be created.
    Note: these metadata-only objects could get images later - we're just starting with the metadata.
    """

    def __init__( self ):
        self.PID_JSON_PATH = os.path.join(DATA_DIR, 'e1__accession_number_to_pid_dict.json')
        self.OUTPUT_PATH = os.path.join(DATA_DIR, 'f1__metadata_only_accession_numbers.json')

    def list_metadata_only_accession_numbers( self ):
        """ Saves a json list of accession_numbers.
            Called manuallly per readme. """
        with open( self.PID_JSON_PATH ) as f:
            dct = json.loads( f.read() )
        dct_lst = sorted( dct['final_accession_pid_dict'].items() )
        lst_to_queue = []
        for (accession_number, pid) in dct_lst:
            if pid == None and not accession_number == "null":
                lst_to_queue.append( accession_number.strip() )
        data = {
            'datetime': str( datetime.datetime.now() ), 'count': len( lst_to_queue ), 'accession_numbers': lst_to_queue }
        self.output_listing( data )

    def output_listing( self, data ):
        """ Saves json file.
            Called by list_metadata_only_accession_numbers() """
        jsn = json.dumps( data, indent=2, sort_keys=True )
        with open( self.OUTPUT_PATH, 'w' ) as f:
            f.write( jsn )


def run_metadata_only_lister():
    """ Runs main MetadataOnlyLister() function.
        Called manually per readme() """
    lister = MetadataOnlyLister()
    lister.list_metadata_only_accession_numbers()
    return


def grab_local_item_dct( accession_number ):
    """ Loads data for given accession_number.
        Called by create_metadata_only_object() """
    logger.debug( 'in metadata.MetadataCreator.grab_item_dct(); accession_number, %s' % accession_number )
    with open( ACCESSION_NUMBER_TO_DATA_PATH ) as f:
        metadata_dct = json.loads( f.read() )
    items = metadata_dct['items']
    item_dct = items[ accession_number ]
    logger.debug( 'in metadata.MetadataCreator.grab_item_dct(); item_dct, %s' % pprint.pformat(item_dct) )
    return item_dct


def grab_bdr_item_dct( pid ):
    r = requests.get( f'{PROD_STORAGE_URL}{pid}/bell_metadata/' )
    if r.ok:
        data = json.loads(r.content)
        return data
    else:
        raise Exception(f'{r.status_code} - {r.text}')


class MetadataCreator:
    """ Handles metadata-creation related tasks. """

    def __init__( self, env, logger ):
        self.logger = logger
        self.MODS_SCHEMA_PATH = MODS_SCHEMA_PATH
        self.COLLECTION_ID = os.environ['BELL_TASKS_META__COLLECTION_ID']
        if env == 'prod':
            self.API_URL = PROD_API_URL
            self.API_IDENTITY = PROD_API_IDENTITY
            self.API_KEY = PROD_API_KEY
            self.OWNING_COLLECTION = os.environ['BELL_TASKS_META__PROD_OWNING_COLLECTION_PID']
        else:
            self.API_URL = os.environ['BELL_TASKS_META__DEV_AUTH_API_URL']
            self.API_IDENTITY = os.environ['BELL_TASKS_META__DEV_AUTH_API_IDENTITY']
            self.API_KEY = os.environ['BELL_TASKS_META__DEV_AUTH_API_KEY']
            self.OWNING_COLLECTION = os.environ['BELL_TASKS_META__DEV_OWNING_COLLECTION_PID']

    def create_metadata_only_object( self, accession_number ):
        """ Gathers source metadata, prepares call to item-api, calls it, and confirms creation.
            Called by run_create_metadata_only_object() """
        params = self.set_basic_params()
        item_dct = grab_local_item_dct( accession_number )
        params['ir'] = self.make_ir_params( item_dct )
        params['mods'] = self.make_mods_params( item_dct )
        ( file_obj, param_string ) = self.prep_content_datastream( item_dct )
        params['content_streams'] = param_string
        pid = self.perform_post( params, file_obj )  # perform_post() closes the file
        self.track_progress( accession_number, pid )

    def set_basic_params( self ):
        """ Sets forthright params.
            Called by run_create_metadata_only_object() """
        params = {
            'identity': self.API_IDENTITY,
            'authorization_code': self.API_KEY,
            'rights': json.dumps({'parameters': {'additional_rights': 'BDR_PUBLIC#discover,display+Bell Gallery#discover,display,modify,delete'}}),
            'rels': json.dumps( {'owning_collection': self.OWNING_COLLECTION} ),
            'content_model': 'CommonMetadataDO'
            }
        return params

    def make_ir_params( self, item_dct ):
        """ Returns json of ir params.
            Called by create_metadata_only_object() NOTE: ir-collection-id in rels -- NOTE: go xml route."""
        ir_param = { 'parameters': {'depositor_name': 'Bell Gallery', 'ir_collection_id': self.COLLECTION_ID} }
        self.logger.debug( 'in metadata.MetadataCreator.make_ir_params(); ir_param, %s' % pprint.pformat(ir_param) )
        jsn = json.dumps( ir_param )
        return jsn

    def make_mods_params( self, item_dct ):
        """ Returns json if mods params.
            Called by create_metadata_only_object() """
        mb = mods_builder.ModsBuilder()
        return_type = 'return_string'  # or 'return_object'
        mods_xml_dct = mb.build_mods_object( item_dct, self.MODS_SCHEMA_PATH, return_type )
        self.logger.debug( 'in metadata.MetadataCreator.make_mods_params(); mods_xml_dct, %s' % pprint.pformat(mods_xml_dct) )
        mods_xml = mods_xml_dct['data']
        mods_param = { 'xml_data': mods_xml }
        jsn = json.dumps( mods_param )
        return jsn

    def prep_content_datastream( self, item_dct ):
        """ Returns file-like object containing the item_dct.
            Called by create_metadata_only_object() """
        jsn = json.dumps( item_dct )
        file_obj = io.BytesIO(jsn.encode('utf8'))
        param_string = json.dumps( [{
            'dsID': 'bell_metadata',
            'file_name': 'bell_item.json',
            'mimetype': 'application/javascript'
            }] )
        return_tuple = ( file_obj, param_string )
        return return_tuple

    def perform_post( self, params, file_obj ):
        """ Hits api w/post. Returns pid.
            Called by create_metadata_only_object() """
        files = { 'bell_item.json': file_obj }
        try:
            r = requests.post( self.API_URL, data=params, files=files )
        except Exception as e:
            self.logger.exception(f'error creating metadata object')
            raise
        finally:
            file_obj.close()
        if r.ok:
            return r.json()['pid']
        else:
            msg = f'error creating metadata object: {r.status_code} - {r.text}'
            self.logger.error(msg)
            raise Exception(msg)

    def track_progress( self, accession_number, pid ):
        """ Logs progress to json file.
            TODO: log progress to redis hash.
            Called by create_metadata_only_object() """
        print(f'{accession_number} - {pid}')
        try:
            with open( TRACKER_PATH ) as f:
                dct = json.loads( f.read() )
        except ( IOError, ValueError ):
            dct = {}
        dct[accession_number] = pid
        with open( TRACKER_PATH, 'w' ) as f:
            f.write( json.dumps(dct, indent=2, sort_keys=True) )

    # end class MetadataCreator


class MetadataUpdater:
    """ Handles metadata-creation related tasks.
        TODO: once this is part of the regular production, refactor relevant functions with MetadataCreator(). """

    def __init__( self ):
        self.SOURCE_FULL_JSON_METADATA_PATH = os.environ['BELL_TASKS_META__FULL_JSON_METADATA_PATH']
        self.API_URL = os.environ['BELL_TASKS_META__AUTH_API_URL']
        self.API_IDENTITY = os.environ['BELL_TASKS_META__AUTH_API_IDENTITY']
        self.API_KEY = os.environ['BELL_TASKS_META__AUTH_API_KEY']
        self.MODS_SCHEMA_PATH = MODS_SCHEMA_PATH
        self.OWNING_COLLECTION = os.environ['BELL_TASKS_META__OWNING_COLLECTION_PID']

    def update_object_metadata( self, accession_number, pid ):
        """ Gathers source metadata, prepares call to item-api, calls it, confirms update, tracks result.
            Called manually for now by one_offs.update_metadata_object.py """
        logger.debug( 'starting updater' )
        params = self.set_basic_params( pid )
        item_dct = self.grab_item_dct( accession_number )
        params['ir'] = self.make_ir_params( item_dct )
        params['mods'] = self.make_mods_params( item_dct )
        # ( file_obj, param_string ) = self.prep_content_datastream( item_dct )
        # params['content_streams'] = param_string
        logger.debug( 'params before post, ```{}```'.format(pprint.pformat(params)) )
        # self.perform_update( params, file_obj )  # perform_update() closes the file
        self.perform_update( params )  # perform_update() closes the file
        # self.track_progress( accession_number, pid )
        return

    def set_basic_params( self, pid ):
        """ Sets forthright params.
            Called by update_object_metadata() """
        params = {
            'pid': pid,
            'identity': self.API_IDENTITY,
            'authorization_code': self.API_KEY,
            'owner_id': self.API_IDENTITY,  # may switch this in future
            'rights': json.dumps({'parameters': {'additional_rights': 'BDR_PUBLIC#discover,display+Bell Gallery#discover,display,modify,delete'}}),
            'rels': json.dumps( {'owning_collection': self.OWNING_COLLECTION} )
            }
        logger.debug( 'initial params, ```{}```'.format(pprint.pformat(params)) )
        return params

    def grab_item_dct( self, accession_number ):
        """ Loads data for given accession_number.
            Called by update_object_metadata() """
        with open( self.SOURCE_FULL_JSON_METADATA_PATH ) as f:
            metadata_dct = json.loads( f.read() )
        items = metadata_dct['items']
        item_dct = items[ accession_number ]
        logger.debug( 'item_dct, ```{}```'.format(pprint.pformat(item_dct)) )
        return item_dct

    def make_ir_params( self, item_dct ):
        """ Returns json of ir params.
            Called by update_object_metadata() NOTE: ir-collection-id in rels -- NOTE: go xml route."""
        ir_param = { 'parameters': {'depositor_name': 'Bell Gallery'} }
        logger.debug( 'ir_param, ```{}```'.format(pprint.pformat(ir_param)) )
        jsn = json.dumps( ir_param )
        return jsn

    def make_mods_params( self, item_dct ):
        """ Returns json if mods params.
            Called by update_object_metadata() """
        mb = mods_builder.ModsBuilder()
        return_type = 'return_string'  # or 'return_object'
        mods_xml_dct = mb.build_mods_object( item_dct, self.MODS_SCHEMA_PATH, return_type )
        logger.debug( 'mods_xml_dct, ```{}```'.format(pprint.pformat(mods_xml_dct)) )
        mods_xml = mods_xml_dct['data']
        mods_param = { 'xml_data': mods_xml }
        logger.debug( 'mods_param, ```{}```'.format(pprint.pformat(mods_param)) )
        jsn = json.dumps( mods_param )
        return jsn

    def prep_content_datastream( self, item_dct ):
        """ Returns file-like object containing the item_dct.
            Called by update_object_metadata() """
        jsn = json.dumps( item_dct )
        file_obj = io.BytesIO(jsn.encode('utf8'))
        param_string = json.dumps( [{
            'dsID': 'bell_metadata',
            'file_name': 'bell_item.json',
            'mimetype': 'application/javascript'
            }] )
        return_tuple = ( file_obj, param_string )
        logger.debug( 'return_tuple, ```{}```'.format(pprint.pformat(return_tuple)) )
        return return_tuple

    def perform_update( self, params ):
        """ Hits api w/patch?
            Called by update_object_metadata() """
        logger.debug( 'api-url, ```{}```'.format(self.API_URL) )
        time.sleep( .5 )
        try:
            r = requests.put( self.API_URL, data=params )
        except Exception as e:
            self._handle_update_exception( e )
        logger.debug( 'r.status_code, `{status_code}`; r.content, ```{content}```'.format(status_code=r.status_code, content=r.content.decode('utf-8', 'replace')) )
        response_data = json.loads( r.content.decode('utf-8') )
        result = response_data['response']['status']
        return result

    def _handle_update_exception( self, e ):
        logger.error( 'exception hitting update-api, ```{}```'.format(repr(e)) )
        raise Exception( 'failure on metadata.MetadataCreator.perform_update()' )

    # end class MetadataUpdater()


## runners ##

def run_create_metadata_only_objects(env='prod'):
    """ Prepares list of accession numbers and enqueues jobs.
        Called manually. """
    with open( METADATA_ONLY_JSON ) as f:
        dct = json.loads( f.read() )
    with open( TRACKER_PATH, 'rb' ) as f:
        tracker_data = f.read().decode('utf8')
        processed_accession_numbers = json.loads(tracker_data).keys()
    accession_numbers = dct['accession_numbers']
    for i, accession_number in enumerate(accession_numbers):
        print(f'{i}: {accession_number}')
        if accession_number in processed_accession_numbers:
            print(f'  {accession_number} already processed - skipping')
            continue
        m = MetadataCreator( env, logger )
        m.create_metadata_only_object( accession_number )
    print('done')
    return


def post_metadata_update_to_bdr(pid, bell_metadata_dct):
    mb = mods_builder.ModsBuilder()
    mods_xml_dct = mb.build_mods_object( bell_metadata_dct, MODS_SCHEMA_PATH, return_type='return_string' )
    mods_data = mods_xml_dct['data']
    params = {
            'identity': PROD_API_IDENTITY,
            'authorization_code': PROD_API_KEY,
            'pid': pid
        }
    params['mods'] = json.dumps({'xml_data': mods_data})
    file_name = '%s_metadata.json' % pid.replace(':', '')
    params['content_streams'] = json.dumps([{'dsID': 'bell_metadata', 'mimeType': 'application/json', 'file_name': file_name}])
    params['overwrite_content'] = 'yes'
    params['generate_derivatives'] = 'no'
    file_object = io.BytesIO(json.dumps(bell_metadata_dct).encode('utf8'))
    r = requests.put(PROD_API_URL, data=params, files={file_name: file_object})
    if not r.ok:
        raise Exception(f'{r.status_code} - {r.text}')


def run_update_metadata_if_needed():
    #TODO: see if record_modified_date can be added to FileMaker export
    with open(os.path.join(DATA_DIR, 'e1__accession_number_to_pid_dict.json'), 'rb') as f:
        data = f.read()
    e1_info = json.loads(data)
    accession_number_pid_mapping = e1_info['final_accession_pid_dict']
    accession_numbers = list(accession_number_pid_mapping.keys())
    with open(os.path.join(DATA_DIR, 'f1__metadata_only_accession_numbers.json'), 'rb') as f:
        data = f.read()
    new_object_accession_numbers = json.loads(data)['accession_numbers']
    for accession_number in accession_numbers:
        if accession_number in new_object_accession_numbers:
            continue #don't need to check the objects we just created
        pid = accession_number_pid_mapping[accession_number]
        print(f'{accession_number} - {pid}')
        data_dct = grab_local_item_dct(accession_number)
        bdr_data_dct = grab_bdr_item_dct(pid)
        if data_dct == bdr_data_dct:
            print('.')
        else:
            post_metadata_update_to_bdr(pid, data_dct)

