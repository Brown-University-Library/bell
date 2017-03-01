# -*- coding: utf-8 -*-

from __future__ import unicode_literals

""" Handles metadata-related tasks. """

import datetime, json, os, logging, pprint, sys, time
from StringIO import StringIO as SIO
import filelike, redis, requests, rq
# from bell_code import bell_logger
from bell_code.utils import logger_setup, mods_builder


logger = logging.getLogger( 'bell_logger' )
logger_setup.check_log_handler()
queue_name = unicode( os.environ.get('BELL_QUEUE_NAME') )
q = rq.Queue( queue_name, connection=redis.Redis() )
r = redis.StrictRedis( host='localhost', port=6379, db=0 )


class MetadataOnlyLister( object ):
    """ Creates json file of accession numbers for which new metatdata-only objects will be created. """

    def __init__( self ):
        self.PID_JSON_PATH = unicode( os.environ['BELL_TASKS_META__PID_DICT_JSON_PATH'] )
        self.OUTPUT_PATH = unicode( os.environ['BELL_TASKS_META__METADATA_ONLY_ACCNUMS_JSON_PATH'] )

    def list_metadata_only_accession_numbers( self ):
        """ Saves a json list of accession_numbers.
            Called manuallly per readme. """
        logger.debug( 'in utils.make_metadata_only_list.MetadataOnlyLister.list_metadata_only_accession_numbers(); starting' )
        with open( self.PID_JSON_PATH ) as f:
            dct = json.loads( f.read() )
        dct_lst = sorted( dct['final_accession_pid_dict'].items() )
        lst_to_queue = []
        for (accession_number, pid) in dct_lst:
            if pid == None and not accession_number == "null":
                lst_to_queue.append( accession_number.strip() )
        data = {
            'datetime': unicode( datetime.datetime.now() ), 'count': len( lst_to_queue ), 'accession_numbers': lst_to_queue }
        self.output_listing( data )

    def output_listing( self, data ):
        """ Saves json file.
            Called by list_metadata_only_accession_numbers() """
        jsn = json.dumps( data, indent=2, sort_keys=True )
        with open( self.OUTPUT_PATH, 'w' ) as f:
            f.write( jsn )
        return

    # end class MetadataOnlyLister


def run_metadata_only_lister():
    """ Runs main MetadataOnlyLister() function.
        Called manually per readme() """
    lister = MetadataOnlyLister()
    lister.list_metadata_only_accession_numbers()
    return


class MetadataCreator( object ):
    """ Handles metadata-creation related tasks. """

    def __init__( self, logger ):
        self.logger = logger
        self.SOURCE_FULL_JSON_METADATA_PATH = unicode( os.environ['BELL_TASKS_META__FULL_JSON_METADATA_PATH'] )
        self.API_URL = unicode( os.environ['BELL_TASKS_META__AUTH_API_URL'] )
        self.API_IDENTITY = unicode( os.environ['BELL_TASKS_META__AUTH_API_IDENTITY'] )
        self.API_KEY = unicode( os.environ['BELL_TASKS_META__AUTH_API_KEY'] )
        self.MODS_SCHEMA_PATH = unicode( os.environ['BELL_TASKS_META__MODS_XSD_PATH'] )
        self.OWNING_COLLECTION = unicode( os.environ['BELL_TASKS_META__OWNING_COLLECTION_PID'] )
        self.TRACKER_PATH = unicode( os.environ['BELL_TASKS_META__TRACKER_JSON_PATH'] )

    def create_metadata_only_object( self, accession_number ):
        """ Gathers source metadata, prepares call to item-api, calls it, and confirms creation.
            Called by run_create_metadata_only_object() """
        self.logger.debug( 'in metadata.MetadataCreator.create_metadata_only_object(); starting' )
        params = self.set_basic_params()
        item_dct = self.grab_item_dct( accession_number )
        params['ir'] = self.make_ir_params( item_dct )
        params['mods'] = self.make_mods_params( item_dct )
        ( file_obj, param_string ) = self.prep_content_datastream( item_dct )
        params['content_streams'] = param_string
        self.logger.debug( 'in metadata.MetadataCreator.create_metadata_only_object(); params, %s' % pprint.pformat(params) )
        pid = self.perform_post( params, file_obj )  # perform_post() closes the file
        self.track_progress( accession_number, pid )
        return

    def set_basic_params( self ):
        """ Sets forthright params.
            Called by run_create_metadata_only_object() """
        params = {
            'identity': self.API_IDENTITY,
            'authorization_code': self.API_KEY,
            'additional_rights': 'BDR_PUBLIC#discover,display+Bell Gallery#discover,display,modify,delete',
            'rels': json.dumps( {'owning_collection': self.OWNING_COLLECTION} )
            # 'content_model': 'CommonMetadataDO'
            }
        return params

    def grab_item_dct( self, accession_number ):
        """ Loads data for given accession_number.
            Called by create_metadata_only_object() """
        self.logger.debug( 'in metadata.MetadataCreator.grab_item_dct(); accession_number, %s' % accession_number )
        with open( self.SOURCE_FULL_JSON_METADATA_PATH ) as f:
            metadata_dct = json.loads( f.read() )
        items = metadata_dct['items']
        item_dct = items[ accession_number ]
        self.logger.debug( 'in metadata.MetadataCreator.grab_item_dct(); item_dct, %s' % pprint.pformat(item_dct) )
        return item_dct

    def make_ir_params( self, item_dct ):
        """ Returns json of ir params.
            Called by create_metadata_only_object() NOTE: ir-collection-id in rels -- NOTE: go xml route."""
        ir_param = { 'parameters': {'depositor_name': 'Bell Gallery'} }
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
        file_obj = filelike.join( [SIO(jsn)] )  # this file_obj works with requests; vanilla StringIO didn't
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
        time.sleep( .5 )
        try:
            r = requests.post( self.API_URL, data=params, files=files, verify=False )
        except Exception as e:
            self._handle_post_exception( e, file_obj )
        file_obj.close()
        self.logger.debug( 'in metadata.MetadataCreator.perform_post(); r.status_code, `{status_code}`; r.content, ```{content}```'.format(status_code=r.status_code, content=r.content.decode('utf-8', 'replace')) )
        response_data = json.loads( r.content.decode('utf-8') )
        pid = response_data['pid']
        return pid

    def _handle_post_exception( self, e, file_obj ):
        self.logger.error( 'in metadata.MetadataCreator.perform_post(); exception, ```{}```'.format(unicode(repr(e))) )
        file_obj.close()
        raise Exception( 'failure on metadata.MetadataCreator.perform_post()' )
        return

    def track_progress( self, accession_number, pid ):
        """ Logs progress to json file.
            TODO: log progress to redis hash.
            Called by create_metadata_only_object() """
        try:
            with open( self.TRACKER_PATH ) as f:
                dct = json.loads( f.read() )
        except ( IOError, ValueError ):
            dct = {}
        dct[accession_number] = pid
        with open( self.TRACKER_PATH, 'w' ) as f:
            f.write( json.dumps(dct, indent=2, sort_keys=True) )
        return

    # end class MetadataCreator


class MetadataUpdater( object ):
    """ Handles metadata-creation related tasks.
        TODO: once this is part of the regular production, refactor relevant functions with MetadataCreator(). """

    def __init__( self ):
        self.SOURCE_FULL_JSON_METADATA_PATH = unicode( os.environ['BELL_TASKS_META__FULL_JSON_METADATA_PATH'] )
        self.API_URL = unicode( os.environ['BELL_TASKS_META__AUTH_API_URL'] )
        self.API_IDENTITY = unicode( os.environ['BELL_TASKS_META__AUTH_API_IDENTITY'] )
        self.API_KEY = unicode( os.environ['BELL_TASKS_META__AUTH_API_KEY'] )
        self.MODS_SCHEMA_PATH = unicode( os.environ['BELL_TASKS_META__MODS_XSD_PATH'] )
        self.OWNING_COLLECTION = unicode( os.environ['BELL_TASKS_META__OWNING_COLLECTION_PID'] )
        self.TRACKER_PATH = unicode( os.environ['BELL_TASKS_META__TRACKER_JSON_PATH'] )

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
            'additional_rights': 'BDR_PUBLIC#discover,display+Bell Gallery#discover,display,modify,delete',
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
        file_obj = filelike.join( [SIO(jsn)] )  # this file_obj works with requests; vanilla StringIO didn't
        param_string = json.dumps( [{
            'dsID': 'bell_metadata',
            'file_name': 'bell_item.json',
            'mimetype': 'application/javascript'
            }] )
        return_tuple = ( file_obj, param_string )
        logger.debug( 'return_tuple, ```{}```'.format(pprint.pformat(return_tuple)) )
        return return_tuple

    # def perform_update( self, params, file_obj ):
    #     """ Hits api w/patch?
    #         Called by update_object_metadata() """
    #     logger.debug( 'api-url, ```{}```'.format(self.API_URL) )
    #     files = { 'bell_item.json': file_obj }
    #     time.sleep( .5 )
    #     try:
    #         r = requests.put( self.API_URL, data=params, files=files, verify=False )
    #         pass
    #     except Exception as e:
    #         self._handle_update_exception( e, file_obj )
    #     file_obj.close()
    #     logger.debug( 'r.status_code, `{status_code}`; r.content, ```{content}```'.format(status_code=r.status_code, content=r.content.decode('utf-8', 'replace')) )
    #     response_data = json.loads( r.content.decode('utf-8') )
    #     pid = response_data['pid']
    #     return pid

    def perform_update( self, params ):
        """ Hits api w/patch?
            Called by update_object_metadata() """
        logger.debug( 'api-url, ```{}```'.format(self.API_URL) )
        time.sleep( .5 )
        try:
            r = requests.put( self.API_URL, data=params, verify=False )
            pass
        except Exception as e:
            self._handle_update_exception( e )
        logger.debug( 'r.status_code, `{status_code}`; r.content, ```{content}```'.format(status_code=r.status_code, content=r.content.decode('utf-8', 'replace')) )
        response_data = json.loads( r.content.decode('utf-8') )
        pid = response_data['pid']
        return pid

    # def _handle_update_exception( self, e, file_obj ):
    #     logger.error( 'exception hitting update-api, ```{}```'.format(unicode(repr(e))) )
    #     file_obj.close()
    #     raise Exception( 'failure on metadata.MetadataCreator.perform_update()' )
    #     return

    def _handle_update_exception( self, e ):
        logger.error( 'exception hitting update-api, ```{}```'.format(unicode(repr(e))) )
        raise Exception( 'failure on metadata.MetadataCreator.perform_update()' )
        return

    # end class MetadataUpdater()


## runners ##

# logger = bell_logger.setup_logger()

def run_enqueue_create_metadata_only_jobs():
    """ Prepares list of accession numbers and enqueues jobs.
        Called manually. """
    METADATA_ONLY_JSON = unicode( os.environ['BELL_TASKS_META__METADATA_ONLY_ACCNUMS_JSON_PATH'] )
    with open( METADATA_ONLY_JSON ) as f:
        dct = json.loads( f.read() )
    accession_numbers = dct['accession_numbers']
    for (i, accession_number) in enumerate( accession_numbers ):
        print 'i is, `%s`' % i
        if i+1 > 500:
            break
        q.enqueue_call(
          func='bell_code.tasks.metadata.run_create_metadata_only_object',
          kwargs={ 'accession_number': accession_number },
          timeout=600 )
    print 'done'
    return

def run_create_metadata_only_object( accession_number ):
    """ Runner for create_metadata_only_object()
        Called by job enqueued by run_enqueue_create_metadata_only_jobs() """
    m = MetadataCreator( logger )
    m.create_metadata_only_object( accession_number )
    return
