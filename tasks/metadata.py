# -*- coding: utf-8 -*-

""" Handles metadata-related tasks. """

import datetime, json, os, pprint, sys
from StringIO import StringIO as SIO
import filelike, redis, requests, rq
from bell_code import bell_logger
from bell_code.utils import mods_builder


queue_name = unicode( os.environ.get(u'BELL_QUEUE_NAME') )
q = rq.Queue( queue_name, connection=redis.Redis() )
r = redis.StrictRedis( host='localhost', port=6379, db=0 )


class MetadataOnlyLister( object ):
    """ Creates json file of accession numbers for which new metatdata-only objects will be created. """

    def __init__( self ):
        self.PID_JSON_PATH = unicode( os.environ[u'BELL_UTILS__PID_DICT_JSON_PATH'] )
        self.OUTPUT_PATH = unicode( os.environ[u'BELL_UTILS__METADATA_ONLY_ACCNUMS_JSON_PATH'] )

    def list_metadata_only_accession_numbers( self ):
        """ Saves a json list of accession_numbers.
            Called manuallly per readme. """
        logger.debug( u'in utils.make_metadata_only_list.MetadataOnlyLister.list_metadata_only_accession_numbers(); starting' )
        with open( self.PID_JSON_PATH ) as f:
            dct = json.loads( f.read() )
        dct_lst = sorted( dct[u'final_accession_pid_dict'].items() )
        lst_to_queue = []
        for (accession_number, pid) in dct_lst:
            if pid == None and not accession_number == "null":
                lst_to_queue.append( accession_number.strip() )
        data = {
            u'datetime': unicode( datetime.datetime.now() ), u'count': len( lst_to_queue ), u'accession_numbers': lst_to_queue }
        self.output_listing( data )

    def output_listing( self, data ):
        """ Saves json file.
            Called by list_metadata_only_accession_numbers() """
        jsn = json.dumps( data, indent=2, sort_keys=True )
        with open( self.OUTPUT_PATH, u'w' ) as f:
            f.write( jsn )
        return

    # end class MetadataOnlyLister


class MetadataCreator( object ):
    """ Handles metadata-related tasks. """

    def __init__( self, logger ):
        self.logger = logger
        self.SOURCE_FULL_JSON_METADATA_PATH = unicode( os.environ[u'BELL_TASKS_META__FULL_JSON_METADATA_PATH'] )
        self.API_URL = unicode( os.environ[u'BELL_TASKS_META__AUTH_API_URL'] )
        self.API_IDENTITY = unicode( os.environ[u'BELL_TASKS_META__AUTH_API_IDENTITY'] )
        self.API_KEY = unicode( os.environ[u'BELL_TASKS_META__AUTH_API_KEY'] )
        self.MODS_SCHEMA_PATH = unicode( os.environ[u'BELL_TASKS_META__MODS_XSD_PATH'] )
        self.OWNING_COLLECTION = unicode( os.environ[u'BELL_TASKS_META__OWNING_COLLECTION_PID'] )

    def create_metadata_only_object( self, accession_number ):
        """ Gathers source metadata, prepares call to item-api, calls it, and confirms creation.
            Called by run_create_metadata_only_object() """
        self.logger.debug( u'in metadata.MetadataCreator.create_metadata_only_object(); starting' )
        params = self.set_basic_params()
        item_dct = self.grab_item_dct( accession_number )
        params[u'ir'] = self.make_ir_params( item_dct )
        params[u'mods'] = self.make_mods_params( item_dct )
        params[u'rels'] = json.dumps( {u'owning_collection': self.OWNING_COLLECTION} )
        ( file_obj, param_string ) = self.prep_content_datastream( item_dct )
        params[u'content_streams'] = param_string
        self.logger.debug( u'in metadata.MetadataCreator.create_metadata_only_object(); params, %s' % pprint.pformat(params) )
        pid = self.perform_post( params, file_obj )  # perform_post() closes the file
        return

    def set_basic_params( self ):
        """ Sets forthright params.
            Called by run_create_metadata_only_object() """
        params = {
            u'identity': self.API_IDENTITY,
            u'authorization_code': self.API_KEY,
            u'additional_rights': u'BDR_PUBLIC#discover,display+Bell Gallery#discover,display,modify,delete',
            # u'content_model': u'CommonMetadataDO'
            }
        return params


    def grab_item_dct( self, accession_number ):
        """ TEST -- Loads data for given accession_number.
            Called by create_metadata_only_object() """
        self.logger.debug( u'in metadata.MetadataCreator.grab_item_dct(); accession_number, %s' % accession_number )
        item_dct = json.loads( '{"ARTISTS::artist_nationality_name": [null], "ARTISTS::artist_alias": [null], "object_title": "Portrait of Linda Tanner", "credit_line": "Gift of Louis A. Tanner \'55 and Linda P. Tanner (Vassar) \'61", "SERIES::series_end_year": ["2007"], "ARTISTS::artist_birth_year": [null], "series_id": "0", "ARTISTS::calc_nationality": [null], "ARTISTS::artist_first_name": ["Sydney"], "object_depth": null, "ARTISTS::artist_middle_name": [null], "OBJECT_ARTISTS::artist_id": ["1293"], "object_year_start": "1966", "SERIES::series_start_year": ["2005"], "OBJECT_ARTISTS::primary_flag": ["yes"], "object_height": null, "object_date": "1966", "ARTISTS::artist_birth_country_id": [null], "object_width": null, "object_year_end": "1966", "OBJECT_ARTISTS::artist_role": [null], "object_id": "6478", "SERIES::series_name": [null], "ARTISTS::artist_lifetime": [null], "object_image_scan_filename": null, "MEDIA::object_medium_name": "Drawing", "ARTISTS::use_alias_flag": ["no"], "object_medium": "Pencil", "ARTISTS::artist_last_name": ["Tillman"], "image_width": null, "OBJECT_MEDIA_SUB::media_sub_id": ["44"], "calc_accession_id": "D 2012.3.46", "image_height": null, "ARTISTS::artist_death_year": [null], "MEDIA_SUB::sub_media_name": ["Pencil"], "ARTISTS::calc_artist_full_name": ["Sydney Tillman"]}' )
        self.logger.debug( u'in metadata.MetadataCreator.grab_item_dct(); item_dct, %s' % pprint.pformat(item_dct) )
        return item_dct

    # def grab_item_dct( self, accession_number ):
    #     """ Loads data for given accession_number.
    #         Called by create_metadata_only_object() """
    #     self.logger.debug( u'in metadata.MetadataCreator.grab_item_dct(); accession_number, %s' % accession_number )
    #     with open( self.SOURCE_FULL_JSON_METADATA_PATH ) as f:
    #         metadata_dct = json.loads( f.read() )
    #     items = metadata_dct[u'items']
    #     item_dct = items[ accession_number ]
    #     self.logger.debug( u'in metadata.MetadataCreator.grab_item_dct(); item_dct, %s' % pprint.pformat(item_dct) )
    #     return item_dct


    def make_ir_params( self, item_dct ):
        """ Returns json of ir params.
            Called by create_metadata_only_object() NOTE: ir-collection-id in rels -- NOTE: go xml route."""
        ir_param = { u'parameters': {u'depositor_name': u'Bell Gallery'} }
        self.logger.debug( u'in metadata.MetadataCreator.make_ir_params(); ir_param, %s' % pprint.pformat(ir_param) )
        jsn = json.dumps( ir_param )
        return jsn

    def make_mods_params( self, item_dct ):
        """ Returns json if mods params.
            Called by create_metadata_only_object() """
        mb = mods_builder.ModsBuilder()
        return_type = u'return_string'  # or u'return_object'
        mods_xml_dct = mb.build_mods_object( item_dct, self.MODS_SCHEMA_PATH, return_type )
        self.logger.debug( u'in metadata.MetadataCreator.make_mods_params(); mods_xml_dct, %s' % pprint.pformat(mods_xml_dct) )
        mods_xml = mods_xml_dct[u'data']
        mods_param = { u'xml_data': mods_xml }
        jsn = json.dumps( mods_param )
        return jsn

    def prep_content_datastream( self, item_dct ):
        """ Returns file-like object containing the item_dct.
            Called by create_metadata_only_object() """
        jsn = json.dumps( item_dct )
        file_obj = filelike.join( [SIO(jsn)] )  # this file_obj works with requests; vanilla StringIO didn't
        param_string = json.dumps( [{
            u'dsID': u'bell_metadata',
            u'file_name': u'bell_item.json',
            u'mimetype': u'application/javascript'
            }] )
        return_tuple = ( file_obj, param_string )
        return return_tuple

    def perform_post( self, params, file_obj ):
        """ Hits api w/post. Returns pid.
            Called by create_metadata_only_object() """
        self.logger.debug( u'in metadata.MetadataCreator.perform_post(); type(file_obj), %s' % type(file_obj) )
        files = { u'bell_item.json': file_obj }
        r = requests.post( self.API_URL, data=params, files=files )
        file_obj.close()
        self.logger.debug( u'in metadata.MetadataCreator.perform_post(); r.status_code, %s' % r.status_code )
        response_data = json.loads( r.content.decode(u'utf-8') )
        self.logger.debug( u'in metadata.MetadataCreator.perform_post(); response_data, %s' % pprint.pformat(response_data) )
        pid = response_data[u'pid']
        return pid

    # end class MetadataCreator


## runners ##

logger = bell_logger.setup_logger()

def run_create_metadata_only_object( accession_number ):
    """ Runner for create_metadata_only_object()
        Called by queued job triggered by... """
    m = MetadataCreator( logger )
    m.create_metadata_only_object( accession_number )
    return
