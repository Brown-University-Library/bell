# -*- coding: utf-8 -*-

""" Handles metadata-related tasks. """

import datetime, json, os, pprint, sys
import redis, rq
from bell_code import bell_logger

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

    # end class MetadataOnlyLister()


class MetadataCreator( object ):
    """ Handles metadata-related tasks. """

    def __init__( self, logger ):
        self.logger = logger

    def create_metadata_only_object( self, accession_number ):
        """ Gathers source metadata, prepares call to item-api, calls it, and confirms creation.
            Called by run_create_metadata_only_object() """
        self.logger.debug( u'in metadata.MetadataCreator.create_metadata_only_object(); starting' )
        # item_dct = self.grab_item_dct( accession_number )
        # rights_params = self.make_rights_params( item_dct )
        # ir_params = self.make_ir_params( item_dct )
        # mods_params = self.make_mods_params( item_dct )
        # pid = self.create_object( rights_params, ir_params, mods_params )
        # self.logger.debug( u'in metadata.MetadataHandler.create_metadata_only_object(); accession_number `%s` object created with pid `%s`' % (accession_number, pid) )
        # if self.confirm_created_metadata_object( pid ) == False:
        #     raise Exception( u'could not confirm creation accession_number `%s` ingestion at pid `%s`' % (accession_number, pid) )
        return

    ## helpers ##

    # end class MetadataCreator()


## runners ##

logger = bell_logger.setup_logger()

def run_create_metadata_only_object( accession_number ):
    """ Runner for create_metadata_only_object()
        Called by queued job triggered by... """
    m = MetadataCreator( logger )
    m.create_metadata_only_object( accession_number )
    return
