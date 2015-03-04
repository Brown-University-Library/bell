# -*- coding: utf-8 -*-

""" Handles metadata-related tasks. """

import json, os, pprint, sys
import redis, rq
from bell_code import bell_logger

queue_name = unicode( os.environ.get(u'BELL_QUEUE_NAME') )
q = rq.Queue( queue_name, connection=redis.Redis() )
r = redis.StrictRedis( host='localhost', port=6379, db=0 )


class MetadataHandler( object ):
    """ Handles metadata-related tasks.
        TODO: call determine_next_task() from runners, not from within class. """

    def __init__( self, logger ):
        self.logger = logger

    def create_metadata_only_listing( self ):
        """ Loads necessary json files, prepares & returns acession-number list of metadata objects to be created.
            Called manually. """
        self.logger.debug( u'in metadata.MetadataHandler.create_metadata_only_listing(); starting' )
        with open( unicode(os.environ.get(u'BELL_METADATA__PID_DICT_JSON_PATH')) ) as f:
            dct = json.loads( f.read() )
        dct_lst = sorted( dct[u'final_accession_pid_dict'].items() )
        lst_to_queue = []
        for (accession_number, pid) in dct_lst:
            if pid == None and not accession_number == "null":
                lst_to_queue.append( accession_number.strip() )
        return lst_to_queue

    def create_metadata_only_object( self, accession_number ):
        """ Gathers source metadata, prepares call to item-api, calls it, and confirms creation.
            Called by run_create_metadata_only_object() """
        self.logger.debug( u'in metadata.MetadataHandler.create_metadata_only_object(); starting' )
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

    # end class MetadataHandler()


## runners ##

logger = bell_logger.setup_logger()

def run_create_metadata_only_listing():
    """ Runner for create_metadata_only_listing()
        Called manually:
        >>> from tasks import metadata
        >>> metadata.run_create_metadata_only_listing() """
    mh = MetadataHandler( logger )
    metadata_only_lst = mh.create_metadata_only_listing()
    pprint.pprint( metadata_only_lst )
    return

def run_create_metadata_only_listing_and_enqueue_jobs():
    """ Runner for create_metadata_only_listing()
        Called manually:
        >>> from tasks import metadata
        >>> metadata.run_create_metadata_only_listing() """
    mh = MetadataHandler( logger )
    metadata_only_lst = mh.run_create_metadata_only_listing_and_enqueue_jobs()
    for accession_number in metadata_only_lst:
        q.enqueue_call( func=u'bell_code.tasks.metadata.run_create_metadata_only_object', kwargs={u'accession_number': accession_number}, timeout=600 )
    return

def run_create_metadata_only_object( accession_number ):
    """ Runner for create_metadata_only_object()
        Called by queued job triggered by MetadataHandler.create_metadata_only_listing() """
    mh = mh = MetadataHandler( logger )
    mh.create_metadata_only_object( accession_number )
    return
