# -*- coding: utf-8 -*-

""" Handles metadata-related tasks. """

import json, os, sys
from bell_code import bell_logger
from bell_code.tasks import task_manager


class MetadataHandler( object ):
    """ Handles metadata-related tasks. """

    def __init__( self, data, logger ):
        self.data = data
        self.logger = logger

    def check_create_metadata( self ):
        """ Determines whether a metadata object needs to be created.
            Called by run_check_create_metadata(). """
        acc_num = self.data[u'item_data_dict'][u'calc_accession_id']
        pid = self._check_pid(acc_num)
        if len( pid ) > 0:
            create_metadata = False
        else:
            create_metadata = True
        task_manager.determine_next_task( current_task=unicode(sys._getframe().f_code.co_name), logger=self.logger,
            data={ u'item_data': self.data[u'item_data_dict'], u'pid': pid, u'create_metadata': create_metadata } )
        return

    def check_update_metadata( self ):
        """ Determines whether existing metadata needs to be updated.
            Called by run_check_update_metadata(). """
        update_metadata = False  # TODO: build metadata-dict,
                                 #       do an item-api solr query and compare it to the existing fedora metadata-dict,
                                 #       and return {u'update_metadata':True} or False
        task_manager.determine_next_task( current_task=unicode(sys._getframe().f_code.co_name), logger=self.logger,
            data={ u'item_data': self.data[u'item_data'], u'pid': self.data[u'pid'], u'update_metadata': update_metadata } )
        return

    ## helpers ##

    def _check_pid( self, acc_num ):
        """ Checks if accession number has a pid and returns it, or None.
            Called by check_create_metadata() """
        PID_FILE_PATH = os.environ.get(u'BELL_METADATA__PID_DICT_JSON_PATH')
        with open( PID_FILE_PATH ) as f:
            full_pid_data_dict = json.loads( f.read() )
        pid = full_pid_data_dict[u'final_accession_pid_dict'][acc_num]
        self.logger.info( u'in metadata._check_pid(); pid, %s' % pid )
        return pid

    # end class MetadataHandler()


## runners ##

logger = bell_logger.setup_logger()

def run_check_create_metadata( data ):
    """ Runner for check_create_metadata().
        Called by queue-job triggered by tasks.task_manager.determine_next_task(). """
    assert sorted(data.keys())[0:2] == [u'ARTISTS::artist_alias', u'ARTISTS::artist_birth_country_id'], sorted(data.keys())[0:1]
    new_data = {}
    new_data[u'item_data_dict'] = data
    mh = MetadataHandler( new_data, logger )
    mh.check_create_metadata()
    return

def run_check_update_metadata( data ):
    """ Runner for check_update_metadata().
        Called by queue-job triggered by tasks.task_manager.determine_next_task(). """
    assert sorted( data.keys() ) == [u'item_data', u'pid'], sorted( data.keys() )
    mh = MetadataHandler( data, logger )
    mh.check_update_metadata()
    return
