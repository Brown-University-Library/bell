# -*- coding: utf-8 -*-

import json, os, pprint, sys, time
import requests
from bell_code import bell_logger
from mysolr import Solr
from bell_code.tasks import task_manager

""" Prepares data for updating and deleting custom-index records.
    Executes custom-index changes as per readme.md """


class CustomIndexPidsLister( object ):
    """ Prepares list of pids from custom-solr index. """

    def __init__( self, logger=None ):
        self.logger = logger
        self.BELL_CUSTOM_IDX_ROOT_URL = unicode( os.environ[u'BELL_TASKS_IDXR__BELL_CUSTOM_IDX_ROOT_URL'] )
        self.OUTPUT_PATH = unicode( os.environ[u'BELL_TASKS_IDXR__CUSTOM_IDX_DELETES_PIDS_JSON_PATH'] )

    def grab_custom_index_pids( self ):
        """ Manages calls to create `h__pids_from_custom_index_list.json`.
            Called by runner as per readme.md """
        pids = self.grab_pids()
        self.output_list( pids )

    def grab_pids( self ):
        """ Returns list of pids from custom-index.
            Called by grab_custom_index_pids() """
        params = {
            u'q': u'*:*', u'fl': u'pid', u'start': u'0', u'rows': u'70000', u'wt': u'json' }
        r = requests.get( self.BELL_CUSTOM_IDX_ROOT_URL, params=params, verify=False )
        self.logger.debug( u'in tasks.indexer.CustomIndexPidsLister.grab_pids(); r.url, `%s`' % r.url )
        dct = json.loads( r.content.decode(u'utf-8') )
        pid_dcts = dct[u'response'][u'docs']
        self.logger.debug( u'in tasks.indexer.CustomIndexPidsLister.grab_pids(); len(pid_dcts), `%s`' % len(pid_dcts) )
        self.logger.debug( u'in tasks.indexer.CustomIndexPidsLister.grab_pids(); pid_dcts[0]["pid"], `%s`' % pid_dcts[0]["pid"] )
        pids = []
        for pid_dct in pid_dcts:
            pids.append( pid_dct[u'pid'] )
        return sorted( pids )

    def output_list( self, pid_list ):
        """ Saves json file.
            Called by grab_bdr_pids() """
        jsn = json.dumps( pid_list, indent=2, sort_keys=True )
        with open( self.OUTPUT_PATH, u'w' ) as f:
            f.write( jsn )
        return

    # end class CustomIndexPidsLister


class DeletePidsLister( object ):
    """ Prepares lists of custom-solr items to be removed. """

    def __init__( self, logger=None ):
        self.logger = logger
        self.BELL_SOURCE_DATA_JSON_PATH = unicode( os.environ[u'BELL_TASKS_IDXR__BELL_SOURCE_DATA_JSON_PATH'] )
        self.CUSTOM_IDX_PIDS_JSON_PATH = unicode( os.environ[u'BELL_TASKS_IDXR__CUSTOM_IDX_PIDS_JSON_PATH'] )
        self.OUTPUT_PATH = unicode( os.environ[u'BELL_TASKS_IDXR__CUSTOM_IDX_DELETES_PIDS_JSON_PATH'] )

    def make_delete_pids_list( self ):
        """ Manages the creation of lists of pids to add/update, and to delete.
            Called by run_make_pids_from_bdr_list() """
        pids_from_xml_dump = self.make_xml_pids()
        pids_from_custom_index = self.grab_custom_index_pids()
        self.logger.debug( u'in tasks.indexer.UpdateAndDeletePidsLister.make_delete_pids_list(); len(pids_from_xml_dump), `%s`' % len(pids_from_xml_dump) )
        self.logger.debug( u'in tasks.indexer.UpdateAndDeletePidsLister.make_delete_pids_list(); len(pids_from_custom_index), `%s`' % len(pids_from_custom_index) )
        deletes = self.prepare_deletes( pids_from_xml_dump, pids_from_custom_index )
        self.output_list( deletes )
        return

    def make_xml_pids( self ):
        """ Returns list of pids from bell-source-data.
            Called by make_delete_pids_list() """
        with open( self.BELL_SOURCE_DATA_JSON_PATH ) as f:
            dct = json.loads( f.read() )
        assert sorted( dct.keys() ) == [ u'count', u'datetime', u'final_accession_pid_dict' ], sorted( json_dict.keys() )
        assert dct[u'count'][u'count_null'] == 0  # not initially null, but is after re-running after ingestions
        pids_from_accession_numbers = dct[u'final_accession_pid_dict'].values()
        return sorted( pids_from_accession_numbers )

    def grab_custom_index_pids( self ):
        """ Loads list of custom-index pids saved previously.
            Called by make_delete_pids_list() """
        with open( self.CUSTOM_IDX_PIDS_JSON_PATH ) as f:
            pids_from_custom_index = json.loads( f.read() )
        return pids_from_custom_index

    def prepare_deletes( self, pids_from_xml_dump, pids_from_custom_index ):
        """ Runs set operations to make lists.
            Called by make_delete_pids_list() """
        deletes_set = set(pids_from_custom_index) - set(pids_from_xml_dump)
        self.logger.debug( u'in tasks.indexer.UpdateAndDeletePidsLister.prepare_deletes(); deletes_set, `%s`' % deletes_set )
        deletes_list = list( deletes_set )
        return deletes_list

    def output_list( self, deletes ):
        """ Saves json file.
            Called by make_delete_pids_list() """
        jsn = json.dumps( deletes, indent=2, sort_keys=True )
        with open( self.OUTPUT_PATH, u'w' ) as f:
            f.write( jsn )
        return

    # end class DeletePidsLister


class UpdatePidsLister( object ):
    """ Prepares lists of custom-solr items to be created/updated. """

    def __init__( self, logger ):
        self.logger = logger
        self.SRC_ACC_NUM_TO_DATA_DCT_JSON_PATH = unicode( os.environ[u'BELL_TASKS_IDXR__SRC_ACC_NUM_TO_DATA_DCT_JSON_PATH'] )
        self.SRC_ACC_NUM_TO_PID_DCT_JSON_PATH = unicode( os.environ[u'BELL_TASKS_IDXR__SRC_ACC_NUM_TO_PID_DCT_JSON_PATH'] )
        self.OUTPUT_PATH = unicode( os.environ[u'BELL_TASKS_IDXR__CUSTOM_IDX_UPDATES_DATA_JSON_PATH'] )

    def make_update_pids_lst( self ):
        """ Loads up two source-data dicts and outputs an accession-number-to-data dict that includes pid-info.
            Called by runner as per readme.md """
        accession_number_to_data_dct = self.load_data_dct()
        accession_number_to_pid_dct = self.load_pid_dct()
        updated_accesson_number_to_data_dct_lst = self.add_pid( accession_number_to_data_dct, accession_number_to_pid_dct )
        self.output_lst( updated_accesson_number_to_data_dct_lst )
        return

    def load_data_dct( self ):
        """ Loads initial source data dct.
            Called by make_update_pids_lst() """
        with open( self.SRC_ACC_NUM_TO_DATA_DCT_JSON_PATH ) as f:
            dct = json.loads( f.read() )
            accession_number_to_data_dct = dct[u'items']
        return accession_number_to_data_dct

    def load_pid_dct( self ):
        """ Loads source pid dct.
            Called by make_update_pids_lst() """
        with open( self.SRC_ACC_NUM_TO_PID_DCT_JSON_PATH ) as f:
            dct = json.loads( f.read() )
            accession_number_to_pid_dct = dct[u'final_accession_pid_dict']
        return accession_number_to_pid_dct

    def add_pid( self, accession_number_to_data_dct, accession_number_to_pid_dct ):
        """ Adds pid to data-dct, returns list of accession-number-to-updated-data-dct.
            Called by make_update_pids_lst() """
        updated_data_lst = []
        for (i, key) in enumerate( sorted(accession_number_to_data_dct.keys()) ):
            if i + 1 > 70000:
                break
            print key
            value_dct = accession_number_to_data_dct[key]
            pid = accession_number_to_pid_dct[key]
            value_dct[u'pid'] = pid
            entry = { key: value_dct }
            updated_data_lst.append( entry )
        return updated_data_lst

    def output_lst( self, lst ):
        """ Saves json file.
            Called by make_delete_pids_list() """
        jsn = json.dumps( lst, indent=2, sort_keys=True )
        with open( self.OUTPUT_PATH, u'w' ) as f:
            f.write( jsn )
        return

    # end class UpdatePidsLister


class CustomIndexUpdater( object ):
    """ Handles custom solr index. """

    def __init__( self, logger=None ):
        self.logger = logger
        self.FULL_DATA_DCTS_JSON_PATH = unicode( os.environ[u'BELL_TASKS_IDXR__FULL_DATA_DCTS_JSON_PATH'] )
        self.REQUIRED_KEYS = [  # used by _validate_solr_dict()
            u'accession_number_original',
            u'author_birth_date',
            u'author_date',
            u'author_death_date',
            u'author_description',
            u'author_display',
            u'author_names_first',
            u'author_names_last',
            u'author_names_middle',
            u'image_height',
            u'image_width',
            u'jp2_image_url',
            u'location_physical_location',
            u'location_shelf_locator',
            u'master_image_url',
            u'note_provenance',
            u'object_date',
            u'object_depth',
            u'object_height',
            u'object_width',
            u'origin_datecreated_end',
            u'origin_datecreated_start',
            u'physical_description_extent',
            u'physical_description_material',
            u'physical_description_technique',
            u'pid',
            u'title',
            ]

    def enqueue_index_jobs( self ):
        """ Loads up json list of dicts and hits a prep&post job.
            Called by runner, triggered manually as per readme.md """
        with open( self.FULL_DATA_DCTS_JSON_PATH ) as f:
            accession_number_to_data_dct_lst = json.loads( f.read() )
        for (i, entry) in enumerate( accession_number_to_data_dct_lst ):  # entry: { accession_number: {data_key_a: data_value_a, etc} }
            if i + 1 > 1:
                break
            ( accession_number, data_dct ) = entry.items[0]
            q.enqueue_call(
              func=u'bell_code.tasks.indexer.run_update_custom_index_entry',
              kwargs={ u'accession_number': accession_number, u'data': data_dct },
              timeout=600 )
        return

    # end class CustomIndexUpdater


## runners ##

logger = bell_logger.setup_logger()

def run_make_pids_from_custom_index():
    """ Saves pids from custom-index to `h__pids_from_custom_index_list.json`.
        Called manually per readme.md """
    logger.debug( u'in tasks.indexer.run_make_pids_from_custom_index(); starting' )
    cip_lstr = CustomIndexPidsLister( logger )
    cip_lstr.grab_custom_index_pids()
    logger.debug( u'in tasks.indexer.run_make_pids_from_custom_index(); done' )
    return

def run_make_delete_pids_list():
    """ Saves custom-index pids to be deleted to `i__custom_index_delete_pids.json`.
        Called manually per readme.md """
    logger.debug( u'in tasks.indexer.run_make_delete_pids_list(); starting' )
    del_lstr = DeletePidsLister( logger )
    del_lstr.make_delete_pids_list()
    logger.debug( u'in tasks.indexer.run_make_delete_pids_list(); done' )
    return

def run_make_update_pids_list():
    """ Saves custom-index pids to be created/updated to `j__custom_index_update_pids.json`.
        Called manually per readme.md """
    logger.debug( u'in tasks.indexer.run_make_update_pids_list(); starting' )
    update_lstr = UpdatePidsLister( logger )
    update_lstr.make_update_pids_lst()
    logger.debug( u'in tasks.indexer.run_make_update_pids_list(); done' )
    return

def run_enqueue_index_jobs():
    """ Enqueues update-custom-solr-index-entry jobs.
        Called manually per readme.md """
    logger.debug( u'in tasks.indexer.run_enqueue_index_jobs(); starting' )
    idxr = CustomIndexUpdater( logger )
    idxr.enqueue_index_jobs()
    logger.debug( u'in tasks.indexer.run_enqueue_index_jobs(); done' )
    return

def run_update_custom_index_entry():
    """ Preps and executes post for single entry.
        Called by CustomIndexUpdater.enqueue_index_jobs() """
    logger.debug( u'in tasks.indexer.run_update_custom_index_entry(); starting' )
    idxr = CustomIndexUpdater( logger )
    idxr.update_custom_index_entry( accession_number, data_dct )
    logger.debug( u'in tasks.indexer.run_update_custom_index_entry(); done' )
    return

