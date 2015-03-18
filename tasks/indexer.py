# -*- coding: utf-8 -*-


""" Prepares data for updating and deleting custom-index records.
    Executes custom-index changes as per readme.md """

import json, os, pprint, sys, time
import redis, requests, rq
from bell_code import bell_logger
from mysolr import Solr
from bell_code.tasks import task_manager


queue_name = unicode( os.environ.get(u'BELL_QUEUE_NAME') )
q = rq.Queue( queue_name, connection=redis.Redis() )
r = redis.StrictRedis( host='localhost', port=6379, db=0 )


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
    """ Handles updating custom solr index.
        TODO: move the metadata-helpers into a different data-prep class. """

    def __init__( self, logger=None ):
        self.logger = logger
        self.FULL_DATA_DCTS_JSON_PATH = unicode( os.environ[u'BELL_TASKS_IDXR__FULL_DATA_DCTS_JSON_PATH'] )
        self.BDR_PUBLIC_ITEM_API_URL_ROOT = unicode( os.environ[u'BELL_TASKS_IDXR__BDR_PUBLIC_ITEM_API_URL_ROOT'] )
        self.CUSTOM_INDEX_SOLR_URL_ROOT = unicode( os.environ[u'BELL_TASKS_IDXR__CUSTOM_INDEX_SOLR_URL_ROOT'] )
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
            if i + 1 > 2:
                break
            print u'entry...'; print entry
            ( accession_number, data_dct ) = entry.items()[0]
            print u'accession_number...'; print accession_number
            print u'data_dct...'; pprint.pprint( data_dct )
            q.enqueue_call(
              func=u'bell_code.tasks.indexer.run_update_custom_index_entry',
              kwargs={ u'accession_number': accession_number, u'data_dct': data_dct, u'pid': data_dct[u'pid'] },
              timeout=600 )
        return

    def update_custom_index_entry( self, accession_number, data_dct, pid ):
        """ Manages prep & post of update custom index entry.
            Called by runner. """
        metadata_solr_dict = self.build_metadata_only_solr_dict( pid, data_dct )
        bdr_api_links_dict = self.grab_bdr_api_links_data( pid )
        updated_solr_dict = self.add_image_metadata( metadata_solr_dict, bdr_api_links_dict )
        validity = self.validate_solr_dict( updated_solr_dict )
        if validity:
            post_result = self.post_to_solr( updated_solr_dict )
            self.update_tracker( accession_number, post_result )
        return

    def build_metadata_only_solr_dict( self, pid, data_dct ):
        """ Builds dict-to-index using just basic item-dict data and pid; image-check handled in separate function.
            Called by run_update_custom_index_entry() """
        solr_dict = {}
        solr_dict[u'accession_number_original'] = data_dct[u'calc_accession_id']
        solr_dict[u'pid'] = pid
        solr_dict = self._set_author_dates( data_dct, solr_dict )
        solr_dict = self._set_author_description( data_dct, solr_dict )
        solr_dict = self._set_author_names( data_dct, solr_dict )
        solr_dict = self._set_height_width_depth( data_dct, solr_dict )
        solr_dict = self._set_locations( data_dct, solr_dict )
        solr_dict = self._set_note_provenance( data_dct, solr_dict )
        solr_dict = self._set_object_dates( data_dct, solr_dict )
        solr_dict = self._set_physical_extent( data_dct, solr_dict )
        solr_dict = self._set_physical_descriptions( data_dct, solr_dict )
        solr_dict = self._set_title( data_dct, solr_dict )
        self.logger.debug( u'in tasks.indexer.CustomIndexUpdater.build_metadata_only_solr_dict(); solr_dict, `%s`' % pprint.pformat(solr_dict) )
        return solr_dict

    def grab_bdr_api_links_data( self, pid ):
        """ Grabs and returns link info from item-api json.
            The links dict is used by tasks.indexer to add to the solr-metadata if needed. """
        url = u'%s/%s/' % ( self.BDR_PUBLIC_ITEM_API_URL_ROOT, pid )
        self.logger.debug( u'in tasks.indexer.CustomIndexUpdater.grab_bdr_api_links_data(); url, `%s`' % url )
        r = requests.get( url )
        api_dict = r.json()
        links_dict = api_dict[u'links']
        self.logger.debug( u'in tasks.indexer.CustomIndexUpdater.grab_bdr_api_links_data(); links_dict, `%s`' % pprint.pformat(links_dict) )
        return links_dict

    def add_image_metadata( self, solr_dict, links_dict ):
        """ Adds image metadata to dict-to-index. """
        solr_dict[u'jp2_image_url'] = self._set_image_urls__get_jp2_url( links_dict )
        solr_dict[u'master_image_url'] = self._set_image_urls__get_master_image_url( links_dict, solr_dict[u'jp2_image_url'] )
        self.logger.debug( u'in tasks.indexer.CustomIndexUpdater.add_image_metadata(); final solr_dict, `%s`' % pprint.pformat(solr_dict) )
        return solr_dict

    def validate_solr_dict( self, solr_dict ):
        """ Returns True if checks pass; False otherwise.
            Checks that required keys are present.
            Checks that there are no None values.
            Checks that any list values are not empty.
            Checks that no members of a list value are of NoneType.
            Called by build_metadata_only_solr_dict() """
        try:
            for required_key in self.REQUIRED_KEYS:
                # self.logger.debug( u'in tasks.indexer.CustomIndexUpdater._validate_solr_dict(); required_key: %s' % required_key )
                assert required_key in solr_dict.keys(), Exception( u'ERROR; missing required key: %s' % required_key )
            for key,value in solr_dict.items():
              assert value != None, Exception( u'ERROR; value is none for key: %s' % key )
              if type(value) == list:
                assert len(value) > 0, Exception( u'ERROR: key "%s" has a value of "%s", which is type-list, which is empty.' % ( key, value ) )
                for element in value:
                  assert element != None, Exception( u'ERROR: key "%s" has a value "%s", which is type-list, which contains a None element' % ( key, value ) )
            self.logger.debug( u'in tasks.indexer.CustomIndexUpdater.validate_solr_dict(); is valid' )
            return True
        except Exception as e:
            self.logger.error( u'in tasks.indexer.CustomIndexUpdater.validate_solr_dict(); exception is: %s' % unicode(repr(e)) )
            return False

    def post_to_solr( self, solr_dict ):
        """ Posts solr_dict to solr. """
        solr = Solr( self.CUSTOM_INDEX_SOLR_URL_ROOT )
        response = solr.update( [solr_dict], u'xml', commit=True )  # 'xml' param converts default json to xml for post; required for our old version of solr
        response_status = response.status
        self.logger.info( u'in tasks.indexer.CustomIndexUpdater.post_to_solr() [for custom-solr]; accession_number, %s; response_status, %s' % (solr_dict[u'accession_number_original'], response_status) )
        if not response_status == 200:
            raise Exception( u'custom-solr post problem logged' )
        return response_status

    ## metadata-only helpers ##

    def _set_author_dates( self, original_dict, solr_dict ):
        """ Sets author dates.
            Called by build_metadata_only_solr_dict() """
        solr_dict[u'author_birth_date'] = original_dict[u'ARTISTS::artist_birth_year']
        solr_dict[u'author_death_date'] = original_dict[u'ARTISTS::artist_death_year']
        solr_dict[u'author_date'] = original_dict[u'ARTISTS::artist_lifetime']
        solr_dict = self.__ensure_list_unicode_values( solr_dict, [u'author_birth_date', u'author_death_date', u'author_date'] )
        return solr_dict

    def _set_author_description( self, original_dict, solr_dict ):
        """ Sets author descriptions.
            Called by build_metadata_only_solr_dict() """
        solr_dict[u'author_description'] = original_dict[u'ARTISTS::calc_nationality']
        solr_dict = self.__ensure_list_unicode_values( solr_dict, [u'author_description'] )
        return solr_dict

    def _set_author_names( self, original_dict, solr_dict ):
        """ Sets author names.
            Called by build_metadata_only_solr_dict() """
        solr_dict[u'author_names_first'] = original_dict[u'ARTISTS::artist_first_name']
        solr_dict[u'author_names_middle'] = original_dict[u'ARTISTS::artist_middle_name']
        solr_dict[u'author_names_last'] = original_dict[u'ARTISTS::artist_last_name']
        solr_dict[u'author_display'] = original_dict[u'ARTISTS::calc_artist_full_name']
        solr_dict = self.__ensure_list_unicode_values( solr_dict, [u'author_names_first',
            u'author_names_middle', u'author_names_last', u'author_display'] )
        return solr_dict

    def _set_height_width_depth( self, original_dict, solr_dict ):
        """ Sets item dimensions.
            Called by build_metadata_only_solr_dict() """
        target_keys = [ u'image_width', u'image_height', u'object_width', u'object_height', u'object_depth' ]
        for entry in target_keys:
            if original_dict[entry] == None:
                solr_dict[entry] = u''
            else:
                solr_dict[entry] = original_dict[entry]
        return solr_dict

    def _set_locations( self, original_dict, solr_dict ):
        """ Returns location_physical_location & location_shelf_locator.
            Called by build_metadata_only_solr_dict() """
        solr_dict[u'location_physical_location'] = u'Bell Art Gallery'
        solr_dict[u'location_shelf_locator'] = u''
        if original_dict[u'MEDIA::object_medium_name'] != None:
          solr_dict[u'location_shelf_locator'] = original_dict[u'MEDIA::object_medium_name']
        return solr_dict

    def _set_note_provenance( self, original_dict, solr_dict ):
        """ Updates note_provenance.
            Called by build_metadata_only_solr_dict() """
        solr_dict[u'note_provenance'] = u''
        if original_dict[u'credit_line'] != None:
          solr_dict[u'note_provenance'] = original_dict[u'credit_line']
        return solr_dict

    def _set_object_dates( self, original_dict, solr_dict ):
        """ Updates object_date, origin_datecreated_start, origin_datecreated_end.
            Called by build_metadata_only_solr_dict() """
        solr_dict[u'object_date'] = u''
        solr_dict[u'origin_datecreated_start'] = u''
        solr_dict[u'origin_datecreated_end'] = u''
        if original_dict[u'object_date'] != None:
            solr_dict[u'object_date'] = original_dict[u'object_date']
        if original_dict[u'object_year_start'] != None:
            solr_dict[u'origin_datecreated_start'] = original_dict[u'object_year_start']
        if original_dict[u'object_year_end'] != None:
            solr_dict[u'origin_datecreated_end'] = original_dict[u'object_year_end']
        return solr_dict

    def _set_physical_extent( self, original_dict, solr_dict ):
        """ Updates physical_description_extent, which is a display field based on the five dimension fields.
            Called by build_metadata_only_solr_dict() """
        ( height, width, depth ) = self.__set_physical_extent_prep( original_dict )
        solr_dict[u'physical_description_extent'] = [ u'' ]
        if height and width and depth:
            solr_dict[u'physical_description_extent'] = [ u'%s x %s x %s' % (height, width, depth) ]
        elif height and width:
            solr_dict[u'physical_description_extent'] = [ u'%s x %s' % (height, width ) ]
        return solr_dict

    def  __set_physical_extent_prep( self, original_dict ):
        """ Sets initial height, width, and depth from possible image and object dimension fields.
            Called by _set_physical_extent() """
        temp_dict = {}
        for field in [ u'image_height', u'image_width', u'object_height', u'object_width', u'object_depth' ]:  # strip original-dict values
            temp_dict[field] = u''
            if original_dict[field]:
                temp_dict[field] = original_dict[field].strip()
        height = temp_dict[u'image_height'] if temp_dict[u'image_height'] else temp_dict[u'object_height']
        width = temp_dict[u'image_width'] if temp_dict[u'image_width'] else temp_dict[u'object_width']
        depth = temp_dict[u'object_depth']
        return ( height, width, depth )

    def _set_physical_descriptions( self, original_dict, solr_dict ):
        """ Updates physical_description_material, physical_description_technique.
            Called by build_metadata_only_solr_dict() """
        solr_dict[u'physical_description_material'] = [ u'' ]
        solr_dict[u'physical_description_technique'] = [ u'' ]
        if original_dict[u'object_medium'] != None:
            solr_dict[u'physical_description_material'] = [ original_dict[u'object_medium'] ]
        if original_dict[u'MEDIA_SUB::sub_media_name'] != None:
            if type( original_dict[u'MEDIA_SUB::sub_media_name'] ) == unicode:
                solr_dict[u'physical_description_technique'] = [ original_dict[u'MEDIA_SUB::sub_media_name'] ]
            elif type( original_dict[u'MEDIA_SUB::sub_media_name'] ) == list:
                cleaned_list = self.__ensure_list_unicode_values__handle_type_list( original_dict[u'MEDIA_SUB::sub_media_name'] )
                solr_dict[u'physical_description_technique'] = cleaned_list
        return solr_dict

    def _set_title( self, original_dict, solr_dict ):
        """ Updates title.
            Called by build_metadata_only_solr_dict() """
        solr_dict[u'title'] = u''
        if original_dict[u'object_title'] != None:
            solr_dict[u'title'] = original_dict[u'object_title']
        return solr_dict

    ## image helpers ##

    def _set_image_urls__get_jp2_url( self, links_dict ):
        """ Returns jp2 url or u''.
            Called by _set_image_urls() """
        try:
            image_url = links_dict[u'content_datastreams'][u'JP2']
        except:
            image_url = u''
        return image_url

    def _set_image_urls__get_master_image_url( self, links_dict, jp2_url ):
        """ Returns master image url or u''.
            Called by _set_image_urls() """
        image_url = None
        if jp2_url == u'':
            image_url = u''
        else:
            try:
                image_url = links_dict[u'content_datastreams'][u'MASTER']  # don't think this is currently exposed
            except:
                pass
            if not image_url:
                try:
                    image_url = links_dict[u'content_datastreams'][u'TIFF']  # should handle some old items
                except:
                    pass
            if not image_url:
                try:
                    jp2_image_url = links_dict[u'content_datastreams'][u'JP2']  # default modern case: if JP2 exists, master is MASTER
                    image_url = jp2_image_url.replace( u'/JP2/', u'/MASTER/' )
                except:
                    pass
            if not image_url:
                self.logger.info( u'in tasks.indexer.CustomIndexUpdater._set_image_urls__get_master_image_url(); odd case, links_dict is `%s`, jp2_url is `%s`' % (pprint.pformat(links_dict), jp2_url) )
                image_url = u''
        return image_url

    ## utils ##

    def __ensure_list_unicode_values( self, solr_dict, fields_to_check ):
        """ Returns solr_dict updated to ensure the values for the specified dictionary-fields_to_check
            are _always_ lists of unicode values.
            Called by _set_author_dates(), _set_author_description(), _set_author_names() """
        solr_dict_copy = solr_dict.copy()
        for key in fields_to_check:
            value = solr_dict[key]
            if type(value) == unicode:
              solr_dict_copy[key] = [ value ]
            elif type(value) == list:
                solr_dict_copy[key] = self.__ensure_list_unicode_values__handle_type_list( value )
            elif value == None:
              solr_dict_copy[key] = [ u'' ]
        return solr_dict_copy

    def __ensure_list_unicode_values__handle_type_list( self, list_value ):
        """ Returns updated list for inclusion as value of solr_dict entry.
            Called by __ensure_list_unicode_values() and _set_physical_descriptions() """
        new_list = []
        if len( list_value ) == 0:
          new_list.append( u'' )
        else:
          for entry in list_value:
            new_list.append( u'' ) if (entry == None) else new_list.append( entry )
        return new_list

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

def run_update_custom_index_entry( accession_number, data_dct, pid ):
    """ Preps and executes post for single entry.
        Called by CustomIndexUpdater.enqueue_index_jobs() """
    logger.debug( u'in tasks.indexer.run_update_custom_index_entry(); starting' )
    idxr = CustomIndexUpdater( logger )
    idxr.update_custom_index_entry( accession_number, data_dct, pid )
    logger.debug( u'in tasks.indexer.run_update_custom_index_entry(); done' )
    return

