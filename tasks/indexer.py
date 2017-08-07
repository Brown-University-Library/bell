""" Prepares data for updating and deleting custom-index records.
    Executes custom-index changes as per readme.md """

import datetime, json, logging, os, pprint, sys, time
import redis, requests, rq
#from mysolr import Solr

LOG_FILENAME = os.environ['BELL_LOG_FILENAME']
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG,
                    format='[%(asctime)s] %(levelname)s [%(module)s-%(funcName)s()::%(lineno)d] %(message)s',
                    datefmt='%d/%b/%Y %H:%M:%S',
                    filename=LOG_FILENAME)

queue_name = os.environ.get('BELL_QUEUE_NAME')
q = rq.Queue( queue_name, connection=redis.Redis() )


class SolrPidsLister:
    """ Prepares lists of custom-solr items to be created/updated. """

    def __init__( self, logger ):
        self.logger = logger
        self.SRC_ACC_NUM_TO_DATA_DCT_JSON_PATH = os.path.join('data', 'c__accession_number_to_data_dict.json')
        self.SRC_ACC_NUM_TO_PID_DCT_JSON_PATH = os.path.join('data', 'e__accession_number_to_pid_dict.json') 
        self.OUTPUT_PATH = os.path.join('data', 'j__solr_pids_list.json')

    def make_solr_pids_lst( self ):
        """ Loads up two source-data dicts and outputs an accession-number-to-data dict that includes pid-info.
            Called by runner as per readme.md """
        accession_number_to_data_dct = self.load_data_dct()
        accession_number_to_pid_dct = self.load_pid_dct()
        updated_accesson_number_to_data_dct_lst = self.add_pid( accession_number_to_data_dct, accession_number_to_pid_dct )
        self.output_lst( updated_accesson_number_to_data_dct_lst )
        return

    def load_data_dct( self ):
        """ Loads initial source data dct.
            Called by make_solr_pids_lst() """
        with open( self.SRC_ACC_NUM_TO_DATA_DCT_JSON_PATH ) as f:
            dct = json.loads( f.read() )
            accession_number_to_data_dct = dct['items']
        return accession_number_to_data_dct

    def load_pid_dct( self ):
        """ Loads source pid dct.
            Called by make_solr_pids_lst() """
        with open( self.SRC_ACC_NUM_TO_PID_DCT_JSON_PATH ) as f:
            dct = json.loads( f.read() )
            accession_number_to_pid_dct = dct['final_accession_pid_dict']
        return accession_number_to_pid_dct

    def add_pid( self, accession_number_to_data_dct, accession_number_to_pid_dct ):
        """ Adds pid to data-dct, returns list of accession-number-to-updated-data-dct.
            Called by make_solr_pids_lst() """
        updated_data_lst = []
        for (i, accession_number_key) in enumerate( sorted(accession_number_to_data_dct.keys()) ):
            if i + 1 > 70000:
                break
            print(accession_number_key)
            value_dct = accession_number_to_data_dct[accession_number_key]
            pid = accession_number_to_pid_dct[accession_number_key]
            value_dct['pid'] = pid
            entry = { accession_number_key: value_dct }
            updated_data_lst.append( entry )
        return updated_data_lst

    def output_lst( self, lst ):
        """ Saves json file.
            Called by make_solr_pids_list() """
        jsn = json.dumps( lst, indent=2, sort_keys=True )
        with open( self.OUTPUT_PATH, 'wt', encoding='utf8' ) as f:
            f.write( jsn )
        return

    # end class SolrPidsLister


class SolrDataBuilder:

    def __init__( self, logger ):
        self.logger = logger
        self.BDR_PUBLIC_ITEM_API_URL_ROOT = os.environ['BELL_TASKS_IMGS__PROD_API_ROOT_URL']
        self.REQUIRED_KEYS = [  # used by _validate_solr_dict()
            'accession_number_original',
            'author_birth_date',
            'author_date',
            'author_death_date',
            'author_description',
            'author_display',
            'author_names_first',
            'author_names_last',
            'author_names_middle',
            'image_height',
            'image_width',
            'jp2_image_url',
            'location_physical_location',
            'location_shelf_locator',
            'master_image_url',
            'note_provenance',
            'object_date',
            'object_depth',
            'object_height',
            'object_width',
            'origin_datecreated_end',
            'origin_datecreated_start',
            'physical_description_extent',
            'physical_description_material',
            'physical_description_technique',
            'pid',
            'title',
            ]

    def create_solr_data( self ):
        file_path = os.path.join('data', 'j__solr_pids_list.json')
        with open( file_path, 'rt', encoding='utf8' ) as f:
            accession_number_to_data_dct_lst = json.loads( f.read() )
        data_list = []
        for (i, accession_number_dict) in enumerate( accession_number_to_data_dct_lst ):  # entry: { accession_number: {data_key_a: data_value_a, etc} }
            if i + 1 > 70000:
                break
            ( accession_number, data_dct ) = list(accession_number_dict.items())[0]
            print('accession_number...'); print(accession_number)
            data_list.append(self.update_custom_index_entry( accession_number, data_dct ))
        self.output_lst( data_list )
        return

    def update_custom_index_entry( self, accession_number, data_dct ):
        """ Manages prep & post of update custom index entry.
            Called by runner. """
        metadata_solr_dict = self.build_metadata_only_solr_dict( data_dct )
        bdr_api_links_dict = self.grab_bdr_api_links_data( data_dct['pid'] )
        updated_solr_dict = self.add_image_metadata( metadata_solr_dict, bdr_api_links_dict )
        validity = self.validate_solr_dict( updated_solr_dict )
        if validity:
            return updated_solr_dict

    def build_metadata_only_solr_dict( self, data_dct ):
        """ Builds dict-to-index using just basic item-dict data and pid; image-check handled in separate function.
            Called by run_update_custom_index_entry() """
        solr_dict = {}
        solr_dict['accession_number_original'] = data_dct['calc_accession_id']
        solr_dict['pid'] = data_dct['pid']
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
        self.logger.debug( 'in tasks.indexer.CustomIndexUpdater.build_metadata_only_solr_dict(); solr_dict, `%s`' % pprint.pformat(solr_dict) )
        return solr_dict

    def grab_bdr_api_links_data( self, pid ):
        """ Grabs and returns link info from item-api json.
            The links dict is used by tasks.indexer to add image info to the solr-metadata if needed.
            Called by update_custom_index_entry() """
        url = '%s/%s/' % ( self.BDR_PUBLIC_ITEM_API_URL_ROOT, pid )
        self.logger.debug( 'url, `%s`' % url )
        r = requests.get( url )
        api_dict = r.json()
        links_dict = api_dict['links']
        self.logger.debug( 'links_dict, `%s`' % pprint.pformat(links_dict) )
        return links_dict

    def add_image_metadata( self, solr_dict, links_dict ):
        """ Adds image metadata to dict-to-index.
            Called by update_custom_index_entry() """
        solr_dict['jp2_image_url'] = self._set_image_urls__get_jp2_url( links_dict )
        solr_dict['master_image_url'] = self._set_image_urls__get_master_image_url( links_dict, solr_dict['jp2_image_url'] )
        self.logger.debug( 'final solr_dict, `%s`' % pprint.pformat(solr_dict) )
        return solr_dict

    def validate_solr_dict( self, solr_dict ):
        """ Returns True if checks pass; False otherwise.
            Checks that required keys are present.
            Checks that there are no None values.
            Checks that any list values are not empty.
            Checks that no members of a list value are of NoneType.
            Called by update_custom_index_entry() """
        try:
            for required_key in self.REQUIRED_KEYS:
                # self.logger.debug( 'in tasks.indexer.CustomIndexUpdater._validate_solr_dict(); required_key: %s' % required_key )
                assert required_key in solr_dict.keys(), Exception( 'ERROR; missing required key: %s' % required_key )
            for key,value in solr_dict.items():
              assert value != None, Exception( 'ERROR; value is none for key: %s' % key )
              if type(value) == list:
                assert len(value) > 0, Exception( 'ERROR: key "%s" has a value of "%s", which is type-list, which is empty.' % ( key, value ) )
                for element in value:
                  assert element != None, Exception( 'ERROR: key "%s" has a value "%s", which is type-list, which contains a None element' % ( key, value ) )
            self.logger.debug( 'is valid' )
            return True
        except Exception as e:
            self.logger.error( 'exception is: %s' % unicode(repr(e)) )
            return False

    def output_lst( self, lst ):
        """ Saves json file.
            Called by make_solr_pids_list() """
        jsn = json.dumps( lst, indent=2, sort_keys=True )
        filepath = os.path.join('data', 'k__data_for_solr.json')
        with open( filepath, 'wb' ) as f:
            f.write( jsn.encode('utf8') )
        return

    ## metadata-only helpers ##

    def _set_author_dates( self, original_dict, solr_dict ):
        """ Sets author dates.
            Called by build_metadata_only_solr_dict() """
        solr_dict['author_birth_date'] = original_dict['ARTISTS::artist_birth_year']
        solr_dict['author_death_date'] = original_dict['ARTISTS::artist_death_year']
        solr_dict['author_date'] = original_dict['ARTISTS::artist_lifetime']
        solr_dict = self.__ensure_list_unicode_values( solr_dict, ['author_birth_date', 'author_death_date', 'author_date'] )
        return solr_dict

    def _set_author_description( self, original_dict, solr_dict ):
        """ Sets author descriptions.
            Called by build_metadata_only_solr_dict() """
        solr_dict['author_description'] = original_dict['ARTISTS::calc_nationality']
        solr_dict = self.__ensure_list_unicode_values( solr_dict, ['author_description'] )
        return solr_dict

    def _set_author_names( self, original_dict, solr_dict ):
        """ Sets author names.
            Called by build_metadata_only_solr_dict() """
        solr_dict['author_names_first'] = original_dict['ARTISTS::artist_first_name']
        solr_dict['author_names_middle'] = original_dict['ARTISTS::artist_middle_name']
        solr_dict['author_names_last'] = original_dict['ARTISTS::artist_last_name']
        solr_dict['author_display'] = original_dict['ARTISTS::calc_artist_full_name']
        solr_dict = self.__ensure_list_unicode_values( solr_dict, ['author_names_first',
            'author_names_middle', 'author_names_last', 'author_display'] )
        return solr_dict

    def _set_height_width_depth( self, original_dict, solr_dict ):
        """ Sets item dimensions.
            Called by build_metadata_only_solr_dict() """
        target_keys = [ 'image_width', 'image_height', 'object_width', 'object_height', 'object_depth' ]
        for entry in target_keys:
            if original_dict[entry] == None:
                solr_dict[entry] = ''
            else:
                solr_dict[entry] = original_dict[entry]
        return solr_dict

    def _set_locations( self, original_dict, solr_dict ):
        """ Returns location_physical_location & location_shelf_locator.
            Called by build_metadata_only_solr_dict() """
        solr_dict['location_physical_location'] = 'Bell Art Gallery'
        solr_dict['location_shelf_locator'] = ''
        if original_dict['MEDIA::object_medium_name'] != None:
          solr_dict['location_shelf_locator'] = original_dict['MEDIA::object_medium_name']
        return solr_dict

    def _set_note_provenance( self, original_dict, solr_dict ):
        """ Updates note_provenance.
            Called by build_metadata_only_solr_dict() """
        solr_dict['note_provenance'] = ''
        if original_dict['credit_line'] != None:
          solr_dict['note_provenance'] = original_dict['credit_line']
        return solr_dict

    def _set_object_dates( self, original_dict, solr_dict ):
        """ Updates object_date, origin_datecreated_start, origin_datecreated_end.
            Called by build_metadata_only_solr_dict() """
        solr_dict['object_date'] = ''
        solr_dict['origin_datecreated_start'] = ''
        solr_dict['origin_datecreated_end'] = ''
        if original_dict['object_date'] != None:
            solr_dict['object_date'] = original_dict['object_date']
        if original_dict['object_year_start'] != None:
            solr_dict['origin_datecreated_start'] = original_dict['object_year_start']
        if original_dict['object_year_end'] != None:
            solr_dict['origin_datecreated_end'] = original_dict['object_year_end']
        return solr_dict

    def _set_physical_extent( self, original_dict, solr_dict ):
        """ Updates physical_description_extent, which is a display field based on the five dimension fields.
            Called by build_metadata_only_solr_dict() """
        ( height, width, depth ) = self.__set_physical_extent_prep( original_dict )
        solr_dict['physical_description_extent'] = [ '' ]
        if height and width and depth:
            solr_dict['physical_description_extent'] = [ '%s x %s x %s' % (height, width, depth) ]
        elif height and width:
            solr_dict['physical_description_extent'] = [ '%s x %s' % (height, width ) ]
        return solr_dict

    def  __set_physical_extent_prep( self, original_dict ):
        """ Sets initial height, width, and depth from possible image and object dimension fields.
            Called by _set_physical_extent() """
        temp_dict = {}
        for field in [ 'image_height', 'image_width', 'object_height', 'object_width', 'object_depth' ]:  # strip original-dict values
            temp_dict[field] = ''
            if original_dict[field]:
                temp_dict[field] = original_dict[field].strip()
        height = temp_dict['image_height'] if temp_dict['image_height'] else temp_dict['object_height']
        width = temp_dict['image_width'] if temp_dict['image_width'] else temp_dict['object_width']
        depth = temp_dict['object_depth']
        return ( height, width, depth )

    def _set_physical_descriptions( self, original_dict, solr_dict ):
        """ Updates physical_description_material, physical_description_technique.
            Called by build_metadata_only_solr_dict() """
        solr_dict['physical_description_material'] = [ '' ]
        solr_dict['physical_description_technique'] = [ '' ]
        if original_dict['object_medium'] != None:
            solr_dict['physical_description_material'] = [ original_dict['object_medium'] ]
        if original_dict['MEDIA_SUB::sub_media_name'] != None:
            if type( original_dict['MEDIA_SUB::sub_media_name'] ) == str:
                solr_dict['physical_description_technique'] = [ original_dict['MEDIA_SUB::sub_media_name'] ]
            elif type( original_dict['MEDIA_SUB::sub_media_name'] ) == list:
                cleaned_list = self.__ensure_list_unicode_values__handle_type_list( original_dict['MEDIA_SUB::sub_media_name'] )
                solr_dict['physical_description_technique'] = cleaned_list
        return solr_dict

    def _set_title( self, original_dict, solr_dict ):
        """ Updates title.
            Called by build_metadata_only_solr_dict() """
        solr_dict['title'] = ''
        if original_dict['object_title'] != None:
            solr_dict['title'] = original_dict['object_title']
        return solr_dict

    ## image helpers ##

    def _set_image_urls__get_jp2_url( self, links_dict ):
        """ Returns jp2 url or ''.
            Called by _set_image_urls() """
        try:
            image_url = links_dict['content_datastreams']['JP2']
        except:
            image_url = ''
        return image_url

    def _set_image_urls__get_master_image_url( self, links_dict, jp2_url ):
        """ Returns master image url or ''.
            Called by _set_image_urls() """
        image_url = None
        if jp2_url == '':
            image_url = ''
        else:
            #'MASTER' datastream is suppressed in the API, so we can't pull it out directly
            try:
                image_url = links_dict['content_datastreams']['TIFF']  # should handle some old items
            except:
                pass
            if not image_url:
                try:
                    jp2_image_url = links_dict['content_datastreams']['JP2']  # default modern case: if JP2 exists, master is MASTER
                    image_url = jp2_image_url.replace( '/JP2/', '/MASTER/' )
                except:
                    pass
            if not image_url:
                self.logger.info( 'odd case, links_dict is `%s`, jp2_url is `%s`' % (pprint.pformat(links_dict), jp2_url) )
                image_url = ''
        return image_url

    ## utils ##

    def __ensure_list_unicode_values( self, solr_dict, fields_to_check ):
        """ Returns solr_dict updated to ensure the values for the specified dictionary-fields_to_check
            are _always_ lists of unicode values.
            Called by _set_author_dates(), _set_author_description(), _set_author_names() """
        solr_dict_copy = solr_dict.copy()
        for key in fields_to_check:
            value = solr_dict[key]
            if type(value) == str:
              solr_dict_copy[key] = [ value ]
            elif type(value) == list:
                solr_dict_copy[key] = self.__ensure_list_unicode_values__handle_type_list( value )
            elif value == None:
              solr_dict_copy[key] = [ '' ]
        return solr_dict_copy

    def __ensure_list_unicode_values__handle_type_list( self, list_value ):
        """ Returns updated list for inclusion as value of solr_dict entry.
            Called by __ensure_list_unicode_values() and _set_physical_descriptions() """
        new_list = []
        if len( list_value ) == 0:
          new_list.append( '' )
        else:
          for entry in list_value:
            new_list.append( '' ) if (entry == None) else new_list.append( entry )
        return new_list


class CustomIndexUpdater:
    """ Handles updating custom solr index for additions and updates.
        TODO: move the metadata-helpers into a different data-prep class. """

    def __init__( self, logger=None ):
        self.logger = logger
        self.FULL_DATA_DCTS_JSON_PATH = os.environ['BELL_TASKS_IDXR__FULL_DATA_DCTS_JSON_PATH']
        self.BDR_PUBLIC_ITEM_API_URL_ROOT = os.environ['BELL_TASKS_IDXR__BDR_PUBLIC_ITEM_API_URL_ROOT']
        self.CUSTOM_INDEX_SOLR_URL_ROOT = os.environ['BELL_TASKS_IDXR__CUSTOM_INDEX_SOLR_URL_ROOT']
        self.TRACKER_JSON_PATH = os.environ['BELL_TASKS_IDXR__CUSTOM_INDEX_UPDATER_TRACKER_JSON_PATH']
        self.REQUIRED_KEYS = [  # used by _validate_solr_dict()
            'accession_number_original',
            'author_birth_date',
            'author_date',
            'author_death_date',
            'author_description',
            'author_display',
            'author_names_first',
            'author_names_last',
            'author_names_middle',
            'image_height',
            'image_width',
            'jp2_image_url',
            'location_physical_location',
            'location_shelf_locator',
            'master_image_url',
            'note_provenance',
            'object_date',
            'object_depth',
            'object_height',
            'object_width',
            'origin_datecreated_end',
            'origin_datecreated_start',
            'physical_description_extent',
            'physical_description_material',
            'physical_description_technique',
            'pid',
            'title',
            ]

    def enqueue_index_jobs( self ):
        """ Loads up json list of dicts and hits a prep&post job.
            Called by runner, triggered manually as per readme.md """
        with open( self.FULL_DATA_DCTS_JSON_PATH ) as f:
            accession_number_to_data_dct_lst = json.loads( f.read() )
        for (i, entry) in enumerate( accession_number_to_data_dct_lst ):  # entry: { accession_number: {data_key_a: data_value_a, etc} }
            if i + 1 > 70000:
                break
            ( accession_number, data_dct ) = entry.items()[0]
            print('accession_number...'); print(accession_number)
            q.enqueue_call(
              func='tasks.indexer.run_update_custom_index_entry',
              kwargs={ 'accession_number': accession_number, 'data_dct': data_dct, 'pid': data_dct['pid'] },
              timeout=600 )
        return

    def update_custom_index_entry( self, accession_number, data_dct, pid ):
        """ Manages prep & post of update custom index entry.
            Called by runner. """
        time.sleep( .5 )
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
        solr_dict['accession_number_original'] = data_dct['calc_accession_id']
        solr_dict['pid'] = pid
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
        self.logger.debug( 'in tasks.indexer.CustomIndexUpdater.build_metadata_only_solr_dict(); solr_dict, `%s`' % pprint.pformat(solr_dict) )
        return solr_dict

    def grab_bdr_api_links_data( self, pid ):
        """ Grabs and returns link info from item-api json.
            The links dict is used by tasks.indexer to add image info to the solr-metadata if needed.
            Called by update_custom_index_entry() """
        url = '%s/%s/' % ( self.BDR_PUBLIC_ITEM_API_URL_ROOT, pid )
        self.logger.debug( 'in tasks.indexer.CustomIndexUpdater.grab_bdr_api_links_data(); url, `%s`' % url )
        r = requests.get( url )
        api_dict = r.json()
        links_dict = api_dict['links']
        self.logger.debug( 'in tasks.indexer.CustomIndexUpdater.grab_bdr_api_links_data(); links_dict, `%s`' % pprint.pformat(links_dict) )
        return links_dict

    def add_image_metadata( self, solr_dict, links_dict ):
        """ Adds image metadata to dict-to-index.
            Called by update_custom_index_entry() """
        solr_dict['jp2_image_url'] = self._set_image_urls__get_jp2_url( links_dict )
        solr_dict['master_image_url'] = self._set_image_urls__get_master_image_url( links_dict, solr_dict['jp2_image_url'] )
        self.logger.debug( 'in tasks.indexer.CustomIndexUpdater.add_image_metadata(); final solr_dict, `%s`' % pprint.pformat(solr_dict) )
        return solr_dict

    def validate_solr_dict( self, solr_dict ):
        """ Returns True if checks pass; False otherwise.
            Checks that required keys are present.
            Checks that there are no None values.
            Checks that any list values are not empty.
            Checks that no members of a list value are of NoneType.
            Called by update_custom_index_entry() """
        try:
            for required_key in self.REQUIRED_KEYS:
                # self.logger.debug( 'in tasks.indexer.CustomIndexUpdater._validate_solr_dict(); required_key: %s' % required_key )
                assert required_key in solr_dict.keys(), Exception( 'ERROR; missing required key: %s' % required_key )
            for key,value in solr_dict.items():
              assert value != None, Exception( 'ERROR; value is none for key: %s' % key )
              if type(value) == list:
                assert len(value) > 0, Exception( 'ERROR: key "%s" has a value of "%s", which is type-list, which is empty.' % ( key, value ) )
                for element in value:
                  assert element != None, Exception( 'ERROR: key "%s" has a value "%s", which is type-list, which contains a None element' % ( key, value ) )
            self.logger.debug( 'in tasks.indexer.CustomIndexUpdater.validate_solr_dict(); is valid' )
            return True
        except Exception as e:
            self.logger.error( 'in tasks.indexer.CustomIndexUpdater.validate_solr_dict(); exception is: %s' % unicode(repr(e)) )
            return False

    def post_to_solr( self, solr_dict ):
        """ Posts solr_dict to solr.
            Called by update_custom_index_entry() """
        solr = Solr( self.CUSTOM_INDEX_SOLR_URL_ROOT )
        response = solr.update( [solr_dict], 'xml', commit=True )  # 'xml' param converts default json to xml for post; required for our old version of solr
        response_status = response.status
        self.logger.info( 'in tasks.indexer.CustomIndexUpdater.post_to_solr() [for custom-solr]; accession_number, %s; response_status, %s' % (solr_dict['accession_number_original'], response_status) )
        if not response_status == 200:
            raise Exception( 'custom-solr post problem logged' )
        return response_status

    def update_tracker( self, accession_number, post_result ):
        """ Stores result to json file.
            Not thread-safe - redis would be good for this.
            Called by update_custom_index_entry() """
        try:
            with open( self.TRACKER_JSON_PATH ) as f:
                dct = json.loads( f.read() )
        except:
            dct = {}
        dct[accession_number] = { 'datetime': unicode(datetime.datetime.now()), 'post_result': post_result }
        jsn = json.dumps( dct, indent=2, sort_keys=True )
        with open( self.TRACKER_JSON_PATH, 'w' ) as f:
            f.write( jsn )
        return

    ## metadata-only helpers ##

    def _set_author_dates( self, original_dict, solr_dict ):
        """ Sets author dates.
            Called by build_metadata_only_solr_dict() """
        solr_dict['author_birth_date'] = original_dict['ARTISTS::artist_birth_year']
        solr_dict['author_death_date'] = original_dict['ARTISTS::artist_death_year']
        solr_dict['author_date'] = original_dict['ARTISTS::artist_lifetime']
        solr_dict = self.__ensure_list_unicode_values( solr_dict, ['author_birth_date', 'author_death_date', 'author_date'] )
        return solr_dict

    def _set_author_description( self, original_dict, solr_dict ):
        """ Sets author descriptions.
            Called by build_metadata_only_solr_dict() """
        solr_dict['author_description'] = original_dict['ARTISTS::calc_nationality']
        solr_dict = self.__ensure_list_unicode_values( solr_dict, ['author_description'] )
        return solr_dict

    def _set_author_names( self, original_dict, solr_dict ):
        """ Sets author names.
            Called by build_metadata_only_solr_dict() """
        solr_dict['author_names_first'] = original_dict['ARTISTS::artist_first_name']
        solr_dict['author_names_middle'] = original_dict['ARTISTS::artist_middle_name']
        solr_dict['author_names_last'] = original_dict['ARTISTS::artist_last_name']
        solr_dict['author_display'] = original_dict['ARTISTS::calc_artist_full_name']
        solr_dict = self.__ensure_list_unicode_values( solr_dict, ['author_names_first',
            'author_names_middle', 'author_names_last', 'author_display'] )
        return solr_dict

    def _set_height_width_depth( self, original_dict, solr_dict ):
        """ Sets item dimensions.
            Called by build_metadata_only_solr_dict() """
        target_keys = [ 'image_width', 'image_height', 'object_width', 'object_height', 'object_depth' ]
        for entry in target_keys:
            if original_dict[entry] == None:
                solr_dict[entry] = ''
            else:
                solr_dict[entry] = original_dict[entry]
        return solr_dict

    def _set_locations( self, original_dict, solr_dict ):
        """ Returns location_physical_location & location_shelf_locator.
            Called by build_metadata_only_solr_dict() """
        solr_dict['location_physical_location'] = 'Bell Art Gallery'
        solr_dict['location_shelf_locator'] = ''
        if original_dict['MEDIA::object_medium_name'] != None:
          solr_dict['location_shelf_locator'] = original_dict['MEDIA::object_medium_name']
        return solr_dict

    def _set_note_provenance( self, original_dict, solr_dict ):
        """ Updates note_provenance.
            Called by build_metadata_only_solr_dict() """
        solr_dict['note_provenance'] = ''
        if original_dict['credit_line'] != None:
          solr_dict['note_provenance'] = original_dict['credit_line']
        return solr_dict

    def _set_object_dates( self, original_dict, solr_dict ):
        """ Updates object_date, origin_datecreated_start, origin_datecreated_end.
            Called by build_metadata_only_solr_dict() """
        solr_dict['object_date'] = ''
        solr_dict['origin_datecreated_start'] = ''
        solr_dict['origin_datecreated_end'] = ''
        if original_dict['object_date'] != None:
            solr_dict['object_date'] = original_dict['object_date']
        if original_dict['object_year_start'] != None:
            solr_dict['origin_datecreated_start'] = original_dict['object_year_start']
        if original_dict['object_year_end'] != None:
            solr_dict['origin_datecreated_end'] = original_dict['object_year_end']
        return solr_dict

    def _set_physical_extent( self, original_dict, solr_dict ):
        """ Updates physical_description_extent, which is a display field based on the five dimension fields.
            Called by build_metadata_only_solr_dict() """
        ( height, width, depth ) = self.__set_physical_extent_prep( original_dict )
        solr_dict['physical_description_extent'] = [ '' ]
        if height and width and depth:
            solr_dict['physical_description_extent'] = [ '%s x %s x %s' % (height, width, depth) ]
        elif height and width:
            solr_dict['physical_description_extent'] = [ '%s x %s' % (height, width ) ]
        return solr_dict

    def  __set_physical_extent_prep( self, original_dict ):
        """ Sets initial height, width, and depth from possible image and object dimension fields.
            Called by _set_physical_extent() """
        temp_dict = {}
        for field in [ 'image_height', 'image_width', 'object_height', 'object_width', 'object_depth' ]:  # strip original-dict values
            temp_dict[field] = ''
            if original_dict[field]:
                temp_dict[field] = original_dict[field].strip()
        height = temp_dict['image_height'] if temp_dict['image_height'] else temp_dict['object_height']
        width = temp_dict['image_width'] if temp_dict['image_width'] else temp_dict['object_width']
        depth = temp_dict['object_depth']
        return ( height, width, depth )

    def _set_physical_descriptions( self, original_dict, solr_dict ):
        """ Updates physical_description_material, physical_description_technique.
            Called by build_metadata_only_solr_dict() """
        solr_dict['physical_description_material'] = [ '' ]
        solr_dict['physical_description_technique'] = [ '' ]
        if original_dict['object_medium'] != None:
            solr_dict['physical_description_material'] = [ original_dict['object_medium'] ]
        if original_dict['MEDIA_SUB::sub_media_name'] != None:
            if type( original_dict['MEDIA_SUB::sub_media_name'] ) == unicode:
                solr_dict['physical_description_technique'] = [ original_dict['MEDIA_SUB::sub_media_name'] ]
            elif type( original_dict['MEDIA_SUB::sub_media_name'] ) == list:
                cleaned_list = self.__ensure_list_unicode_values__handle_type_list( original_dict['MEDIA_SUB::sub_media_name'] )
                solr_dict['physical_description_technique'] = cleaned_list
        return solr_dict

    def _set_title( self, original_dict, solr_dict ):
        """ Updates title.
            Called by build_metadata_only_solr_dict() """
        solr_dict['title'] = ''
        if original_dict['object_title'] != None:
            solr_dict['title'] = original_dict['object_title']
        return solr_dict

    ## image helpers ##

    def _set_image_urls__get_jp2_url( self, links_dict ):
        """ Returns jp2 url or ''.
            Called by _set_image_urls() """
        try:
            image_url = links_dict['content_datastreams']['JP2']
        except:
            image_url = ''
        return image_url

    def _set_image_urls__get_master_image_url( self, links_dict, jp2_url ):
        """ Returns master image url or ''.
            Called by _set_image_urls() """
        image_url = None
        if jp2_url == '':
            image_url = ''
        else:
            try:
                image_url = links_dict['content_datastreams']['MASTER']  # don't think this is currently exposed
            except:
                pass
            if not image_url:
                try:
                    image_url = links_dict['content_datastreams']['TIFF']  # should handle some old items
                except:
                    pass
            if not image_url:
                try:
                    jp2_image_url = links_dict['content_datastreams']['JP2']  # default modern case: if JP2 exists, master is MASTER
                    image_url = jp2_image_url.replace( '/JP2/', '/MASTER/' )
                except:
                    pass
            if not image_url:
                self.logger.info( 'in tasks.indexer.CustomIndexUpdater._set_image_urls__get_master_image_url(); odd case, links_dict is `%s`, jp2_url is `%s`' % (pprint.pformat(links_dict), jp2_url) )
                image_url = ''
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
              solr_dict_copy[key] = [ '' ]
        return solr_dict_copy

    def __ensure_list_unicode_values__handle_type_list( self, list_value ):
        """ Returns updated list for inclusion as value of solr_dict entry.
            Called by __ensure_list_unicode_values() and _set_physical_descriptions() """
        new_list = []
        if len( list_value ) == 0:
          new_list.append( '' )
        else:
          for entry in list_value:
            new_list.append( '' ) if (entry == None) else new_list.append( entry )
        return new_list

    # end class CustomIndexUpdater


class CustomIndexDeleter( object ):
    """ Handles removing enries from custom solr index.
        Data-source: custom_index_delete_pids.json
        Outputs to: solr_pids_deleted_tracker.json """

    def __init__( self, logger=None ):
        self.logger = logger
        self.PIDS_TO_DELETE_SOURCE_DATA_JSON_PATH = unicode( os.environ['BELL_TASKS_IDXR__CUSTOM_IDX_DELETES_PIDS_JSON_PATH'] )
        self.CUSTOM_INDEX_SOLR_URL_ROOT = unicode( os.environ['BELL_TASKS_IDXR__CUSTOM_INDEX_SOLR_URL_ROOT'] )
        self.TRACKER_JSON_PATH = unicode( os.environ['BELL_TASKS_IDXR__CUSTOM_INDEX_DELETER_TRACKER_JSON_PATH'] )

    def delete_target_custom_solr_pids( self ):
        ## load pids to be deleted
        with open( self.PIDS_TO_DELETE_SOURCE_DATA_JSON_PATH ) as f:
            deletion_pid_lst = json.loads( f.read() )
        ## run deletion loop, tracking along way
        for pid in deletion_pid_lst:
            solr = Solr( self.CUSTOM_INDEX_SOLR_URL_ROOT )
            response = solr.delete_by_query( 'pid:"%s"' % pid, commit=True )
            response_status = response.status
            self.update_tracker( pid, response_status )
            if not response_status == 200:
                logger.error( 'custom-solr delete problem-response for pid `{pid}`: ```{response}```'.format(pid=pid, resp=response_status) )
        return

    def update_tracker( self, pid, delete_result ):
        """ Stores result to json file.
            Not thread-safe - redis would be good for this.
            Called by delete_target_custom_solr_pids() """
        try:
            with open( self.TRACKER_JSON_PATH ) as f:
                dct = json.loads( f.read() )
        except:
            dct = {}
        dct[pid] = { 'datetime': unicode(datetime.datetime.now()), 'delete_result': delete_result }
        jsn = json.dumps( dct, indent=2, sort_keys=True )
        with open( self.TRACKER_JSON_PATH, 'w' ) as f:
            f.write( jsn )
        return

    # end class CustomIndexUpdater


## runners ##


def run_make_solr_pids_list():
    """ Saves custom-index pids to be created/updated to `j__solr_pids_list.json`.
        Called manually per readme.md """
    logger.debug( 'starting' )
    solr_lstr = SolrPidsLister( logger )
    solr_lstr.make_solr_pids_lst()
    logger.debug( 'done' )
    return

def run_enqueue_index_jobs():
    """ Enqueues update-custom-solr-index-entry jobs.
        Called manually per readme.md """
    logger.debug( 'in tasks.indexer.run_enqueue_index_jobs(); starting' )
    idxr = CustomIndexUpdater( logger )
    idxr.enqueue_index_jobs()
    logger.debug( 'in tasks.indexer.run_enqueue_index_jobs(); done' )
    return

def run_create_solr_data():
    sdb = SolrDataBuilder( logger )
    sdb.create_solr_data()
    return

def run_update_custom_index_entry( accession_number, data_dct, pid ):
    """ Preps and executes post for single entry.
        Called by CustomIndexUpdater.enqueue_index_jobs() """
    logger.debug( 'in tasks.indexer.run_update_custom_index_entry(); starting' )
    idxr = CustomIndexUpdater( logger )
    idxr.update_custom_index_entry( accession_number, data_dct, pid )
    logger.debug( 'in tasks.indexer.run_update_custom_index_entry(); done' )
    return

def run_solr_deletions():
    """ Deletes pids marked for deletion.
        Called manually per readme.md """
    logger.debug( 'in tasks.indexer.runs_solr_deletions(); starting' )
    deleter = CustomIndexDeleter( logger )
    deleter.delete_target_custom_solr_pids()
    logger.debug( 'in tasks.indexer.runs_solr_deletions(); done' )
    return
