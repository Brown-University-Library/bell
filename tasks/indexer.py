# -*- coding: utf-8 -*-

import os, pprint, sys, time
import requests
from bell_code import bell_logger
from mysolr import Solr
from bell_code.tasks import task_manager

""" Handles custom solr indexing.
    Typically auto-called after ingestion.
    TODO: Now that this has been made into a class, update previous calling code. """


class Indexer( object ):
    """ Handles custom solr index. """

    def __init__( self, logger=None ):
        self.logger = logger
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

    ## main functions ##

    def build_metadata_only_solr_dict( self, pid, original_dict ):
        """ Builds dict-to-index using just basic item-dict data and pid; image-check handled in separate function.
            Called by utils.update_custom_index.py """
        solr_dict = {}
        solr_dict[u'accession_number_original'] = original_dict[u'calc_accession_id']
        solr_dict[u'pid'] = pid
        solr_dict = self._set_author_dates( original_dict, solr_dict )
        solr_dict = self._set_author_description( original_dict, solr_dict )
        solr_dict = self._set_author_names( original_dict, solr_dict )
        solr_dict = self._set_height_width_depth( original_dict, solr_dict )
        solr_dict = self._set_locations( original_dict, solr_dict )
        solr_dict = self._set_note_provenance( original_dict, solr_dict )
        solr_dict = self._set_object_dates( original_dict, solr_dict )
        solr_dict = self._set_physical_extent( original_dict, solr_dict )
        solr_dict = self._set_physical_descriptions( original_dict, solr_dict )
        solr_dict = self._set_title( original_dict, solr_dict )
        self.logger.debug( u'in indexer.build_metadata_only_solr_dict(); solr_dict, `%s`' % pprint.pformat(solr_dict) )
        return solr_dict

    def add_image_metadata( self, solr_dict, links_dict ):
        """ Adds image metadata to dict-to-index. """
        solr_dict[u'jp2_image_url'] = self._set_image_urls__get_jp2_url( links_dict )
        solr_dict[u'master_image_url'] = self._set_image_urls__get_master_image_url( links_dict, solr_dict[u'jp2_image_url'] )
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
                # self.logger.debug( u'in tasks.indexer._validate_solr_dict(); required_key: %s' % required_key )
                assert required_key in solr_dict.keys(), Exception( u'ERROR; missing required key: %s' % required_key )
            for key,value in solr_dict.items():
              assert value != None, Exception( u'ERROR; value is none for key: %s' % key )
              if type(value) == list:
                assert len(value) > 0, Exception( u'ERROR: key "%s" has a value of "%s", which is type-list, which is empty.' % ( key, value ) )
                for element in value:
                  assert element != None, Exception( u'ERROR: key "%s" has a value "%s", which is type-list, which contains a None element' % ( key, value ) )
            return True
        except Exception as e:
            self.logger.error( u'in tasks.indexer._validate_solr_dict(); exception is: %s' % unicode(repr(e)) )
            return False

    def post_to_solr( self, solr_dict ):
        """ Posts solr_dict to solr. """
        SOLR_ROOT_URL = ( os.environ.get(u'BELL_I_SOLR_ROOT') )
        solr = Solr( SOLR_ROOT_URL )
        response = solr.update( [solr_dict], u'xml', commit=True )  # 'xml' param converts default json to xml for post; required for our old version of solr
        response_status = response.status
        self.logger.info( u'in tasks.indexer.post_to_solr() [for custom-solr]; accession_number, %s; response_status, %s' % (solr_dict[u'accession_number_original'], response_status) )
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
                self.logger.info( u'in tasks.indexer._set_image_urls__get_master_image_url(); odd case, links_dict is `%s`, jp2_url is `%s`' % (pprint.pformat(links_dict), jp2_url) )
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

    # end class Indexer()








# # -*- coding: utf-8 -*-

# import os, pprint, sys, time
# import requests
# from bell_code import bell_logger
# from mysolr import Solr
# from bell_code.tasks import task_manager

# """ Handles custom solr indexing.
#     Typically auto-called after ingestion.
#     TODO: make this into a class so functions can be called with or without queueing. """


# REQUIRED_KEYS = [  # used by _validate_solr_dict()
#     u'accession_number_original',
#     u'author_birth_date',
#     u'author_date',
#     u'author_death_date',
#     u'author_description',
#     u'author_display',
#     u'author_names_first',
#     u'author_names_last',
#     u'author_names_middle',
#     u'image_height',
#     u'image_width',
#     u'jp2_image_url',
#     u'location_physical_location',
#     u'location_shelf_locator',
#     u'master_image_url',
#     # u'mods',  # no longer needed
#     u'note_provenance',
#     u'object_date',
#     u'object_depth',
#     u'object_height',
#     u'object_width',
#     u'origin_datecreated_end',
#     u'origin_datecreated_start',
#     u'physical_description_extent',
#     u'physical_description_material',
#     u'physical_description_technique',
#     u'pid',
#     u'title',
#     ]


# def build_metadata_only_solr_dict( data ):
#     """ Builds dict-to-index using just basic item-dict data and pid; no need to check image info.
#         Called after fedora_metadata_only_builder.run__create_fedora_metadata_object() task. """
#     assert sorted( data.keys() ) == [ u'item_data', u'pid' ], Exception( u'- in indexer.build_metadata_only_solr_dict(); unexpected data.keys(): %s' % sorted(data.keys()) )
#     logger = bell_logger.setup_logger()
#     original_dict = data[u'item_data']
#     solr_dict = {}
#     # solr_dict = _set_accession_number_original( original_dict, solr_dict )
#     solr_dict[u'accession_number_original'] = original_dict[u'calc_accession_id']
#     solr_dict[u'pid'] = data[u'pid']
#     solr_dict = _set_author_dates( original_dict, solr_dict )
#     solr_dict = _set_author_description( original_dict, solr_dict )
#     solr_dict = _set_author_names( original_dict, solr_dict )
#     solr_dict = _set_height_width_depth( original_dict, solr_dict )
#     solr_dict = _set_image_urls( solr_dict, pid=None, flag=u'metadata_only' )
#     solr_dict = _set_locations( original_dict, solr_dict )
#     solr_dict = _set_note_provenance( original_dict, solr_dict )
#     solr_dict = _set_object_dates( original_dict, solr_dict )
#     solr_dict = _set_physical_extent( original_dict, solr_dict )
#     solr_dict = _set_physical_descriptions( original_dict, solr_dict )
#     solr_dict = _set_title( original_dict, solr_dict )
#     all_good = _validate_solr_dict( solr_dict, logger )
#     logger.info( u'in indexer.build_metadata_only_solr_dict(); all_good_flag is %s; solr_dict is %s' % (all_good, solr_dict) )
#     if all_good:
#         task_manager.determine_next_task(
#             unicode(sys._getframe().f_code.co_name),
#             data={ u'solr_dict': solr_dict },
#             logger=logger
#             )
#     else:
#         raise Exception( u'problem preparing solr_dict, check logs' )  # should move job to failed queue
#     return solr_dict  # returned value only for testing


# def build_metadata_and_image_solr_dict( data ):
#     """ Builds dict-to-index using basic item-dict data and pid.
#         Called after fedora_metadata_updater_and_image_builder.run__update_existing_metadata_and_create_image() task. """
#     assert sorted( data.keys() ) == [ u'item_data', u'pid' ], Exception( u'- in indexer.build_metadata_only_solr_dict(); unexpected data.keys(): %s' % sorted(data.keys()) )
#     logger = bell_logger.setup_logger()
#     time.sleep( 10 )  # gives solr index time to be updated so image url can be grabbed
#     original_dict = data[u'item_data']
#     solr_dict = {}
#     # solr_dict = _set_accession_number_original( original_dict, solr_dict )
#     solr_dict[u'accession_number_original'] = original_dict[u'calc_accession_id']
#     solr_dict[u'pid'] = data[u'pid']
#     solr_dict = _set_author_dates( original_dict, solr_dict )
#     solr_dict = _set_author_description( original_dict, solr_dict )
#     solr_dict = _set_author_names( original_dict, solr_dict )
#     solr_dict = _set_height_width_depth( original_dict, solr_dict )
#     solr_dict = _set_image_urls( solr_dict, pid=data[u'pid'], flag=None, logger=logger )
#     solr_dict = _set_locations( original_dict, solr_dict )
#     solr_dict = _set_note_provenance( original_dict, solr_dict )
#     solr_dict = _set_object_dates( original_dict, solr_dict )
#     solr_dict = _set_physical_extent( original_dict, solr_dict )
#     solr_dict = _set_physical_descriptions( original_dict, solr_dict )
#     solr_dict = _set_title( original_dict, solr_dict )
#     all_good = _validate_solr_dict( solr_dict, logger )
#     logger.info( u'in indexer.build_metadata_and_image_solr_dict(); all_good_flag is %s; solr_dict is %s' % (all_good, solr_dict) )
#     if all_good:
#         task_manager.determine_next_task(
#             unicode(sys._getframe().f_code.co_name),
#             data={ u'solr_dict': solr_dict },
#             logger=logger
#             )
#     else:
#         raise Exception( u'problem preparing solr_dict, check logs' )  # should move job to failed queue
#     return solr_dict  # returned value only for testing


# def post_to_solr( data ):
#     """ Posts solr_dict to solr. """
#     (SOLR_ROOT_URL, solr_dict, logger) = ( os.environ.get(u'BELL_I_SOLR_ROOT'), data[u'solr_dict'], bell_logger.setup_logger() )  # setup
#     solr = Solr( SOLR_ROOT_URL )
#     response = solr.update( [solr_dict], u'xml', commit=True )  # 'xml' param converts json to xml for post; required for our old version of solr
#     response_status = response.status
#     logger.info( u'in indexer.post_to_solr() [for custom-solr]; accession_number, %s; response_status, %s' % (solr_dict[u'accession_number_original'], response_status) )
#     if not response_status == 200:
#         raise Exception( u'custom-solr post problem logged' )


# def _set_author_dates( original_dict, solr_dict ):
#     """ Sets author dates.
#         Called by build_metadata_only_solr_dict() """
#     solr_dict[u'author_birth_date'] = original_dict[u'ARTISTS::artist_birth_year']
#     solr_dict[u'author_death_date'] = original_dict[u'ARTISTS::artist_death_year']
#     solr_dict[u'author_date'] = original_dict[u'ARTISTS::artist_lifetime']
#     solr_dict = __ensure_list_unicode_values( solr_dict, [u'author_birth_date', u'author_death_date', u'author_date'] )
#     return solr_dict


# def _set_author_description( original_dict, solr_dict ):
#     """ Sets author descriptions.
#         Called by build_metadata_only_solr_dict() """
#     solr_dict[u'author_description'] = original_dict[u'ARTISTS::calc_nationality']
#     solr_dict = __ensure_list_unicode_values( solr_dict, [u'author_description'] )
#     return solr_dict


# def _set_author_names( original_dict, solr_dict ):
#     """ Sets author names.
#         Called by build_metadata_only_solr_dict() """
#     solr_dict[u'author_names_first'] = original_dict[u'ARTISTS::artist_first_name']
#     solr_dict[u'author_names_middle'] = original_dict[u'ARTISTS::artist_middle_name']
#     solr_dict[u'author_names_last'] = original_dict[u'ARTISTS::artist_last_name']
#     solr_dict[u'author_display'] = original_dict[u'ARTISTS::calc_artist_full_name']
#     solr_dict = __ensure_list_unicode_values( solr_dict, [u'author_names_first',
#         u'author_names_middle', u'author_names_last', u'author_display'] )
#     return solr_dict


# def _set_height_width_depth( original_dict, solr_dict ):
#     """ Sets item dimensions.
#         Called by build_metadata_only_solr_dict() """
#     target_keys = [ u'image_width', u'image_height', u'object_width', u'object_height', u'object_depth' ]
#     for entry in target_keys:
#         if original_dict[entry] == None:
#             solr_dict[entry] = u''
#         else:
#             solr_dict[entry] = original_dict[entry]
#     return solr_dict


# def _set_image_urls( solr_dict, pid=None, flag=None, logger=None ):
#     """  Sets jp2 and master image-url info.
#         Called by build_metadata_only_solr_dict() """
#     solr_dict[u'jp2_image_url'] = u''
#     solr_dict[u'master_image_url'] = u''
#     if flag == u'metadata_only':
#         pass
#     else:
#         logger.debug( u'in indexer._set_image_urls() [for custom-solr]; pid, %s; starting...' % pid )
#         assert pid != None
#         item_api_dict = _set_image_urls__get_item_api_data( pid, logger )
#         logger.debug( u'in indexer._set_image_urls() [for custom-solr]; pid, %s; item_api_dict, %s' % (pid, pprint.pformat(item_api_dict)) )
#         if item_api_dict != None:
#             solr_dict[u'jp2_image_url'] = _set_image_urls__get_jp2_url( item_api_dict )
#             solr_dict[u'master_image_url'] = _set_image_urls__get_master_image_url( item_api_dict )
#     return solr_dict


# def _set_image_urls__get_item_api_data( pid, logger ):
#     """ Returns repo public item-api json, or None.
#         Called by _set_image_urls() """
#     for i in range( 5 ):
#       try:
#         url = u'https://repository.library.brown.edu/api/pub/items/%s/' % pid
#         r = requests.get( url, verify=False )
#         logger.debug( u'in indexer._set_image_urls__get_item_api_data(); r.text, %s' % r.text )
#         jdict = r.json()
#         return jdict
#       except Exception as e:
#         logger.error( u'in indexer._set_image_urls__get_item_api_data(); exception, %s' % unicode(repr(e)) )
#         time.sleep( 2 )
#     return None


# def _set_image_urls__get_jp2_url( item_api_dict ):
#     """ Returns jp2 url or u''.
#         Called by _set_image_urls() """
#     try:
#         image_url = item_api_dict[u'links'][u'content_datastreams'][u'JP2']
#     except:
#         image_url = u''
#     return image_url


# def _set_image_urls__get_master_image_url( item_api_dict ):
#     """ Returns master image url or u''.
#         Called by _set_image_urls() """
#     try:
#         temp_image_url = item_api_dict[u'links'][u'content_datastreams'][u'JP2']
#         image_url = temp_image_url.replace( u'/JP2/', u'/MASTER/' )
#     except:
#         image_url = u''
#     return image_url


# def _set_locations( original_dict, solr_dict ):
#     """ Returns location_physical_location & location_shelf_locator.
#         Called by build_metadata_only_solr_dict() """
#     solr_dict[u'location_physical_location'] = u'Bell Art Gallery'
#     solr_dict[u'location_shelf_locator'] = u''
#     if original_dict[u'MEDIA::object_medium_name'] != None:
#       solr_dict[u'location_shelf_locator'] = original_dict[u'MEDIA::object_medium_name']
#     return solr_dict


# def _set_note_provenance( original_dict, solr_dict ):
#     """ Updates note_provenance.
#         Called by build_metadata_only_solr_dict() """
#     solr_dict[u'note_provenance'] = u''
#     if original_dict[u'credit_line'] != None:
#       solr_dict[u'note_provenance'] = original_dict[u'credit_line']
#     return solr_dict


# def _set_object_dates( original_dict, solr_dict ):
#     """ Updates object_date, origin_datecreated_start, origin_datecreated_end.
#         Called by build_metadata_only_solr_dict() """
#     solr_dict[u'object_date'] = u''
#     solr_dict[u'origin_datecreated_start'] = u''
#     solr_dict[u'origin_datecreated_end'] = u''
#     if original_dict[u'object_date'] != None:
#         solr_dict[u'object_date'] = original_dict[u'object_date']
#     if original_dict[u'object_year_start'] != None:
#         solr_dict[u'origin_datecreated_start'] = original_dict[u'object_year_start']
#     if original_dict[u'object_year_end'] != None:
#         solr_dict[u'origin_datecreated_end'] = original_dict[u'object_year_end']
#     return solr_dict


# def _set_physical_extent( original_dict, solr_dict ):
#     """ Updates physical_description_extent, which is a display field based on the five dimension fields.
#         Called by build_metadata_only_solr_dict() """
#     ( height, width, depth ) = _set_physical_extent_prep( original_dict )
#     solr_dict[u'physical_description_extent'] = [ u'' ]
#     if height and width and depth:
#         solr_dict[u'physical_description_extent'] = [ u'%s x %s x %s' % (height, width, depth) ]
#     elif height and width:
#         solr_dict[u'physical_description_extent'] = [ u'%s x %s' % (height, width ) ]
#     return solr_dict


# def  _set_physical_extent_prep( original_dict ):
#     """ Sets initial height, width, and depth from possible image and object dimension fields.
#         Called by _set_physical_extent() """
#     temp_dict = {}
#     for field in [ u'image_height', u'image_width', u'object_height', u'object_width', u'object_depth' ]:  # strip original-dict values
#         temp_dict[field] = u''
#         if original_dict[field]:
#             temp_dict[field] = original_dict[field].strip()
#     height = temp_dict[u'image_height'] if temp_dict[u'image_height'] else temp_dict[u'object_height']
#     width = temp_dict[u'image_width'] if temp_dict[u'image_width'] else temp_dict[u'object_width']
#     depth = temp_dict[u'object_depth']
#     return ( height, width, depth )


# def _set_physical_descriptions( original_dict, solr_dict ):
#     """ Updates physical_description_material, physical_description_technique.
#         Called by build_metadata_only_solr_dict() """
#     solr_dict[u'physical_description_material'] = [ u'' ]
#     solr_dict[u'physical_description_technique'] = [ u'' ]
#     if original_dict[u'object_medium'] != None:
#         solr_dict[u'physical_description_material'] = [ original_dict[u'object_medium'] ]
#     if original_dict[u'MEDIA_SUB::sub_media_name'] != None:
#         if type( original_dict[u'MEDIA_SUB::sub_media_name'] ) == unicode:
#             solr_dict[u'physical_description_technique'] = [ original_dict[u'MEDIA_SUB::sub_media_name'] ]
#         elif type( original_dict[u'MEDIA_SUB::sub_media_name'] ) == list:
#             cleaned_list = __ensure_list_unicode_values__handle_type_list( original_dict[u'MEDIA_SUB::sub_media_name'] )
#             solr_dict[u'physical_description_technique'] = cleaned_list
#     return solr_dict


# def _set_title( original_dict, solr_dict ):
#     """ Updates title.
#         Called by build_metadata_only_solr_dict() """
#     solr_dict[u'title'] = u''
#     if original_dict[u'object_title'] != None:
#         solr_dict[u'title'] = original_dict[u'object_title']
#     return solr_dict


# def _validate_solr_dict( solr_dict, logger ):
#     """ Returns True if checks pass; False otherwise.
#         Checks that required keys are present.
#         Checks that there are no None values.
#         Checks that any list values are not empty.
#         Checks that no members of a list value are of NoneType.
#         Called by build_metadata_only_solr_dict() """
#     try:
#         for required_key in REQUIRED_KEYS:
#             logger.debug( u'in tasks.indexer._validate_solr_dict(); required_key: %s' % required_key )
#             assert required_key in solr_dict.keys(), Exception( u'ERROR; missing required key: %s' % required_key )
#         for key,value in solr_dict.items():
#           assert value != None, Exception( u'ERROR; value is none for key: %s' % key )
#           if type(value) == list:
#             assert len(value) > 0, Exception( u'ERROR: key "%s" has a value of "%s", which is type-list, which is empty.' % ( key, value ) )
#             for element in value:
#               assert element != None, Exception( u'ERROR: key "%s" has a value "%s", which is type-list, which contains a None element' % ( key, value ) )
#         return True
#     except Exception as e:
#         logger.error( u'in tasks.indexer._validate_solr_dict(); exception is: %e' )
#         return False


# ## utils ##


# def __ensure_list_unicode_values( solr_dict, fields_to_check ):
#     """ Returns solr_dict updated to ensure the values for the specified dictionary-fields_to_check
#         are _always_ lists of unicode values.
#         Called by _set_author_dates(), _set_author_description(), _set_author_names() """
#     solr_dict_copy = solr_dict.copy()
#     for key in fields_to_check:
#         value = solr_dict[key]
#         if type(value) == unicode:
#           solr_dict_copy[key] = [ value ]
#         elif type(value) == list:
#             solr_dict_copy[key] = __ensure_list_unicode_values__handle_type_list( value )
#         elif value == None:
#           solr_dict_copy[key] = [ u'' ]
#     return solr_dict_copy


# def __ensure_list_unicode_values__handle_type_list( list_value ):
#     """ Returns updated list for inclusion as value of solr_dict entry.
#         Called by __ensure_list_unicode_values() and _set_physical_descriptions() """
#     new_list = []
#     if len( list_value ) == 0:
#       new_list.append( u'' )
#     else:
#       for entry in list_value:
#         new_list.append( u'' ) if (entry == None) else new_list.append( entry )
#     return new_list
