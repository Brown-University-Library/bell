# -*- coding: utf-8 -*-

from __future__ import unicode_literals

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

    ## main functions ##

    def build_metadata_only_solr_dict( self, pid, original_dict ):
        """ Builds dict-to-index using just basic item-dict data and pid; image-check handled in separate function.
            Called by utils.update_custom_index.py """
        solr_dict = {}
        solr_dict['accession_number_original'] = original_dict['calc_accession_id']
        solr_dict['pid'] = pid
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
        self.logger.debug( 'in tasks.indexer.build_metadata_only_solr_dict(); solr_dict, `%s`' % pprint.pformat(solr_dict) )
        return solr_dict

    def add_image_metadata( self, solr_dict, links_dict ):
        """ Adds image metadata to dict-to-index. """
        solr_dict['jp2_image_url'] = self._set_image_urls__get_jp2_url( links_dict )
        solr_dict['master_image_url'] = self._set_image_urls__get_master_image_url( links_dict, solr_dict['jp2_image_url'] )
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
                # self.logger.debug( 'in tasks.indexer._validate_solr_dict(); required_key: %s' % required_key )
                assert required_key in solr_dict.keys(), Exception( 'ERROR; missing required key: %s' % required_key )
            for key,value in solr_dict.items():
              assert value != None, Exception( 'ERROR; value is none for key: %s' % key )
              if type(value) == list:
                assert len(value) > 0, Exception( 'ERROR: key "%s" has a value of "%s", which is type-list, which is empty.' % ( key, value ) )
                for element in value:
                  assert element != None, Exception( 'ERROR: key "%s" has a value "%s", which is type-list, which contains a None element' % ( key, value ) )
            return True
        except Exception as e:
            self.logger.error( 'in tasks.indexer._validate_solr_dict(); exception is: %s' % unicode(repr(e)) )
            return False

    def post_to_solr( self, solr_dict ):
        """ Posts solr_dict to solr. """
        SOLR_ROOT_URL = ( os.environ.get('BELL_I_SOLR_ROOT') )
        solr = Solr( SOLR_ROOT_URL )
        response = solr.update( [solr_dict], 'xml', commit=True )  # 'xml' param converts default json to xml for post; required for our old version of solr
        response_status = response.status
        self.logger.info( 'in tasks.indexer.post_to_solr() [for custom-solr]; accession_number, %s; response_status, %s' % (solr_dict['accession_number_original'], response_status) )
        if not response_status == 200:
            raise Exception( 'custom-solr post problem logged' )
        return response_status

    def delete_item( self, pid ):
        """ Deletes item from custom bell index.
            Called by one_offs.rebuild_custom_index(). """
        SOLR_ROOT_URL = ( os.environ.get('BELL_I_SOLR_ROOT') )
        self.logger.info( 'in tasks.indexer.delete_item() [for custom-solr]; SOLR_ROOT_URL, %s' % SOLR_ROOT_URL )
        solr = Solr( SOLR_ROOT_URL )
        response = solr.delete_by_query( 'pid:"%s"' % pid, commit=True )
        response_status = response.status
        self.logger.info( 'in tasks.indexer.delete_item() [for custom-solr]; pid, %s; response_status, %s' % (pid, response_status) )
        if not response_status == 200:
            raise Exception( 'custom-solr delete problem logged' )
        return response_status

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
                self.logger.info( 'in tasks.indexer._set_image_urls__get_master_image_url(); odd case, links_dict is `%s`, jp2_url is `%s`' % (pprint.pformat(links_dict), jp2_url) )
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

    # end class Indexer()


## runners ##

def run_update_custom_index( data ):
    """ Runner for update_custom_index().
        Called by queue-job triggered by tasks.task_manager.determine_next_task(). """
    assert sorted( data.keys() ) == ['item_data', 'pid'], sorted( data.keys() )
    pass  # TODO: build this out (may require changing indexer to pass it the data-dict)
    # idx = Indexer( data, bell_logger.setup_logger() )
    # result_dict = idx.update_custom_index()  # done!
    return
