# -*- coding: utf-8 -*-

import bell_logger

""" Handles custom solr indexing after ingestion. """


REQUIRED_KEYS = [  # used by _validate_json()
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
    u'mods',
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


def build_metadata_only_solr_dict( data ):
    """ Builds dict-to-index using just basic item-dict data and pid; no need to check image info.
        Called after fedora_metadata_only_builder.run__create_fedora_metadata_object() task. """
    assert sorted( data.keys() ) == [ u'item_data', u'pid' ], Exception( u'- in indexer.build_metadata_only_solr_dict(); unexpected data.keys(): %s' % sorted(data.keys()) )
    logger = bell_logger.setup_logger()
    logger.info( u'in indexer.index_metadata_only(); acc_num , %s; pid, %s; processing will go here.' % (
        data[u'item_data'][u'calc_accession_id'], data[u'pid') )
    original_dict = data[u'item_dict']
    solr_dict = {}
    solr_dict = _set_accession_number_original( original_dict, solr_dict )
    solr_dict = _set_author_dates( original_dict, solr_dict )
    solr_dict = _set_author_description( original_dict, solr_dict )
    solr_dict = _set_author_names( original_dict, solr_dict )
    solr_dict = _set_height_width_depth( original_dict, solr_dict )
    solr_dict = _set_image_urls( original_dict, solr_dict, flag=u'metadata_only' )
    solr_dict = _set_locations( original_dict, solr_dict )
    solr_dict = _set_note_provenance( original_dict, solr_dict )
    solr_dict = _set_object_dates( original_dict, solr_dict )
    solr_dict = _set_physical_extent( original_dict, solr_dict )
    solr_dict = _set_physical_descriptions( original_dict, solr_dict )
    solr_dict = _set_title( original_dict, solr_dict )

    all_good_flag = _validate_solr_dict( solr_dict )
    logger.info( u'in indexer.build_metadata_only_solr_dict(); all_good_flag is %s; solr_dict is %s' % (
        (all_good_flag, solr_dict) )

    ## get next task ( 'index' or 'update_tracker_with_failure' )

    return


def _set_accession_number_original( original_dict, solr_dict ):
    """ Sets accession_number. """
    solr_dict[u'accession_number_original'] = original_dict[u'calc_accession_id']
    return solr_dict


def _set_author_dates( original_dict, solr_dict ):
    """ Sets author dates. """
    solr_dict[u'author_birth_date'] = original_dict[u'ARTISTS::artist_birth_year']
    solr_dict[u'author_death_date'] = original_dict[u'ARTISTS::artist_death_year']
    solr_dict[u'author_date'] = original_dict[u'ARTISTS::artist_lifetime']
    solr_dict = _ensure_list_unicode_values( solr_dict, [u'author_birth_date', u'author_death_date', u'author_date'] )
    return solr_dict


def _set_author_description( original_dict, solr_dict ):
    """ Sets author descriptions. """
    solr_dict[u'author_description'] = original_dict[u'ARTISTS::calc_nationality']
    solr_dict = _ensure_list_unicode_values( solr_dict, [u'author_description'] )
    return solr_dict


def _set_author_names( original_dict, solr_dict ):
    """ Sets author names. """
    solr_dict[u'author_names_first'] = original_dict[u'ARTISTS::artist_first_name']
    solr_dict[u'author_names_middle'] = original_dict[u'ARTISTS::artist_middle_name']
    solr_dict[u'author_names_last'] = original_dict[u'ARTISTS::artist_last_name']
    solr_dict[u'author_display'] = original_dict[u'ARTISTS::calc_artist_full_name']
    solr_dict = _ensure_list_unicode_values( solr_dict, [u'author_names_first',
        u'author_names_middle', u'author_names_last', u'author_display'] )
    return solr_dict


def _set_height_width_depth( original_dict, solr_dict ):
    """ Sets item dimensions. """
    target_keys = [ u'image_width', u'image_height', u'object_width', u'object_height', u'object_depth' ]
    for entry in target_keys:
        if original_dict[entry] == None:
            solr_dict[entry] = u''
        else:
            solr_dict[entry] = original_dict[entry]
    return solr_dict


def _set_image_urls( solr_dict, pid=None, flag=u'metadata_only' ):
    """  Sets jp2 and master image-url info. """
    solr_dict[u'jp2_image_url'] = u''
    solr_dict[u'master_image_url'] = u''
    if flag == u'metadata_only':
        pass
    else:
        assert pid not None
        item_api_dict = _set_image_urls__get_item_api_data( pid )
        if item_api_dict not None:
            solr_dict[u'jp2_image_url'] = _set_image_urls__get_jp2_url( item_api_dict )
            solr_dict[u'master_image_url'] = _set_image_urls__get_master_image_url( item_api_dict )
    return solr_dict


def _set_image_urls__get_item_api_data( pid ):
    """ Returns repo public item-api json, or None.
        Called by _set_image_urls() """
    for i in range( 5 ):
      try:
        url = u'https://repository.library.brown.edu/api/pub/items/%s/' % pid
        r = requests.get( url )
        jdict = r.json()
        return jdict
      except:
        time.sleep( 2 )
    return None


def _set_image_urls__get_jp2_url( item_api_dict ):
    """ Returns jp2 url or u''.
        Called by _set_image_urls() """
    try:
        image_url = item_api_dict[u'links'][u'content_datastreams'][u'JP2']
    except:
        image_url = u''
    return image_url


def _set_image_urls__get_master_image_url( item_api_dict ):
    """ Returns master image url or u''.
        Called by _set_image_urls() """
    try:
        image_url = item_api_dict[u'links'][u'content_datastreams'][u'TIFF']
    except:
        try:
            image_url = item_api_dict[u'links'][u'content_datastreams'][u'JPG']
        except:
            image_url = u''
    return image_url


def _set_locations( original_dict, solr_dict ):
    """ Returns location_physical_location & location_shelf_locator. """
    solr_dict[u'location_physical_location'] = u'Bell Art Gallery'
    solr_dict[u'location_shelf_locator'] = u''
    if original_dict[u'MEDIA::object_medium_name'] not None:
      solr_dict[u'location_shelf_locator'] = original_dict[u'MEDIA::object_medium_name']
    return solr_dict


def _set_note_provenance( original_dict, solr_dict ):
    """ Updates note_provenance. """
    solr_dict[u'note_provenance'] = u''
    if original_dict[u'credit_line'] not None:
      solr_dict[u'note_provenance'] = original_dict[u'credit_line']
    return solr_dict


def _set_object_dates( original_dict, solr_dict ):
    """ Updates object_date, origin_datecreated_start, origin_datecreated_end. """
    solr_dict[u'object_date'] = u''
    solr_dict[u'origin_datecreated_start'] = u''
    solr_dict[u'origin_datecreated_end'] = u''
    if original_dict[u'object_date'] not None:
        solr_dict[u'object_date'] = original_dict[u'object_date']
    if original_dict[u'object_year_start'] not None:
        solr_dict[u'origin_datecreated_start'] = original_dict[u'object_year_start']
    if original_dict[u'object_year_end'] not None:
        solr_dict[u'origin_datecreated_end'] = original_dict[u'object_year_end']
    return solr_dict


def _set_physical_extent( original_dict, solr_dict ):
    """ Updates physical_description_extent, which is a display field based on the five dimension fields. """
    ( height, width, depth ) = _set_physical_extent_prep( original_dict )
    solr_dict[u'physical_description_extent'] = [ u'' ]
    if height and width and depth:
        solr_dict[u'physical_description_extent'] = [ u'%s x %s x %s' % (height, width, depth) ]
    elif height and width:
        solr_dict[u'physical_description_extent'] = [ u'%s x %s' % (height, width ) ]
    return solr_dict


def  _set_physical_extent_prep( original_dict ):
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


def _set_physical_descriptions( original_dict, solr_dict ):
    """ Updates physical_description_material, physical_description_technique. """
    solr_dict[u'physical_description_material'] = [ u'' ]
    solr_dict[u'physical_description_technique'] = [ u'' ]
    if original_dict[u'object_medium'] not None:
        solr_dict[u'physical_description_material'] = [ original_dict[u'object_medium'] ]
    if original_dict[u'MEDIA_SUB::sub_media_name'] not None:
        if type( original_dict[u'MEDIA_SUB::sub_media_name'] ) == unicode:
            solr_dict[u'physical_description_technique'] = [ original_dict[u'MEDIA_SUB::sub_media_name'] ]
        elif type( original_dict[u'MEDIA_SUB::sub_media_name'] ) == list:
            cleaned_list = _ensure_list_unicode_values__handle_type_list( original_dict[u'MEDIA_SUB::sub_media_name'] ):
            solr_dict[u'physical_description_technique'] = cleaned_list
    return solr_dict


def _set_title( original_dict, solr_dict ):
    """ Updates title. """
    solr_dict[u'object_title'] = u''
    if original_dict[u'title'] not None:
        solr_dict[u'object_title'] = original_dict[u'title']
    return solr_dict


def _validate_solr_dict( solr_dict ):
    """ Returns True if checks pass; False otherwise.
        Checks that required keys are present.
        Checks that there are no None values.
        Checks that any list values are not empty.
        Checks that no members of a list value are of NoneType. """
    try:
        for required_key in REQUIRED_KEYS:
            assert required_key in solr_dict.keys(), Exception( u'ERROR; missing required key: %s' % required_key )
        for key,value in solr_dict.items():
          assert value != None, Exception( u'ERROR; value is none for key: %s' % key )
          if type(value) == list:
            assert len(value) > 0, Exception( u'key "%s" has a value of "%s", which is type-list, which is empty.' % ( key, value ) )
            for element in value:
              assert element != None, Exception( u'key "%s" has a value "%s", which is type-list, which contains a None element' % ( key, value ) )
        return True
    except:
        return False


## utils ##


def _ensure_list_unicode_values( solr_dict, fields_to_check ):
    """ Returns solr_dict updated to ensure the values for the specified dictionary-fields_to_check
        are _always_ lists of unicode values. """
    solr_dict_copy = solr_dict.copy()
    for key in fields_to_check:
        value = solr_dict[key]
        if type(value) == unicode:
          solr_dict_copy[key] = [ value ]
        elif type(value) == list:
            solr_dict_copy[key] = _ensure_list_unicode_values__handle_type_list( value )
        elif value == None:
          solr_dict_copy[key] = [ u'' ]
    return solr_dict_copy


def _ensure_list_unicode_values__handle_type_list( list_value ):
    """ Returns updated list for inclusion as value of solr_dict entry.
        Called by _ensure_list_unicode_values() and _set_physical_descriptions() """
    new_list = []
    if len( list_value ) == 0:
      new_list.append( u'' )
    else:
      for entry in list_value:
        new_list.append( u'' ) if (entry == None) else new_list.append( entry )
    return new_list
