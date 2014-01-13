# -*- coding: utf-8 -*-

import bell_logger

""" Handles custom solr indexing after ingestion. """


def build_metadata_only_solr_dict( data ):
    """ Builds dict-to-index using just basic item-dict data and pid; no need to check image info.
        Called after fedora_metadata_only_builder.run__create_fedora_metadata_object() task. """
    assert sorted( data.keys() ) == [ u'item_data', u'pid' ]
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
    solr_dict = _set_mods( original_dict, solr_dict )
    solr_dict = _set_note_provenance( original_dict, solr_dict )
    solr_dict = _set_object_dates( original_dict, solr_dict )
    solr_dict = _set_physical_extent( original_dict, solr_dict )
    solr_dict = _set_physical_descriptions( original_dict, solr_dict )
    solr_dict = _set_title( original_dict, solr_dict )

    all_good_flag = _validate_json( solr_dict )
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
        Called by _ensure_list_unicode_values() """
    new_list = []
    if len( list_value ) == 0:
      new_list.append( u'' )
    else:
      for entry in list_value:
        new_list.append( u'' ) if (entry == None) else new_list.append( entry )
    return new_list
