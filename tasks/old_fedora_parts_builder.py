# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import datetime, os, pprint, urllib
import envoy, eulxml
from bdrxml import irMetadata, mods, rels, rights
from lxml import etree
from lxml.etree import XMLSyntaxError

""" Contains ImageBuilder(), IRBuilder(), ModsBuilder(), and RightsBuilder() classes.
    Currently called by fedora_metadata_only_builder.py and fedora_metadata_and_image_builder.py; likely others in future. """


class ImageBuilder( object ):
    """ Handles repo new-object datastream assignment.
        Handles repo new-object rels-int assignment.
        Creates jp2. """

    def __init__( self, logger ):
        self.logger = logger

    ## create jp2 ##

    def create_jp2( self, source_filepath, destination_filepath ):
        """ Creates jp2.
            Called by fedora_metadata_and_image_builder.add_metadata_and_image().
            TODO: consider making this a separate queue job. """
        self.logger.debug( 'in fedora_parts_builder._create_jp2(); source_filepath, %s' % source_filepath )
        self.logger.debug( 'in fedora_parts_builder._create_jp2(); destination_filepath, %s' % destination_filepath )
        KAKADU_COMMAND_PATH = unicode( os.environ.get('BELL_FPB__KAKADU_COMMAND_PATH') )
        CONVERT_COMMAND_PATH = unicode( os.environ.get('BELL_FPB__CONVERT_COMMAND_PATH') )
        if source_filepath.split( '.' )[-1] == 'tif':
            self.logger.debug( 'in fedora_parts_builder._create_jp2(); in `tif` handling' )
            self._create_jp2_from_tif( KAKADU_COMMAND_PATH, source_filepath, destination_filepath )
        elif source_filepath.split( '.' )[-1] == 'jpg':
            self.logger.debug( 'in fedora_parts_builder._create_jp2(); in `jpg` handling' )
            self._create_jp2_from_jpg( CONVERT_COMMAND_PATH, KAKADU_COMMAND_PATH, source_filepath, destination_filepath )
        return

    def _create_jp2_from_tif( self, KAKADU_COMMAND_PATH, source_filepath, destination_filepath ):
        """ Creates jp2 directly. """
        cleaned_source_filepath = source_filepath.replace( ' ', '\ ' )
        cmd = '%s -i "%s" -o "%s" Creversible=yes -rate -,1,0.5,0.25 Clevels=12' % (
            KAKADU_COMMAND_PATH, cleaned_source_filepath, destination_filepath )
        self.logger.info( 'in fedora_parts_builder._create_jp2_from_tif(); cmd, %s' % cmd )
        r = envoy.run( cmd.encode('utf-8', 'replace') )  # envoy requires a non-unicode string
        self.logger.info( 'in fedora_parts_builder._create_jp2_from_tif(); r.std_out, %s' % r.std_out )
        self.logger.info( 'in fedora_parts_builder._create_jp2_from_tif(); r.std_err, %s' % r.std_err )
        return

    def _create_jp2_from_jpg( self, CONVERT_COMMAND_PATH, KAKADU_COMMAND_PATH, source_filepath, destination_filepath ):
        """ Creates jp2 after first converting jpg to tif (due to server limitation). """
        cleaned_source_filepath = source_filepath.replace( ' ', '\ ' )
        self.logger.debug( 'in fedora_parts_builder._create_jp2_from_jpg(); cleaned_source_filepath, %s' % cleaned_source_filepath )
        tif_destination_filepath = destination_filepath[0:-4] + '.tif'
        self.logger.debug( 'in fedora_parts_builder._create_jp2_from_jpg(); tif_destination_filepath, %s' % tif_destination_filepath )
        cmd = '%s -compress None "%s" %s' % (
            CONVERT_COMMAND_PATH, cleaned_source_filepath, tif_destination_filepath )  # source-filepath quotes needed for filename containing spaces
        self.logger.debug( 'in fedora_parts_builder._create_jp2_from_jpg(); cmd, %s' % cmd )
        r = envoy.run( cmd.encode('utf-8', 'replace') )
        self.logger.info( 'in fedora_parts_builder._create_jp2_from_jpg(); r.std_out, %s; type(r.std_out), %s' % (r.std_out, type(r.std_out)) )
        self.logger.info( 'in fedora_parts_builder._create_jp2_from_jpg(); r.std_err, %s; type(r.std_err), %s' % (r.std_err, type(r.std_err)) )
        if len( r.std_err ) > 0:
            raise Exception( 'Problem making intermediate .tif from .jpg' )
        source_filepath = tif_destination_filepath
        cmd = '%s -i "%s" -o "%s" Creversible=yes -rate -,1,0.5,0.25 Clevels=12' % (
            KAKADU_COMMAND_PATH, source_filepath, destination_filepath )
        r = envoy.run( cmd.encode('utf-8', 'replace') )
        os.remove( tif_destination_filepath )
        return

    ## datastream work ##

    def build_master_datastream_vars( self, filename, image_dir_url ):
        """ Builds pieces necessary for assigning the master datastream.
            Called by fedora_metadata_and_image_builder.add_metadata_and_image(). """
        encoded_filename = urllib.quote( filename ).decode( 'utf-8' )
        file_url = '%s/%s' % ( image_dir_url, encoded_filename )
        dsID = 'MASTER'
        mime_type = 'image/tiff'
        if filename.split( '.' )[-1] == 'jpg':
            mime_type = 'image/jpeg'
        return ( file_url, dsID, mime_type )

    def build_jp2_datastream_vars( self, filename, image_dir_url ):
        """ Builds pieces necessary for assigning the jp2 datastream.
            Called by fedora_metadata_and_image_builder.add_metadata_and_image(). """
        encoded_filename = urllib.quote( filename ).decode( 'utf-8' )
        file_url = '%s/%s' % ( image_dir_url, encoded_filename )
        dsID = 'JP2'
        mime_type = 'image/jp2'
        return ( file_url, dsID, mime_type )

    def update_object_datastream( self, new_obj, dsID, file_url, mime_type ):
        """ Updates the new-object's datastream property.
            Returns the updated new-object.
            Called by fedora_metadata_and_image_builder.add_metadata_and_image().
            Possible TODO: try passing this a 'data-stream' object (instead of the whole new_obj, update it, and send it back.
                  then in the calling code, say new_obj.master = ds_object and new_obj.jp2 = ds_object. """
        python_dsID = dsID.lower()
        ds = getattr( new_obj, python_dsID )
        ds.ds_location = file_url
        ds.mimetype = mime_type
        return new_obj

    ## rels-int work ##

    def update_newobj_relsint( self, filename, new_obj, dsID ):
        """ Takes the repo new-obj and a dsID of 'MASTER' or 'JP2'.
            Updates the new-object's rels-int.
            Returns the updated new-object.
            Called by fedora_metadata_and_image_builder.add_metadata_and_image(). """
        download_filename = filename.replace( ' ', '_' )
        download_description = rels.Description()
        download_description.about = '%s/%s' % ( new_obj.uriref, dsID )
        download_description.download_filename = download_filename
        new_obj.rels_int.content.descriptions.append( download_description )
        return new_obj

    # end class ImageBuilder()


class ModsBuilder( object ):
    """ Handles mods creation.
         """

    def __init__( self ):
        """ Simplifies mods-element and MODS-namespace references in multiple functions. """
        self.MODS = None  # namespace holder
        self.mods = None  # etree.Element()
        self.accession_number = None  # populated by _build...; used by _make...

    def build_mods_object( self, bell_dict_item, mods_schema_path, return_type ):
        """ CONTROLLER.
            Returns validated mods_xml. """
        self._initialize_mods( bell_dict_item['calc_accession_id'] )
        self._build_mods_element( bell_dict_item )
        mods_xml = self._make_mods_xml_string()
        self._validate_mods( mods_xml, mods_schema_path )
        if return_type == 'return_string':
            return { 'data': mods_xml, 'accession_number': self.accession_number }
        else:
            mods_object = eulxml.xmlmap.load_xmlobject_from_string( mods_xml, mods.Mods )  # eulfedora.server.Repository compatible
            assert unicode(repr(type(mods_object))) == u"<class 'bdrxml.mods.Mods'>", unicode(repr(type(mods_object)))
            return { 'data': mods_object, 'accession_number': self.accession_number }

    ## HELPER FUNCTIONS ##

    def _initialize_mods( self, mods_id ):
        """ Initialize empty self.mods element; also sets namespace reference. """
        MODS_NAMESPACE = 'http://www.loc.gov/mods/v3'
        self.MODS = '{%s}' % MODS_NAMESPACE
        NSMAP = { 'mods' : MODS_NAMESPACE }
        self.mods = etree.Element(
            self.MODS+'mods',
            nsmap=NSMAP,
            xmlns_xsi='http://www.w3.org/2001/XMLSchema-instance',
            ID='TEMP_MODS_ID',
            xsi_schemaLocation='http://www.loc.gov/mods/v3 http://www.loc.gov/standards/mods/futures/mods-3-4.xsd'  # underscore will be replaced with colon
            )
        return self.mods

    def _build_mods_element( self, data_dict ):
        """ Builds out (previously-initialized) self.mods element. """
        self.__build_title_stuff( data_dict )
        self.__build_name_stuff( data_dict )
        self.__build_origin_stuff( data_dict )
        self.__build_physical_stuff( data_dict )
        self.__build_note_stuff( data_dict )
        self.__build_location_stuff( data_dict )
        self.__build_identifier_stuff( data_dict )
        return

    def __build_title_stuff( self, data_dict ):
        """ Adds to self.mods title_info and title.
            Called by _build_mods_element() """
        title_info = etree.SubElement( self.mods, self.MODS+'titleInfo' )
        title = etree.SubElement( title_info, self.MODS+'title' )
        title.text = data_dict[ 'object_title' ]
        return

    def __build_name_stuff( self, data_dict ):
        """ Adds to self.mods name, name_part, name_display_form, name_part_dates, name_role.
            Called by _build_mods_element() """
        for i,name in enumerate( data_dict['ARTISTS::artist_last_name'] ):
            name = etree.SubElement( self.mods, self.MODS+'name', type='personal' )
            name_part = etree.SubElement( name, self.MODS+'namePart' )
            name_part.text = '%s, %s' % ( data_dict['ARTISTS::artist_last_name'][i], data_dict['ARTISTS::artist_first_name'][i] )
            name_display_form = etree.SubElement( name, self.MODS+'displayForm' )
            name_display_form.text = data_dict[ 'ARTISTS::calc_artist_full_name' ][i]
            name_part_dates = etree.SubElement( name, self.MODS+'namePart', type='date' )
            name_part_dates.text = data_dict[ 'ARTISTS::artist_lifetime' ][i]
            name_role = etree.SubElement( name, self.MODS+'role' )
            name_role_term = etree.SubElement( name_role, self.MODS+'roleTerm', type='text' )
            name_role_term.text = 'artist'
        return

    def __build_origin_stuff( self, data_dict ):
        """ Adds to self.mods origin_info.
            Called by _build_mods_element() """
        origin_info = etree.SubElement( self.mods, self.MODS+'originInfo' )
        origin_info_dt_created_start = etree.SubElement( origin_info, self.MODS+'dateCreated', encoding='w3cdtf', point='start' )
        origin_info_dt_created_start.text = data_dict[ 'object_year_start' ]
        origin_info_dt_created_end = etree.SubElement( origin_info, self.MODS+'dateCreated', encoding='w3cdtf', point='end' )
        origin_info_dt_created_end.text = data_dict[ 'object_year_end' ]
        return

    def __build_physical_stuff( self, data_dict ):
        """ Adds to self.mods physical_description, physical_description_form_material, physical_description_form_technique.
            Called by _build_mods_element() """
        physical_description = etree.SubElement( self.mods, self.MODS+'physicalDescription' )
        physical_description_form_material = etree.SubElement( physical_description, self.MODS+'form', type='material' )
        physical_description_form_material.text = data_dict[ 'object_medium' ]
        for entry in data_dict['MEDIA_SUB::sub_media_name']:
            physical_description_form_technique = etree.SubElement( physical_description, self.MODS+'form', type='technique' )
            physical_description_form_technique.text = entry
        return

    def __build_note_stuff( self, data_dict ):
        """ Adds to self.mods note.
            Called by _build_mods_element() """
        note = etree.SubElement( self.mods, self.MODS+'note', type='provenance' )
        note.text = data_dict[ 'credit_line' ]
        return

    def __build_location_stuff( self, data_dict ):
        """ Adds to self.mods location_physical_location, location_holdings_simple_copy_information_shelf_locator.
            Called by _build_mods_element() """
        location = etree.SubElement( self.mods, self.MODS+'location' )
        location_physical_location = etree.SubElement( location, self.MODS+'physicalLocation' )
        location_physical_location.text = 'Bell Art Gallery'
        location_holdings_simple = etree.SubElement( location, self.MODS+'holdingSimple' )
        location_holdings_simple_copy_information = etree.SubElement( location_holdings_simple, self.MODS+'copyInformation' )
        location_holdings_simple_copy_information_shelf_locator = etree.SubElement( location_holdings_simple_copy_information, self.MODS+'shelfLocator' )
        location_holdings_simple_copy_information_shelf_locator.text = data_dict[ 'MEDIA::object_medium_name' ]
        return

    def __build_identifier_stuff( self, data_dict ):
        """ Adds to self.mods identifiers 'bell_accession_number' and 'bell_object_id'.
            Populates self.accession_number
            Called by _build_mods_element() """
        identifier = etree.SubElement( self.mods, self.MODS+'identifier', type='bell_accession_number' )
        identifier.text = data_dict[ 'calc_accession_id' ]
        identifier2 = etree.SubElement( self.mods, self.MODS+'identifier', type='bell_object_id' )
        identifier2.text = data_dict[ 'object_id' ]
        self.accession_number = identifier.text  # will be used by _make...
        return

    def _make_mods_xml_string( self ):
        """ Returns unicode xml string to pass to validator. """
        doc = etree.ElementTree( self.mods )
        mods_string = etree.tostring( doc, pretty_print=True ).decode( 'utf-8', 'replace' )
        mods_string = mods_string.replace( 'xmlns_xsi', 'xmlns:xsi')
        mods_string = mods_string.replace( 'xsi_schemaLocation', 'xsi:schemaLocation' )
        valid_id = self.accession_number.replace( ' ', '' )
        valid_id = valid_id.replace( ',', '' )
        mods_string = mods_string.replace( 'TEMP_MODS_ID', valid_id )
        assert type(mods_string) == unicode
        return mods_string

    def _validate_mods( self, mods_xml, mods_schema_path ):
        """ Validates mods_xml string. """
        schema_object = self.__make_schema_object( mods_schema_path )
        parser = etree.XMLParser( schema=schema_object )
        try:
            doc = etree.fromstring( mods_xml, parser )
        except XMLSyntaxError as e:
            message = '- in BellModsMaker._validate_mods(); error is, %s' % unicode(repr(e))
            self.logger.error( message )
            raise Exception( message )
        return True

    def __make_schema_object( self, mods_schema_path ):
        """ Returns etree schema object.
            Called by _validate_mods(); separated out to debug intermittent schema-creation problem. """
        with open( mods_schema_path, 'r' ) as f:
            schema_string = f.read()
        schema_ustring = schema_string.decode( 'utf-8', 'replace' )
        schema_root = etree.XML( schema_ustring.encode( 'utf-8', 'replace') )  # "Unicode strings with encoding declaration are not supported."
        try:
            schema_object = etree.XMLSchema( schema_root )
        except Exception as e:
            message = '- in BellModsMaker.__make_schema_object(); mods_schema_path is, %s; schema_ustring is, %s; type(schema_root) is, %s; and error is, %s' % ( mods_schema_path, schema_ustring, unicode(repr(type(schema_root))), unicode(repr(e)) )
            self.logger.error( message )
            raise Exception( message )
        return schema_object

    # end class ModsBuilder()


class IRBuilder( object ):
    """ Handles ir creation via bdrxml. """

    def build_ir_object( self ):
        ''' Creates basic bell ir object.
            'ir_obj.date' and if necessary, 'ir_obj.filename', will be set dynamically
            NOTE: 2015-03-05 -- date unnecessary; already in fedora.
            '''
        obj = irMetadata.make_ir()
        assert unicode(repr(type(obj))) == u"<class 'bdrxml.irMetadata.IR'>", unicode(repr(type(obj)))
        obj.depositor_name = 'Bell Gallery'
        obj.date = datetime.datetime.today()  # ir.date = '%s' % now.strftime('%Y-%m-%d')
        obj.date = '%s' % datetime.datetime.now().strftime( '%Y-%m-%d' )
        return obj

    # end class IRBuilder()


class RightsBuilder( object ):
    """ Handles rights creation via bdrxml. """

    def __init__( self ):
        self.context_dict = {
            'repo_manager': {
                'id': 'rts001', 'username': 'BROWN:DEPARTMENT:LIBRARY:REPOSITORY', 'cclass': 'REPOSITORY MGR',
                'discover': True, 'display': True, 'modify': True, 'delete': True },
            'bell_gallery': {
                'id': 'rts002', 'username': 'Bell Gallery', 'cclass': 'GENERAL PUBLIC',
                'discover': True, 'display': True, 'modify': True, 'delete': True },
            'general_public': {
                'id': 'rts003', 'username': 'BDR_PUBLIC', 'cclass': 'GENERAL PUBLIC',
                'discover': True, 'display': True, 'modify': False, 'delete': False },
            }

    def build_rights_object( self ):
        """ CONTROLLER.
            Returns basic bell rights object created via bdrxml. """
        obj = rights.make_rights()
        obj.category = 'COPYRIGHTED'
        obj = self._add_holder_data( obj )
        obj = self._add_rights_context( obj, 'repo_manager' )
        obj = self._add_rights_context( obj, 'bell_gallery' )
        obj = self._add_rights_context( obj, 'general_public' )
        obj.date = datetime.date.today()
        assert unicode(repr(type(obj))) == "<class 'bdrxml.rights.Rights'>", unicode(repr(type(obj)))
        return obj

    def _add_holder_data( self, obj ):
        """ Returns object with holder.name and holder.context_ids """
        obj.create_holder()
        obj.holder.name = 'Contact Bell Gallery for details: <http://www.brown.edu/campus-life/arts/bell-gallery/about/contact-information>'
        obj.holder.context_ids = 'rts001 rts002 rts003'
        return obj

    def _add_rights_context( self, obj, rights_holder ):
        """ Returns object with a rights-context """
        rc = rights.Context()
        rc.id = self.context_dict[rights_holder]['id']
        rc.usertype = 'GROUP'
        rc.username = self.context_dict[rights_holder]['username']
        rc.cclass = self.context_dict[rights_holder]['cclass']
        rc.discover = self.context_dict[rights_holder]['discover']
        rc.display = self.context_dict[rights_holder]['display']
        rc.modify = self.context_dict[rights_holder]['modify']
        rc.delete = self.context_dict[rights_holder]['delete']
        obj.ctext.append( rc )
        return obj

    # end class RightsBuilder()

