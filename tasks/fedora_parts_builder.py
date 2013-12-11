# -*- coding: utf-8 -*-

import datetime, os, pprint

from bdrxml import irMetadata, mods, rights
import eulxml
# from bdrxml.mods import Mods

from lxml import etree
from lxml.etree import XMLSyntaxError


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
        self._initialize_mods( bell_dict_item[u'calc_accession_id'] )
        self._build_mods_element( bell_dict_item )
        mods_xml = self._make_mods_xml_string()
        self._validate_mods( mods_xml, mods_schema_path )
        if return_type == u'return_string':
            return { u'data': mods_xml, u'accession_number': self.accession_number }
        else:
            mods_object = eulxml.xmlmap.load_xmlobject_from_string( mods_xml, mods.Mods )  # eulfedora.server.Repository compatible
            assert unicode(repr(type(mods_object))) == u"<class 'bdrxml.mods.Mods'>", unicode(repr(type(mods_object)))
            return { u'data': mods_object, u'accession_number': self.accession_number }

    ## HELPER FUNCTIONS ##

    def _initialize_mods( self, mods_id ):
        """ Initialize empty self.mods element; also sets namespace reference. """
        MODS_NAMESPACE = u'http://www.loc.gov/mods/v3'
        self.MODS = u'{%s}' % MODS_NAMESPACE
        NSMAP = { u'mods' : MODS_NAMESPACE }
        self.mods = etree.Element(
            self.MODS+u'mods',
            nsmap=NSMAP,
            xmlns_xsi=u'http://www.w3.org/2001/XMLSchema-instance',
            ID=u'TEMP_MODS_ID',
            xsi_schemaLocation=u'http://www.loc.gov/mods/v3 http://www.loc.gov/standards/mods/futures/mods-3-4.xsd'  # underscore will be replaced with colon
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
        title_info = etree.SubElement( self.mods, self.MODS+u'titleInfo' )
        title = etree.SubElement( title_info, self.MODS+u'title' )
        title.text = data_dict[ u'object_title' ]
        return

    def __build_name_stuff( self, data_dict ):
        """ Adds to self.mods name, name_part, name_display_form, name_part_dates, name_role.
            Called by _build_mods_element() """
        for i,name in enumerate( data_dict[u'ARTISTS::artist_last_name'] ):
            name = etree.SubElement( self.mods, self.MODS+u'name', type=u'personal' )
            name_part = etree.SubElement( name, self.MODS+u'namePart' )
            name_part.text = u'%s, %s' % ( data_dict[u'ARTISTS::artist_last_name'][i], data_dict[u'ARTISTS::artist_first_name'][i] )
            name_display_form = etree.SubElement( name, self.MODS+u'displayForm' )
            name_display_form.text = data_dict[ u'ARTISTS::calc_artist_full_name' ][i]
            name_part_dates = etree.SubElement( name, self.MODS+u'namePart', type=u'date' )
            name_part_dates.text = data_dict[ u'ARTISTS::artist_lifetime' ][i]
            name_role = etree.SubElement( name, self.MODS+u'role' )
            name_role_term = etree.SubElement( name_role, self.MODS+u'roleTerm', type=u'text' )
            name_role_term.text = u'artist'
        return

    def __build_origin_stuff( self, data_dict ):
        """ Adds to self.mods origin_info.
            Called by _build_mods_element() """
        origin_info = etree.SubElement( self.mods, self.MODS+u'originInfo' )
        origin_info_dt_created_start = etree.SubElement( origin_info, self.MODS+u'dateCreated', encoding=u'w3cdtf', point=u'start' )
        origin_info_dt_created_start.text = data_dict[ u'object_year_start' ]
        origin_info_dt_created_end = etree.SubElement( origin_info, self.MODS+u'dateCreated', encoding=u'w3cdtf', point=u'end' )
        origin_info_dt_created_end.text = data_dict[ u'object_year_end' ]
        return

    def __build_physical_stuff( self, data_dict ):
        """ Adds to self.mods physical_description, physical_description_form_material, physical_description_form_technique.
            Called by _build_mods_element() """
        physical_description = etree.SubElement( self.mods, self.MODS+u'physicalDescription' )
        physical_description_form_material = etree.SubElement( physical_description, self.MODS+u'form', type=u'material' )
        physical_description_form_material.text = data_dict[ u'object_medium' ]
        for entry in data_dict[u'MEDIA_SUB::sub_media_name']:
            physical_description_form_technique = etree.SubElement( physical_description, self.MODS+u'form', type=u'technique' )
            physical_description_form_technique.text = entry
        return

    def __build_note_stuff( self, data_dict ):
        """ Adds to self.mods note.
            Called by _build_mods_element() """
        note = etree.SubElement( self.mods, self.MODS+u'note', type=u'provenance' )
        note.text = data_dict[ u'credit_line' ]
        return

    def __build_location_stuff( self, data_dict ):
        """ Adds to self.mods location_physical_location, location_holdings_simple_copy_information_shelf_locator.
            Called by _build_mods_element() """
        location = etree.SubElement( self.mods, self.MODS+u'location' )
        location_physical_location = etree.SubElement( location, self.MODS+u'physicalLocation' )
        location_physical_location.text = u'Bell Art Gallery'
        location_holdings_simple = etree.SubElement( location, self.MODS+u'holdingSimple' )
        location_holdings_simple_copy_information = etree.SubElement( location_holdings_simple, self.MODS+u'copyInformation' )
        location_holdings_simple_copy_information_shelf_locator = etree.SubElement( location_holdings_simple_copy_information, self.MODS+u'shelfLocator' )
        location_holdings_simple_copy_information_shelf_locator.text = data_dict[ u'MEDIA::object_medium_name' ]
        return

    def __build_identifier_stuff( self, data_dict ):
        """ Adds to self.mods identifiers 'bell_accession_number' and 'bell_object_id'.
            Populates self.accession_number
            Called by _build_mods_element() """
        identifier = etree.SubElement( self.mods, self.MODS+u'identifier', type=u'bell_accession_number' )
        identifier.text = data_dict[ u'calc_accession_id' ]
        identifier2 = etree.SubElement( self.mods, self.MODS+u'identifier', type=u'bell_object_id' )
        identifier2.text = data_dict[ u'object_id' ]
        self.accession_number = identifier.text  # will be used by _make...
        return

    def _make_mods_xml_string( self ):
        """ Returns unicode xml string to pass to validator. """
        doc = etree.ElementTree( self.mods )
        mods_string = etree.tostring( doc, pretty_print=True ).decode( u'utf-8', u'replace' )
        mods_string = mods_string.replace( u'xmlns_xsi', u'xmlns:xsi')
        mods_string = mods_string.replace( u'xsi_schemaLocation', u'xsi:schemaLocation' )
        valid_id = self.accession_number.replace( u' ', u'' )
        valid_id = valid_id.replace( u',', '' )
        mods_string = mods_string.replace( u'TEMP_MODS_ID', valid_id )
        assert type(mods_string) == unicode
        return mods_string

    def _validate_mods( self, mods_xml, mods_schema_path ):
        """ Validates mods_xml string. """
        schema_object = self.__make_schema_object( mods_schema_path )
        parser = etree.XMLParser( schema=schema_object )
        try:
            doc = etree.fromstring( mods_xml, parser )
        except XMLSyntaxError as e:
            message = u'- in BellModsMaker._validate_mods(); error is, %s' % unicode(repr(e))
            self.logger.error( message )
            raise Exception( message )
        return True

    def __make_schema_object( self, mods_schema_path ):
        """ Returns etree schema object.
            Called by _validate_mods(); separated out to debug intermittent schema-creation problem. """
        with open( mods_schema_path, u'r' ) as f:
            schema_string = f.read()
        schema_ustring = schema_string.decode( u'utf-8', u'replace' )
        schema_root = etree.XML( schema_ustring.encode( u'utf-8', u'replace') )  # "Unicode strings with encoding declaration are not supported."
        try:
            schema_object = etree.XMLSchema( schema_root )
        except Exception as e:
            message = u'- in BellModsMaker.__make_schema_object(); mods_schema_path is, %s; schema_ustring is, %s; type(schema_root) is, %s; and error is, %s' % ( mods_schema_path, schema_ustring, unicode(repr(type(schema_root))), unicode(repr(e)) )
            self.logger.error( message )
            raise Exception( message )
        return schema_object

    # end class ModsBuilder()


class IRBuilder( object ):
    """ Handles ir creation via bdrxml. """

    def build_ir_object( self ):
        ''' Creates basic bell ir object.
            Called by bell_2013_05.ingest_new_metadata_objects()
            'ir_obj.date' and if necessary, 'ir_obj.filename', will be set dynamically '''
        obj = irMetadata.make_ir()
        obj.depositor_name = u'Bell Gallery'
        assert unicode(repr(type(obj))) == u"<class 'bdrxml.irMetadata.IR'>", unicode(repr(type(obj)))
        return obj

    # end class IRBuilder()


class RightsBuilder( object ):
    """ Handles rights creation via bdrxml. """

    def __init__( self ):
        self.context_dict = {
            u'repo_manager': {
                u'id': u'rts001', u'username': u'BROWN:DEPARTMENT:LIBRARY:REPOSITORY', u'cclass': u'REPOSITORY MGR',
                u'discover': True, u'display': True, u'modify': True, u'delete': True },
            u'bell_gallery': {
                u'id': u'rts002', u'username': u'Bell Gallery', u'cclass': u'GENERAL PUBLIC',
                u'discover': True, u'display': True, u'modify': True, u'delete': True },
            u'general_public': {
                u'id': u'rts003', u'username': u'BDR_PUBLIC', u'cclass': u'GENERAL PUBLIC',
                u'discover': True, u'display': True, u'modify': False, u'delete': False },
            }

    def build_rights_object( self ):
        """ CONTROLLER.
            Returns basic bell rights object created via bdrxml. """
        obj = rights.make_rights()
        obj.category = u'COPYRIGHTED'
        obj = self._add_holder_data( obj )
        obj = self._add_rights_context( obj, u'repo_manager' )
        obj = self._add_rights_context( obj, u'bell_gallery' )
        obj = self._add_rights_context( obj, u'general_public' )
        obj.date = datetime.date.today()
        assert unicode(repr(type(obj))) == "<class 'bdrxml.rights.Rights'>", unicode(repr(type(obj)))
        return obj

    def _add_holder_data( self, obj ):
        """ Returns object with holder.name and holder.context_ids """
        obj.create_holder()
        obj.holder.name = u'Contact Bell Gallery for details: <http://www.brown.edu/campus-life/arts/bell-gallery/about/contact-information>'
        obj.holder.context_ids = u'rts001 rts002 rts003'
        return obj

    def _add_rights_context( self, obj, rights_holder ):
        """ Returns object with a rights-context """
        rc = rights.Context()
        rc.id = self.context_dict[rights_holder][u'id']
        rc.usertype = u'GROUP'
        rc.username = self.context_dict[rights_holder][u'username']
        rc.cclass = self.context_dict[rights_holder][u'cclass']
        rc.discover = self.context_dict[rights_holder][u'discover']
        rc.display = self.context_dict[rights_holder][u'display']
        rc.modify = self.context_dict[rights_holder][u'modify']
        rc.delete = self.context_dict[rights_holder][u'delete']
        obj.ctext.append( rc )
        return obj

    # end class RightsBuilder()

