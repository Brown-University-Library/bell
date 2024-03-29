""" Builds mods for item-api param. """
import json, logging, os
import eulxml
from bdrxml import irMetadata, mods, rels, rights
from lxml import etree
from lxml.etree import XMLSyntaxError


LOG_FILENAME = os.environ['BELL_LOG_FILENAME']
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG,
                    format='[%(asctime)s] %(levelname)s [%(module)s-%(funcName)s()::%(lineno)d] %(message)s',
                    datefmt='%d/%b/%Y %H:%M:%S',
                    filename=LOG_FILENAME)


class ModsBuilder:

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
            assert str(type(mods_object)) == "<class 'bdrxml.mods.Mods'>", str(type(mods_object))
            return { 'data': mods_object, 'accession_number': self.accession_number }

    ## HELPER FUNCTIONS ##

    def _initialize_mods( self, mods_id ):
        """ Initialize empty self.mods element; also sets namespace reference.
            Switching from previous straight etree.Element() build for better bdr compatibility going forward. """
        MODS_NAMESPACE = 'http://www.loc.gov/mods/v3'
        self.MODS = '{%s}' % MODS_NAMESPACE
        bdrxml_mods_obj = mods.make_mods()
        utf8_xml = bdrxml_mods_obj.serialize()
        self.mods = etree.fromstring( utf8_xml )
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
        self.__build_type_of_resource_stuff()
        return

    def __build_title_stuff( self, data_dict ):
        """ Adds to self.mods title_info and title.
            Called by _build_mods_element() """
        title_info = etree.SubElement( self.mods, self.MODS+'titleInfo' )
        title = etree.SubElement( title_info, self.MODS+'title' )
        title.text = data_dict[ 'object_title' ] or 'No title'

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

    def __build_origin_stuff( self, data_dict ):
        """ Adds to self.mods origin_info.
            Called by _build_mods_element() """
        origin_info = etree.SubElement( self.mods, self.MODS+'originInfo' )
        origin_info_dt_created_start = etree.SubElement( origin_info, self.MODS+'dateCreated', encoding='w3cdtf', point='start' )
        origin_info_dt_created_start.text = data_dict[ 'object_year_start' ]
        origin_info_dt_created_end = etree.SubElement( origin_info, self.MODS+'dateCreated', encoding='w3cdtf', point='end' )
        origin_info_dt_created_end.text = data_dict[ 'object_year_end' ]

    def __build_physical_stuff( self, data_dict ):
        """ Adds to self.mods physical_description, physical_description_form_material, physical_description_form_technique.
            Called by _build_mods_element() """
        physical_description = etree.SubElement( self.mods, self.MODS+'physicalDescription' )
        physical_description_form_material = etree.SubElement( physical_description, self.MODS+'form', type='material' )
        physical_description_form_material.text = data_dict[ 'object_medium' ]
        for entry in data_dict['MEDIA_SUB::sub_media_name']:
            physical_description_form_technique = etree.SubElement( physical_description, self.MODS+'form', type='technique' )
            physical_description_form_technique.text = entry

    def __build_note_stuff( self, data_dict ):
        """ Adds to self.mods note.
            Called by _build_mods_element() """
        note = etree.SubElement( self.mods, self.MODS+'note', type='provenance' )
        note.text = data_dict[ 'credit_line' ]

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

    def __build_identifier_stuff( self, data_dict ):
        """ Adds to self.mods identifiers 'bell_accession_number' and 'bell_object_id'.
            Populates self.accession_number
            Called by _build_mods_element() """
        identifier = etree.SubElement( self.mods, self.MODS+'identifier', type='bell_accession_number' )
        identifier.text = data_dict[ 'calc_accession_id' ]
        identifier2 = etree.SubElement( self.mods, self.MODS+'identifier', type='bell_object_id' )
        identifier2.text = data_dict[ 'object_id' ]
        self.accession_number = identifier.text  # will be used by _make...

    def __build_type_of_resource_stuff( self ):
        """ Adds to self.mods `typeOfResource`.
            Called by _build_mods_element() """
        t_o_r = etree.SubElement( self.mods, self.MODS+'typeOfResource' )
        t_o_r.text = 'still image'

    def _make_mods_xml_string( self ):
        """ Returns unicode xml string to pass to validator.
            String replacements in previous version no longer needed since new mods-initialization via bdrxml. """
        doc = etree.ElementTree( self.mods )
        mods_string = etree.tostring( doc, pretty_print=True ).decode( 'utf-8', 'replace' )
        assert type(mods_string) == str
        return mods_string

    def _validate_mods( self, mods_xml, mods_schema_path ):
        """ Validates mods_xml string. """
        # schema_object = self.__make_schema_object( mods_schema_path )
        # parser = etree.XMLParser( schema=schema_object )
        # try:
        #     doc = etree.fromstring( mods_xml, parser )
        # except XMLSyntaxError as e:
        #     message = '- in BellModsMaker._validate_mods(); error is, %s' % unicode(repr(e))
        #     logger.error( message )
        #     logger.error( 'problematic mods_xml, ```{}```'.format(mods_xml) )
        #     logger.error( 'mods_schema_path, ```{}```'.format(mods_schema_path) )
        #     raise Exception( message )
        return True

    def __make_schema_object( self, mods_schema_path ):
        """ Returns etree schema object.
            Called by _validate_mods(); separated out to debug intermittent schema-creation problem. """
        with open( mods_schema_path, 'r' ) as f:
            schema_string = f.read()
        schema_ustring = schema_string.decode( 'utf-8', 'replace' )
        logger.debug( 'schema_string, ```{}```'.format(schema_string) )
        schema_root = etree.XML( schema_ustring.encode( 'utf-8', 'replace') )  # "Unicode strings with encoding declaration are not supported."
        try:
            schema_object = etree.XMLSchema( schema_root )
        except Exception as e:
            message = '- in BellModsMaker.__make_schema_object(); mods_schema_path is, %s; schema_ustring is, %s; type(schema_root) is, %s; and error is, %s' % ( mods_schema_path, schema_ustring, repr(type(schema_root)), repr(e) )
            logger.error( message )
            raise Exception( message )
        logger.debug( 'schema object seems ok' )
        return schema_object

    # end class ModsBuilder()
