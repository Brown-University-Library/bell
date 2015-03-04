# -*- coding: utf-8 -*-

import datetime, json, os, pprint
import lxml, requests
from lxml import etree


class SourceDictMaker( object ):
    """ Handles creation of an accession_number-to-item-info dict, saved as a json file.
        Purpose: This is one of the essential files that should exist before doing almost any bell processing.
                 It converts the raw filemaker-pro xml into json data for easy processing and viewing.
        if __name__... at bottom indicates how to run this script. """

    def __init__( self ):
        self.NAMESPACE = { u'default': u'http://www.filemaker.com/fmpxmlresult' }

    def convert_fmproxml_to_json(
        self, FMPRO_XML_PATH, JSON_OUTPUT_PATH ):
        """ CONTROLLER
            Produces accession-number dict, and saves to a json file.
            Example: { count:5000,
                   #   datetime: 2013...,
                   #   items:{ accnum_1:{artist:abc, title:def}, accnum_2:{etc.}, etc. }
                   # } """
        #Get data
        #Purpose: gets raw filemaker-pro xml unicode-string from gist
        unicode_xml_string = self._get_data( FMPRO_XML_PATH )
        print u'- data grabbed'
        #
        #Docify xml string
        #Purpose: converts unicode-string to <type 'lxml.etree._Element'>
        XML_DOC = self._docify_xml( unicode_xml_string)
        print u'- data doc-ified'
        #
        #Make key list
        #Purpose: creates list of keys that will be used for each item-dict
        #Example returned data: [ u'object_id', u'object_title', u'object_date', etc. ]
        dict_keys = self._make_dict_keys( XML_DOC, self.NAMESPACE )
        print u'- list of keys created'
        #
        #Make list of doc-items
        #Purpose: creates list of xml-doc items
        xml_doc_rows = self._get_xml_doc_rows( XML_DOC, self.NAMESPACE )
        print u'- xml_doc_rows grabbed'
        #
        #Make initial dict-list
        #Purpose: creates initial list of dict-items. For a given key, the value-type may vary by item.
        #Example returned data: [ {u'artist_alias': u'abc', u'artist_birth_country_id': u'123', etc.}, {etc.}, ... ]
        result_list = self._process_rows( xml_doc_rows, self.NAMESPACE, dict_keys )
        print u'- initial result_list generated'
        #
        #Make key-type dict
        #Purpose: creats dict of key-name:key-type; all data examined to see which keys should have list vs unicode-string values.
        #Example returned data: [  {u'ARTISTS::calc_nationality': <type 'list'>, u'ARTISTS::use_alias_flag': <type 'unicode'>, etc.} ]
        key_type_dict = self._make_key_type_dict( result_list )
        print u'- key_type_dict created'
        #
        #Normalize dict-values
        #Purpose: creates final list of dict-items. For a given key, the value-type will _not_ vary by item.
        #Example returned data: [ {u'artist_alias': [u'abc'], u'artist_birth_country_id': [u'123'], etc.}, {etc.}, ... ]
        result_list = self._normalize_value_types( key_type_dict, result_list )
        print u'- final result_list generated'
        #
        #Dictify item-list
        #Purpose: creates accession-number to item-data-dict dictionary, adds count & datestamp
        #Example returned data: { count:5000,
                              #   items:{ accnum_1:{artist:abc, title:def}, accnum_2:{etc.}, etc. }
                              # }
        dictified_data = self._dictify_data( result_list )
        print u'- final data dictified'
        #
        #Output json
        self._save_json( dictified_data, JSON_OUTPUT_PATH )
        print u'- json saved; processing done'
        return

    def _get_data( self, FMPRO_XML_PATH ):
        """ Reads and returns source filemaker pro xml. """
        with open( FMPRO_XML_PATH ) as f:
          utf8_xml = f.read()
        unicode_data = utf8_xml.decode( u'utf-8' )
        assert type(unicode_data) == unicode
        return unicode_data

    def _docify_xml( self, unicode_xml_string):
        ''' Returns xml-doc. '''
        byte_string = unicode_xml_string.encode(u'utf-8', u'replace')
        XML_DOC = etree.fromstring( byte_string )  # str required because xml contains an encoding declaration
        assert type(XML_DOC) == lxml.etree._Element, type(XML_DOC)
        return XML_DOC

    def _make_dict_keys( self, XML_DOC, NAMESPACE ):
        ''' Returns list of field names; they'll later become keys in each item-dict. '''
        assert type(XML_DOC) == lxml.etree._Element, type(XML_DOC)
        xpath = u'/default:FMPXMLRESULT/default:METADATA/default:FIELD'
        elements = XML_DOC.xpath( xpath, namespaces=(NAMESPACE) )
        dict_keys = []
        for e in elements:
          dict_keys.append( e.attrib[u'NAME'].decode(u'utf-8', u'replace') )
        assert type(dict_keys) == list, type(dict_keys)
        return dict_keys

    def _get_xml_doc_rows( self, XML_DOC, NAMESPACE ):
        ''' Returns list of item docs. '''
        assert type(XML_DOC) == lxml.etree._Element, type(XML_DOC)
        xpath = u'/default:FMPXMLRESULT/default:RESULTSET/default:ROW'
        rows = XML_DOC.xpath( xpath, namespaces=(NAMESPACE) )
        assert type(rows) == list, type(rows)
        sample_element = rows[0]
        assert type(sample_element) == lxml.etree._Element, type(sample_element)
        return rows

    def _process_rows( self, xml_doc_rows, NAMESPACE, dict_keys ):
        ''' Returns list of item dictionaries.
            Calls _make_data_dict() helper. '''
        result_list = []
        for i,row in enumerate(xml_doc_rows):
          ## get columns (fixed number of columns per row)
          xpath = u'default:COL'
          columns = row.xpath( xpath, namespaces=(NAMESPACE) )
          assert len(columns) == 36, len(columns);   # was less before spring db revision
          ## get data_elements (variable number per column)
          item_dict = self._makeDataDict( columns, NAMESPACE, dict_keys )
          result_list.append( item_dict )  # if i > 5: break
        return result_list

    def _makeDataDict( self, columns, NAMESPACE, keys ):
        ''' Returns info-dict for a single item; eg { u'artist_first_name': u'andy', u'artist_last_name': u'warhol' }
            Called by: _process_rows()
            Calls: self.__run_asserts(), self.__handle_single_element(), self.__handle_multiple_elements() '''
        self.__run_asserts( columns, keys )
        xpath = u'default:DATA'; d_dict = {}  # setup
        for i,column in enumerate(columns):
            data = column.xpath( xpath, namespaces=(NAMESPACE) )  # type(data) always a list, but of an empty, a single or multiple elements?
            if len(data) == 0:    # eg <COL(for artist-firstname)></COL>
                d_dict[ keys[i] ] = None
            elif len(data) == 1:  # eg <COL(for artist-firstname)><DATA>'artist_firstname'</DATA></COL>
                d_dict[ keys[i] ] = self.__handle_single_element( data, keys[i] )
            else:                 # eg <COL(for artist-firstname)><DATA>'artist_a_firstname'</DATA><DATA>'artist_b_firstname'</DATA></COL>
                d_dict[ keys[i] ] = self.__handle_multiple_elements( data, keys[i] )
        return d_dict

    def __run_asserts( self, columns, keys ):
        ''' Documents the inputs.
            Called by _makeDataDict() '''
        assert type(columns) == list, type(columns)
        assert type(columns[0]) == lxml.etree._Element, type(columns[0])
        assert type(keys) == list, type(keys)
        return

    def __handle_single_element( self, data, the_key ):
        ''' Stores either None or the single unicode value to the key.
            Called by _makeDataDict() '''
        return_val = None
        if data[0].text:
            if type( data[0].text ) == unicode:
                return_val = data[0].text
            else:
                return_val = data[0].text.decode( u'utf-8', u'replace' )
        return return_val

    def __handle_multiple_elements( self, data, the_key ):
        ''' Stores list of unicode values to the key.
            Called by _makeDataDict() '''
        d_list = []
        for data_element in data:
            if data_element.text:
                if type( data_element.text ) == unicode:
                    d_list.append( data_element.text )
                else:
                    d_list.append( data_element.text.decode(u'utf-8', u'replace') )
            else:
                d_list.append( None )
        return d_list

    def _make_key_type_dict( self, result_list ):
        ''' Determines. '''
        key_type_dict = {}
        for entry_dict in result_list:
          for (key,value) in entry_dict.items():
            if not key in key_type_dict.keys():
              key_type_dict[key] = unicode
              # print u'key_type_dict now...'; pprint.pprint( key_type_dict )
            if type(value) == list and len(value) > 0:
              key_type_dict[key] = list
        return key_type_dict

    def _normalize_value_types( self, key_type_dict, result_list ):
        ''' Determines stable type for each field. '''
        updated_result_list = []
        assert len( key_type_dict.keys() ) == 36
        for entry_dict in result_list:
          assert len( entry_dict.keys() ) == 36
          for key, val in entry_dict.items():
            if key_type_dict[key] == list and ( type(val) == unicode or val == None ) :
              entry_dict[key] = [ val ]
          updated_result_list.append( entry_dict )
        return updated_result_list

    def _dictify_data( self, source_list ):
        """ Takes raw bell list of dict_data, returns accession-number dict. """
        accession_number_dict = {}
        for entry in source_list:
            # print u'- entry, `%s`' % entry
            # print u'-calc_accession_id, `%s`' % entry[u'calc_accession_id']
            if entry[u'calc_accession_id']:  # handles a null entry
                accession_number_dict[ entry[u'calc_accession_id'].strip() ] = entry
            else:
                print u'- entry, `%s`' % entry
        final_dict = {
          u'count': len( accession_number_dict.items() ),
          u'datetime': unicode( datetime.datetime.now() ),
          u'items': accession_number_dict }
        return final_dict

    def _save_json( self, result_list, JSON_OUTPUT_PATH ):
        ''' Saves the list of item-dicts to .json file. '''
        json_string = json.dumps( result_list, indent=2, sort_keys=True )
        assert type(json_string) == str, type(json_string)
        f = open( JSON_OUTPUT_PATH, u'w+' )
        f.write( json_string )
        f.close()
        return

    def _print_settings( self, FMPRO_XML_PATH, JSON_OUTPUT_PATH ):
        """ Outputs settings derived from environmental variables for development. """
        print u'- settings...'
        print u'- FMPRO_XML_PATH: %s' % FMPRO_XML_PATH
        print u'- JSON_OUTPUT_PATH: %s' % JSON_OUTPUT_PATH
        print u'---'
        return

  # end class SourceDictMaker()




if __name__ == u'__main__':
    """ Assumes env is activated.
        ( 'ANTD' used as a namespace prefix for this 'acc_num_to_data.py' file. ) """
    # pprint.pprint( os.environ.__dict__ )
    FMPRO_XML_PATH=os.environ[u'BELL_ANTD__FMPRO_XML_PATH']
    JSON_OUTPUT_PATH=os.environ[u'BELL_ANTD__JSON_OUTPUT_PATH']
    maker = SourceDictMaker()
    maker._print_settings(
        FMPRO_XML_PATH, JSON_OUTPUT_PATH )
    maker.convert_fmproxml_to_json(
        FMPRO_XML_PATH, JSON_OUTPUT_PATH )
