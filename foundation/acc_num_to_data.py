# -*- coding: utf-8 -*-

import json, os, pprint

import lxml, requests
from lxml import etree


class SourceDictMaker( object ):
    """ Handles creation of an accession_number-to-item-info dict, saved as a json file.
        Purpose: This is one of three essential files that should exist before doing almost any bell processing.
                 It converts the raw filemaker-pro xml into json data for easy processing and viewing.
        if __name__... at bottom indicates how to run this script. """

    # def __init__( self ):
    #     self.NAMESPACE = { u'default': u'http://www.filemaker.com/fmpxmlresult' }

    def convert_fmproxml_to_json(
        self, FMPRO_XML_URL, FMPRO_XML_FILENAME, JSON_OUTPUT_PATH, NAMESPACE ):
        """ CONTROLLER
          Produces accession-number dict, and saves to a json file.
          Example: { accnum_1: {author:abc, title:def, etc.}, accnum_2:{etc.} } """
        unicode_xml_string = self._get_data( FMPRO_XML_URL, FMPRO_XML_FILENAME )   # get data
        XML_DOC = self._docify_xml( unicode_xml_string)                            # docify xml string
        dict_keys = self._make_dict_keys( XML_DOC, NAMESPACE )                     # get dict keys
        xml_doc_rows = self._get_xml_doc_rows( XML_DOC, NAMESPACE )                # get list of doc-items
        result_list = self._process_rows( xml_doc_rows, NAMESPACE, dict_keys )     # process rows
        key_type_dict = self._make_key_type_dict( result_list )                    # figures type for each field
        result_list = self._normalize_value_types( key_type_dict, result_list )    # ensures values are consistently of type list or u-string
        self._save_json( result_list, JSON_OUTPUT_PATH )                           # save json
        return

    ## helpers

    def _get_data( self, FMPRO_XML_URL, FMPRO_XML_FILENAME ):
        ''' Returns original xml from github gist. '''
        # print u'- FMPRO_XML_URL, FMPRO_XML_FILENAME: %s, %s' % ( FMPRO_XML_URL, FMPRO_XML_FILENAME )
        r = requests.get( FMPRO_XML_URL )
        d = r.json()
        # print u'- d.keys()...'; pprint.pprint( d.keys() )
        # print u'- files.keys()...'; pprint.pprint( d[u'files'].keys() )
        unicode_data = d[u'files'][FMPRO_XML_FILENAME][u'content']
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
        # XML_DOC = etree.fromstring( unicode_xml_string.encode(u'utf-8', u'replace') )  # str required because xml contains an encoding declaration
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
            Calls _make_data_dict helper. '''
        result_list = []
        for i,row in enumerate(xml_doc_rows):
          ## get columns (fixed number of columns per row)
          xpath = u'default:COL'
          columns = row.xpath( xpath, namespaces=(NAMESPACE) )
          assert len(columns) == 36, len(columns);   # was less before spring db revision
          ## get data_elements (variable number per column)
          item_dict = _makeDataDict( columns, NAMESPACE, dict_keys )
          result_list.append( item_dict )  # if i > 5: break
        return result_list

    def _makeDataDict( self, columns, NAMESPACE, keys ):
        ''' Returns info-dict for a single item; eg { u'artist_first_name': u'andy', u'artist_last_name': u'warhol' }
            Called by: _process_rows() '''
        ## helpers
        def __run_asserts():
          ''' Documents the inputs. '''
          assert type(columns) == list, type(columns)
          assert type(columns[0]) == lxml.etree._Element, type(columns[0])
          assert type(keys) == list, type(keys)
        def __handle_single_element( data, the_key ):
          ''' Stores either None or the single unicode value to the key. '''
          if data[0].text == None:
            d_dict[ the_key ] = None
          else:
            d_dict[ the_key ] = smart_unicode( data[0].text, u'utf-8', u'replace' )
          return
        def __handle_multiple_elements( data, the_key ):
          ''' Stores list of values to the key. '''
          d_list = []
          for data_element in data:
            d_list.append( smart_unicode( data_element.text, u'utf-8', u'replace' ) )
          d_dict[ the_key ] = d_list
          return
        ## work
        __run_asserts()
        xpath = u'default:DATA'; d_dict = {}  # setup
        for i,column in enumerate(columns):
          data = column.xpath( xpath, namespaces=(NAMESPACE) )  # type(data) always a list, but of an empty, a single or multiple elements?
          if len(data) == 0:  # no 'DATA' element in 'COL' element
            d_dict[ keys[i] ] = None
          elif len(data) == 1:
            __handle_single_element( data, keys[i] )
          else:
            __handle_multiple_elements( data, keys[i] )
        return d_dict

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
        # pprint.pprint( key_type_dict )

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

    def _save_json( self, result_list, JSON_OUTPUT_PATH ):
        ''' Saves the list of item-dicts to .json file. '''
        json_string = json.dumps( result_list, indent=2, sort_keys=True )
        assert type(json_string) == str, type(json_string)
        f = open( JSON_OUTPUT_PATH, u'w+' )
        f.write( json_string )
        f.close()
        return


    def _print_settings( self, FMPRO_XML_URL, FMPRO_XML_FILENAME, JSON_OUTPUT_PATH ):
        """ Outputs settings derived from environmental variables for developement. """
        print u'- FMPRO_XML_URL: %s' % FMPRO_XML_URL
        print u'- FMPRO_XML_FILENAME: %s' % FMPRO_XML_FILENAME
        print u'- JSON_OUTPUT_PATH: %s' % JSON_OUTPUT_PATH
        return

  # end class SourceDictMaker()




if __name__ == u'__main__':
    """ Assumes env is activated.
        ( 'ANTD' used as a namespace prefix for this 'acc_num_to_data.py' file. ) """
    pprint.pprint( os.environ.__dict__ )
    source_dict_maker = SourceDictMaker()
    source_dict_maker._print_settings(
        FMPRO_XML_URL=os.environ.get( u'BELL_ANTD__FMPRO_XML_URL', u'' ),
        FMPRO_XML_FILENAME=os.environ.get( u'BELL_ANTD__FMPRO_XML_FILENAME', u'' ),  # used to pull proper element from gist-api
        JSON_OUTPUT_PATH=os.environ.get( u'BELL_ANTD__JSON_OUTPUT_PATH', u'' ),
        )
    # source_dict_maker.convert_fmproxml_to_json(
    #     FMPRO_XML_URL=os.environ.get( u'BELL_ANTD__FMPRO_XML_URL', u'' ),
    #     FMPRO_XML_FILENAME=os.environ.get( u'BELL_ANTD__FMPRO_XML_FILENAME', u'' ),
    #     JSON_OUTPUT_PATH=os.environ.get( u'BELL_ANTD__JSON_OUTPUT_PATH', u'' ),
    #     )
