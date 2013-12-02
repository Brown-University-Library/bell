# -*- coding: utf-8 -*-

import json, pprint
import logging.handlers
import lxml, requests
from django.utils.encoding import smart_unicode
from lxml import etree

''' Makes source bell json from filemaker pro xml. '''


# def setup_logger():
#   """ Configure logger to write to file. """
#   formatter = logging.Formatter( u'%(asctime)s - %(levelname)s - %(message)s' )
#   logger = logging.getLogger( u'parallel_logger' )
#   logger.setLevel( logging.DEBUG )
#   file_handler = logging.handlers.RotatingFileHandler( bell_settings.LOG_PATH_201310, maxBytes=16777216, backupCount=0 )
#   file_handler.setFormatter( formatter )
#   logger.addHandler( file_handler )
#   return logger


def convert_fmproxml_to_json(
  FMPRO_XML_URL, FMPRO_XML_FILENAME, JSON_OUTPUT_PATH, NAMESPACE ):
  ''' Controller: calls helper functions. '''
  unicode_xml_string = _get_data( FMPRO_XML_URL, FMPRO_XML_FILENAME )   # get data
  XML_DOC = _docify_xml( unicode_xml_string)                            # docify xml string
  dict_keys = _make_dict_keys( XML_DOC, NAMESPACE )                     # get dict keys
  xml_doc_rows = _get_xml_doc_rows( XML_DOC, NAMESPACE )                # get list of doc-items
  result_list = _process_rows( xml_doc_rows, NAMESPACE, dict_keys )     # process rows
  key_type_dict = _make_key_type_dict( result_list )                    # figures type for each field
  result_list = _normalize_value_types( key_type_dict, result_list )    # ensures values are consistently of type list or u-string
  _save_json( result_list, JSON_OUTPUT_PATH )                           # save json
  return


## helpers


def _get_data( FMPRO_XML_URL, FMPRO_XML_FILENAME ):
  ''' Returns original xml from github gist. '''
  # print u'- FMPRO_XML_URL, FMPRO_XML_FILENAME: %s, %s' % ( FMPRO_XML_URL, FMPRO_XML_FILENAME )
  r = requests.get( FMPRO_XML_URL )
  d = r.json()
  # print u'- d.keys()...'; pprint.pprint( d.keys() )
  # print u'- files.keys()...'; pprint.pprint( d[u'files'].keys() )
  unicode_data = d[u'files'][FMPRO_XML_FILENAME][u'content']
  assert type(unicode_data) == unicode
  return unicode_data


def _docify_xml( unicode_xml_string):
  ''' Returns xml-doc. '''
  byte_string = unicode_xml_string.encode(u'utf-8', u'replace')
  XML_DOC = etree.fromstring( byte_string )  # str required because xml contains an encoding declaration
  assert type(XML_DOC) == lxml.etree._Element, type(XML_DOC)
  return XML_DOC


def _make_dict_keys( XML_DOC, NAMESPACE ):
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


def _get_xml_doc_rows( XML_DOC, NAMESPACE ):
  ''' Returns list of item docs. '''
  assert type(XML_DOC) == lxml.etree._Element, type(XML_DOC)
  xpath = u'/default:FMPXMLRESULT/default:RESULTSET/default:ROW'
  rows = XML_DOC.xpath( xpath, namespaces=(NAMESPACE) )
  assert type(rows) == list, type(rows)
  sample_element = rows[0]
  assert type(sample_element) == lxml.etree._Element, type(sample_element)
  return rows


def _process_rows( xml_doc_rows, NAMESPACE, dict_keys ):
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


def _makeDataDict( columns, NAMESPACE, keys ):
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


def _make_key_type_dict( result_list ):
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


def _normalize_value_types( key_type_dict, result_list ):
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


def _save_json( result_list, JSON_OUTPUT_PATH ):
  ''' Saves the list of item-dicts to .json file. '''
  json_string = json.dumps( result_list, indent=2, sort_keys=True )
  assert type(json_string) == str, type(json_string)
  f = open( JSON_OUTPUT_PATH, u'w+' )
  f.write( json_string )
  f.close()
  return




if __name__ == u'__main__':

  from bdr_ingest_settings.tasks_app import BELL201310_settings as bell_settings
  convert_fmproxml_to_json(
    FMPRO_XML_URL = bell_settings.FMPRO_XML_URL,
    FMPRO_XML_FILENAME = bell_settings.FMPRO_XML_FILENAME,
    JSON_OUTPUT_PATH = bell_settings.JSON_OUTPUT_PATH,
    NAMESPACE = { u'default': u'http://www.filemaker.com/fmpxmlresult' }
    )
