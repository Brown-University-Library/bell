# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import pprint, unittest
from acc_num_to_data import SourceDictMaker


class MakeSourceBellJson_Test(unittest.TestCase):

    def test__process_rows_a(self):
        ''' Tests conversion of xml into dict.
            Note, normalization of data-types happens later. '''
        maker = SourceDictMaker()
        NAMESPACE = { 'default': 'http://www.filemaker.com/fmpxmlresult' }
        XML_DOC = maker._docify_xml( self.TEST_FMPRO_XML_A )                      # docify xml string
        dict_keys = maker._make_dict_keys( XML_DOC, NAMESPACE )                 # get dict keys
        xml_doc_rows = maker._get_xml_doc_rows( XML_DOC, NAMESPACE )            # get list of doc-items
        result_list = maker._process_rows( xml_doc_rows, NAMESPACE, dict_keys ) # process rows
        expected = self.TEST_INITIAL_DICT_LIST_A
        self.assertEquals( expected, result_list )

    def test__process_rows_b(self):
        ''' Tests conversion of xml into dict.
            This xml contains multiple empty data elements.
            Note, normalization of data-types happens later. '''
        maker = SourceDictMaker()
        NAMESPACE = { 'default': 'http://www.filemaker.com/fmpxmlresult' }
        XML_DOC = maker._docify_xml( self.TEST_FMPRO_XML_B )                      # docify xml string
        dict_keys = maker._make_dict_keys( XML_DOC, NAMESPACE )                 # get dict keys
        xml_doc_rows = maker._get_xml_doc_rows( XML_DOC, NAMESPACE )            # get list of doc-items
        result_list = maker._process_rows( xml_doc_rows, NAMESPACE, dict_keys ) # process rows
        expected = self.TEST_INITIAL_DICT_LIST_B
        self.assertEquals( expected, result_list )

  ## TODO: ensure set of single-author entries still yields author list info vs string info
  # def test__normalize_value_types(self):
  #   ''' Tests normalization of dict-entries.
  #       Example: single author entries converted from string to list. '''
  #   key_type_dict = msbj._make_key_type_dict( self.TEST_INITIAL_DICT_LIST )
  #   result = msbj._normalize_value_types( key_type_dict, self.TEST_INITIAL_DICT_LIST )
  #   pprint.pprint( result )
  #   self.assertEquals( 'z', result )

  ## class test data ##

    TEST_FMPRO_XML_A = '''<?xml version="1.0" ?>
<FMPXMLRESULT xmlns="http://www.filemaker.com/fmpxmlresult">
  <ERRORCODE>0</ERRORCODE>
  <PRODUCT BUILD="03-21-2013" NAME="FileMaker" VERSION="Pro 12.0v4"/>
  <DATABASE DATEFORMAT="M/d/yyyy" LAYOUT="" NAME="Bell_Gallery_Collections.fmp12" RECORDS="5829" TIMEFORMAT="h:mm:ss a"/>
  <METADATA>
    <FIELD EMPTYOK="NO" MAXREPEAT="1" NAME="object_id" TYPE="NUMBER"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="object_title" TYPE="TEXT"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="object_date" TYPE="TEXT"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="object_medium" TYPE="TEXT"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="object_width" TYPE="NUMBER"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="object_height" TYPE="NUMBER"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="object_depth" TYPE="NUMBER"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="image_height" TYPE="NUMBER"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="image_width" TYPE="NUMBER"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="credit_line" TYPE="TEXT"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="object_year_start" TYPE="NUMBER"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="object_year_end" TYPE="NUMBER"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="calc_accession_id" TYPE="TEXT"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="MEDIA::object_medium_name" TYPE="TEXT"/>
    <FIELD EMPTYOK="NO" MAXREPEAT="1" NAME="OBJECT_ARTISTS::artist_id" TYPE="NUMBER"/>
    <FIELD EMPTYOK="NO" MAXREPEAT="1" NAME="ARTISTS::use_alias_flag" TYPE="TEXT"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="ARTISTS::artist_first_name" TYPE="TEXT"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="ARTISTS::artist_last_name" TYPE="TEXT"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="ARTISTS::artist_middle_name" TYPE="TEXT"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="ARTISTS::artist_alias" TYPE="TEXT"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="ARTISTS::calc_artist_full_name" TYPE="TEXT"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="ARTISTS::artist_nationality_name" TYPE="TEXT"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="ARTISTS::artist_birth_country_id" TYPE="NUMBER"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="ARTISTS::artist_birth_year" TYPE="NUMBER"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="ARTISTS::artist_death_year" TYPE="NUMBER"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="ARTISTS::artist_lifetime" TYPE="TEXT"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="ARTISTS::calc_nationality" TYPE="TEXT"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="series_id" TYPE="NUMBER"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="SERIES::series_name" TYPE="TEXT"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="SERIES::series_start_year" TYPE="NUMBER"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="SERIES::series_end_year" TYPE="NUMBER"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="OBJECT_MEDIA_SUB::media_sub_id" TYPE="NUMBER"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="MEDIA_SUB::sub_media_name" TYPE="TEXT"/>
    <FIELD EMPTYOK="NO" MAXREPEAT="1" NAME="OBJECT_ARTISTS::primary_flag" TYPE="TEXT"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="OBJECT_ARTISTS::artist_role" TYPE="TEXT"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="object_image_scan_filename" TYPE="TEXT"/>
  </METADATA>
  <RESULTSET FOUND="5829">
    <ROW MODID="25" RECORDID="312">
      <COL>
        <DATA>176</DATA>
      </COL>
      <COL>
        <DATA>A Glimpse of Thomas Traherne by Thomas Traherne</DATA>
      </COL>
      <COL>
        <DATA>1978</DATA>
      </COL>
      <COL>
        <DATA>Letterpress and lithography</DATA>
      </COL>
      <COL>
        <DATA/>
      </COL>
      <COL>
        <DATA/>
      </COL>
      <COL>
        <DATA/>
      </COL>
      <COL>
        <DATA/>
      </COL>
      <COL>
        <DATA/>
      </COL>
      <COL>
        <DATA>Gift of Saul P. Steinberg</DATA>
      </COL>
      <COL>
        <DATA>1978</DATA>
      </COL>
      <COL>
        <DATA>1978</DATA>
      </COL>
      <COL>
        <DATA>B 1980.1566</DATA>
      </COL>
      <COL>
        <DATA>Book</DATA>
      </COL>
      <COL>
        <DATA>223</DATA>
      </COL>
      <COL>
        <DATA/>
      </COL>
      <COL>
        <DATA>Berenice</DATA>
      </COL>
      <COL>
        <DATA>Abbott</DATA>
      </COL>
      <COL>
        <DATA/>
      </COL>
      <COL>
        <DATA/>
      </COL>
      <COL>
        <DATA>Berenice Abbott</DATA>
      </COL>
      <COL>
        <DATA>American</DATA>
      </COL>
      <COL>
        <DATA>231</DATA>
      </COL>
      <COL>
        <DATA>1898</DATA>
      </COL>
      <COL>
        <DATA>1991</DATA>
      </COL>
      <COL>
        <DATA>1898-1991</DATA>
      </COL>
      <COL>
        <DATA>American</DATA>
      </COL>
      <COL>
        <DATA/>
      </COL>
      <COL/>
      <COL/>
      <COL/>
      <COL>
        <DATA>32</DATA>
        <DATA>35</DATA>
      </COL>
      <COL>
        <DATA>Letterpress</DATA>
        <DATA>Lithograph </DATA>
      </COL>
      <COL>
        <DATA>yes</DATA>
      </COL>
      <COL>
        <DATA/>
      </COL>
      <COL>
        <DATA/>
      </COL>
    </ROW>
  </RESULTSET>
</FMPXMLRESULT>'''

    TEST_INITIAL_DICT_LIST_A = [ {  # it's ok that author entries are strings instead of lists at this point
    'ARTISTS::artist_alias': None,
    'ARTISTS::artist_birth_country_id': '231',
    'ARTISTS::artist_birth_year': '1898',
    'ARTISTS::artist_death_year': '1991',
    'ARTISTS::artist_first_name': 'Berenice',
    'ARTISTS::artist_last_name': 'Abbott',
    'ARTISTS::artist_lifetime': '1898-1991',
    'ARTISTS::artist_middle_name': None,
    'ARTISTS::artist_nationality_name': 'American',
    'ARTISTS::calc_artist_full_name': 'Berenice Abbott',
    'ARTISTS::calc_nationality': 'American',
    'ARTISTS::use_alias_flag': None,
    'MEDIA::object_medium_name': 'Book',
    'MEDIA_SUB::sub_media_name': ['Letterpress', 'Lithograph '],
    'OBJECT_ARTISTS::artist_id': '223',
    'OBJECT_ARTISTS::artist_role': None,
    'OBJECT_ARTISTS::primary_flag': 'yes',
    'OBJECT_MEDIA_SUB::media_sub_id': ['32', '35'],
    'SERIES::series_end_year': None,
    'SERIES::series_name': None,
    'SERIES::series_start_year': None,
    'calc_accession_id': 'B 1980.1566',
    'credit_line': 'Gift of Saul P. Steinberg',
    'image_height': None,
    'image_width': None,
    'object_date': '1978',
    'object_depth': None,
    'object_height': None,
    'object_id': '176',
    'object_image_scan_filename': None,
    'object_medium': 'Letterpress and lithography',
    'object_title': 'A Glimpse of Thomas Traherne by Thomas Traherne',
    'object_width': None,
    'object_year_end': '1978',
    'object_year_start': '1978',
    'series_id': None } ]


    TEST_FMPRO_XML_B = '''<?xml version="1.0" ?>
<FMPXMLRESULT xmlns="http://www.filemaker.com/fmpxmlresult">
  <ERRORCODE>0</ERRORCODE>
  <PRODUCT BUILD="03-21-2013" NAME="FileMaker" VERSION="Pro 12.0v4"/>
  <DATABASE DATEFORMAT="M/d/yyyy" LAYOUT="" NAME="Bell_Gallery_Collections.fmp12" RECORDS="5829" TIMEFORMAT="h:mm:ss a"/>
  <METADATA>
    <FIELD EMPTYOK="NO" MAXREPEAT="1" NAME="object_id" TYPE="NUMBER"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="object_title" TYPE="TEXT"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="object_date" TYPE="TEXT"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="object_medium" TYPE="TEXT"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="object_width" TYPE="NUMBER"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="object_height" TYPE="NUMBER"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="object_depth" TYPE="NUMBER"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="image_height" TYPE="NUMBER"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="image_width" TYPE="NUMBER"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="credit_line" TYPE="TEXT"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="object_year_start" TYPE="NUMBER"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="object_year_end" TYPE="NUMBER"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="calc_accession_id" TYPE="TEXT"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="MEDIA::object_medium_name" TYPE="TEXT"/>
    <FIELD EMPTYOK="NO" MAXREPEAT="1" NAME="OBJECT_ARTISTS::artist_id" TYPE="NUMBER"/>
    <FIELD EMPTYOK="NO" MAXREPEAT="1" NAME="ARTISTS::use_alias_flag" TYPE="TEXT"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="ARTISTS::artist_first_name" TYPE="TEXT"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="ARTISTS::artist_last_name" TYPE="TEXT"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="ARTISTS::artist_middle_name" TYPE="TEXT"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="ARTISTS::artist_alias" TYPE="TEXT"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="ARTISTS::calc_artist_full_name" TYPE="TEXT"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="ARTISTS::artist_nationality_name" TYPE="TEXT"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="ARTISTS::artist_birth_country_id" TYPE="NUMBER"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="ARTISTS::artist_birth_year" TYPE="NUMBER"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="ARTISTS::artist_death_year" TYPE="NUMBER"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="ARTISTS::artist_lifetime" TYPE="TEXT"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="ARTISTS::calc_nationality" TYPE="TEXT"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="series_id" TYPE="NUMBER"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="SERIES::series_name" TYPE="TEXT"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="SERIES::series_start_year" TYPE="NUMBER"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="SERIES::series_end_year" TYPE="NUMBER"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="OBJECT_MEDIA_SUB::media_sub_id" TYPE="NUMBER"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="MEDIA_SUB::sub_media_name" TYPE="TEXT"/>
    <FIELD EMPTYOK="NO" MAXREPEAT="1" NAME="OBJECT_ARTISTS::primary_flag" TYPE="TEXT"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="OBJECT_ARTISTS::artist_role" TYPE="TEXT"/>
    <FIELD EMPTYOK="YES" MAXREPEAT="1" NAME="object_image_scan_filename" TYPE="TEXT"/>
  </METADATA>
  <RESULTSET FOUND="5829">
    <ROW MODID="16" RECORDID="314">
      <COL>
        <DATA>178</DATA>
      </COL>
      <COL>
        <DATA>Red Bird</DATA>
      </COL>
      <COL>
        <DATA>1979</DATA>
      </COL>
      <COL>
        <DATA>Letterpress and screenprint</DATA>
      </COL>
      <COL>
        <DATA/>
      </COL>
      <COL>
        <DATA/>
      </COL>
      <COL>
        <DATA/>
      </COL>
      <COL>
        <DATA/>
      </COL>
      <COL>
        <DATA/>
      </COL>
      <COL>
        <DATA>Gift of Saul P. Steinberg</DATA>
      </COL>
      <COL>
        <DATA>1979</DATA>
      </COL>
      <COL>
        <DATA>1979</DATA>
      </COL>
      <COL>
        <DATA>B 1980.1570</DATA>
      </COL>
      <COL>
        <DATA>Book</DATA>
      </COL>
      <COL>
        <DATA>466</DATA>
        <DATA>122</DATA>
      </COL>
      <COL>
        <DATA/>
        <DATA/>
      </COL>
      <COL>
        <DATA>Christopher</DATA>
        <DATA>John</DATA>
      </COL>
      <COL>
        <DATA>Logue</DATA>
        <DATA>Christie</DATA>
      </COL>
      <COL>
        <DATA/>
        <DATA/>
      </COL>
      <COL>
        <DATA/>
        <DATA/>
      </COL>
      <COL>
        <DATA>Christopher Logue</DATA>
        <DATA>John Christie</DATA>
      </COL>
      <COL>
        <DATA/>
        <DATA>British</DATA>
      </COL>
      <COL>
        <DATA/>
        <DATA/>
      </COL>
      <COL>
        <DATA/>
        <DATA>1945</DATA>
      </COL>
      <COL>
        <DATA/>
        <DATA/>
      </COL>
      <COL>
        <DATA/>
        <DATA>1945</DATA>
      </COL>
      <COL>
        <DATA/>
        <DATA>British</DATA>
      </COL>
      <COL>
        <DATA/>
      </COL>
      <COL/>
      <COL/>
      <COL/>
      <COL>
        <DATA>32</DATA>
        <DATA>52</DATA>
      </COL>
      <COL>
        <DATA>Letterpress</DATA>
        <DATA>Screenprint</DATA>
      </COL>
      <COL>
        <DATA>yes</DATA>
        <DATA>no</DATA>
      </COL>
      <COL>
        <DATA/>
        <DATA/>
      </COL>
      <COL>
        <DATA/>
      </COL>
    </ROW>
  </RESULTSET>
</FMPXMLRESULT>'''

    TEST_INITIAL_DICT_LIST_B = [{'ARTISTS::artist_alias': [None, None],
  'ARTISTS::artist_birth_country_id': [None, None],
  'ARTISTS::artist_birth_year': [None, '1945'],
  'ARTISTS::artist_death_year': [None, None],
  'ARTISTS::artist_first_name': ['Christopher', 'John'],
  'ARTISTS::artist_last_name': ['Logue', 'Christie'],
  'ARTISTS::artist_lifetime': [None, '1945'],
  'ARTISTS::artist_middle_name': [None, None],
  'ARTISTS::artist_nationality_name': [None, 'British'],
  'ARTISTS::calc_artist_full_name': ['Christopher Logue', 'John Christie'],
  'ARTISTS::calc_nationality': [None, 'British'],
  'ARTISTS::use_alias_flag': [None, None],
  'MEDIA::object_medium_name': 'Book',
  'MEDIA_SUB::sub_media_name': ['Letterpress', 'Screenprint'],
  'OBJECT_ARTISTS::artist_id': ['466', '122'],
  'OBJECT_ARTISTS::artist_role': [None, None],
  'OBJECT_ARTISTS::primary_flag': ['yes', 'no'],
  'OBJECT_MEDIA_SUB::media_sub_id': ['32', '52'],
  'SERIES::series_end_year': None,
  'SERIES::series_name': None,
  'SERIES::series_start_year': None,
  'calc_accession_id': 'B 1980.1570',
  'credit_line': 'Gift of Saul P. Steinberg',
  'image_height': None,
  'image_width': None,
  'object_date': '1979',
  'object_depth': None,
  'object_height': None,
  'object_id': '178',
  'object_image_scan_filename': None,
  'object_medium': 'Letterpress and screenprint',
  'object_title': 'Red Bird',
  'object_width': None,
  'object_year_end': '1979',
  'object_year_start': '1979',
  'series_id': None}]

  # end class MakeSourceBellJson_Test()




if __name__ == "__main__":
    unittest.TestCase.maxDiff = None  # allows error to show in long output
    unittest.main()
