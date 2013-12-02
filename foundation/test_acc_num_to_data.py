# -*- coding: utf-8 -*-

import pprint, unittest
from acc_num_to_data import SourceDictMaker


class MakeSourceBellJson_Test(unittest.TestCase):

    def test__process_rows_a(self):
        ''' Tests conversion of xml into dict.
            Note, normalization of data-types happens later. '''
        maker = SourceDictMaker()
        NAMESPACE = { u'default': u'http://www.filemaker.com/fmpxmlresult' }
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
        NAMESPACE = { u'default': u'http://www.filemaker.com/fmpxmlresult' }
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
  #   self.assertEquals( u'z', result )

  ## class test data ##

    TEST_FMPRO_XML_A = u'''<?xml version="1.0" ?>
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
    u'ARTISTS::artist_alias': None,
    u'ARTISTS::artist_birth_country_id': u'231',
    u'ARTISTS::artist_birth_year': u'1898',
    u'ARTISTS::artist_death_year': u'1991',
    u'ARTISTS::artist_first_name': u'Berenice',
    u'ARTISTS::artist_last_name': u'Abbott',
    u'ARTISTS::artist_lifetime': u'1898-1991',
    u'ARTISTS::artist_middle_name': None,
    u'ARTISTS::artist_nationality_name': u'American',
    u'ARTISTS::calc_artist_full_name': u'Berenice Abbott',
    u'ARTISTS::calc_nationality': u'American',
    u'ARTISTS::use_alias_flag': None,
    u'MEDIA::object_medium_name': u'Book',
    u'MEDIA_SUB::sub_media_name': [u'Letterpress', u'Lithograph '],
    u'OBJECT_ARTISTS::artist_id': u'223',
    u'OBJECT_ARTISTS::artist_role': None,
    u'OBJECT_ARTISTS::primary_flag': u'yes',
    u'OBJECT_MEDIA_SUB::media_sub_id': [u'32', u'35'],
    u'SERIES::series_end_year': None,
    u'SERIES::series_name': None,
    u'SERIES::series_start_year': None,
    u'calc_accession_id': u'B 1980.1566',
    u'credit_line': u'Gift of Saul P. Steinberg',
    u'image_height': None,
    u'image_width': None,
    u'object_date': u'1978',
    u'object_depth': None,
    u'object_height': None,
    u'object_id': u'176',
    u'object_image_scan_filename': None,
    u'object_medium': u'Letterpress and lithography',
    u'object_title': u'A Glimpse of Thomas Traherne by Thomas Traherne',
    u'object_width': None,
    u'object_year_end': u'1978',
    u'object_year_start': u'1978',
    u'series_id': None } ]


    TEST_FMPRO_XML_B = u'''<?xml version="1.0" ?>
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

    TEST_INITIAL_DICT_LIST_B = [{u'ARTISTS::artist_alias': [None, None],
  u'ARTISTS::artist_birth_country_id': [None, None],
  u'ARTISTS::artist_birth_year': [None, u'1945'],
  u'ARTISTS::artist_death_year': [None, None],
  u'ARTISTS::artist_first_name': [u'Christopher', u'John'],
  u'ARTISTS::artist_last_name': [u'Logue', u'Christie'],
  u'ARTISTS::artist_lifetime': [None, u'1945'],
  u'ARTISTS::artist_middle_name': [None, None],
  u'ARTISTS::artist_nationality_name': [None, u'British'],
  u'ARTISTS::calc_artist_full_name': [u'Christopher Logue', u'John Christie'],
  u'ARTISTS::calc_nationality': [None, u'British'],
  u'ARTISTS::use_alias_flag': [None, None],
  u'MEDIA::object_medium_name': u'Book',
  u'MEDIA_SUB::sub_media_name': [u'Letterpress', u'Screenprint'],
  u'OBJECT_ARTISTS::artist_id': [u'466', u'122'],
  u'OBJECT_ARTISTS::artist_role': [None, None],
  u'OBJECT_ARTISTS::primary_flag': [u'yes', u'no'],
  u'OBJECT_MEDIA_SUB::media_sub_id': [u'32', u'52'],
  u'SERIES::series_end_year': None,
  u'SERIES::series_name': None,
  u'SERIES::series_start_year': None,
  u'calc_accession_id': u'B 1980.1570',
  u'credit_line': u'Gift of Saul P. Steinberg',
  u'image_height': None,
  u'image_width': None,
  u'object_date': u'1979',
  u'object_depth': None,
  u'object_height': None,
  u'object_id': u'178',
  u'object_image_scan_filename': None,
  u'object_medium': u'Letterpress and screenprint',
  u'object_title': u'Red Bird',
  u'object_width': None,
  u'object_year_end': u'1979',
  u'object_year_start': u'1979',
  u'series_id': None}]

  # end class MakeSourceBellJson_Test()




if __name__ == "__main__":
    unittest.TestCase.maxDiff = None  # allows error to show in long output
    unittest.main()
