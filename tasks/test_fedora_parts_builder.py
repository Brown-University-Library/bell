# -*- coding: utf-8 -*-

import json, os, pprint, unittest
from fedora_parts_builder import ModsBuilder

""" To run tests:
    - activate v-env
    - cd into bell_code dir
    - All tests:
      python ./bell_code/tasks/test_fedora_parts_builder.py
    - Single test:
      python ./bell_code/tasks/test_fedora_parts_builder.py MakeModsFromBellJson_Test.test__mods_single_artist """


class MakeModsFromBellJson_Test(unittest.TestCase):

  """ Tests BellModsMaker.direct_mods_creation(). """

  def test__mods_single_artist(self):
    """ Tests bell mods creation. """
    data_dict = json.loads( self.TEST_BELL_JSON_ENTRY_SINGLE_ARTIST )
    mods_schema_path = os.path.abspath( u'./lib/mods-3-4.xsd' )
    expected = self.TEST_EXPECTED_SINGLE_ARTIST_XML
    mods_maker = ModsBuilder()
    result = mods_maker.build_mods_object( data_dict, mods_schema_path, u'return_string' )
    self.assertEquals( expected, result['data'] )

  def test__mods_multiple_artists(self):
    """ Tests bell mods creation. """
    data_dict = json.loads( self.TEST_BELL_JSON_ENTRY_MULTIPLE_ARTISTS )
    mods_schema_path = os.path.abspath( u'./lib/mods-3-4.xsd' )
    expected = self.TEST_EXPECTED_MULTIPLE_ARTISTS_XML
    mods_maker = ModsBuilder()
    result = mods_maker.build_mods_object( data_dict, mods_schema_path, u'return_string' )
    self.assertEquals( expected, result[u'data'] )

  ## end of tests ##

  ## test data ##

  TEST_BELL_JSON_ENTRY_SINGLE_ARTIST = u'''
  {
    "ARTISTS::artist_alias": [
      null
    ],
    "ARTISTS::artist_birth_country_id": [
      "231"
    ],
    "ARTISTS::artist_birth_year": [
      "1898"
    ],
    "ARTISTS::artist_death_year": [
      "1991"
    ],
    "ARTISTS::artist_first_name": [
      "Berenice"
    ],
    "ARTISTS::artist_last_name": [
      "Abbott"
    ],
    "ARTISTS::artist_lifetime": [
      "1898-1991"
    ],
    "ARTISTS::artist_middle_name": [
      null
    ],
    "ARTISTS::artist_nationality_name": [
      "American"
    ],
    "ARTISTS::calc_artist_full_name": [
      "Berenice Abbott"
    ],
    "ARTISTS::calc_nationality": [
      "American"
    ],
    "ARTISTS::use_alias_flag": [
      null
    ],
    "MEDIA::object_medium_name": "Book",
    "MEDIA_SUB::sub_media_name": [
      "Letterpress",
      "Lithograph "
    ],
    "OBJECT_ARTISTS::artist_id": [
      "223"
    ],
    "OBJECT_ARTISTS::artist_role": [
      null
    ],
    "OBJECT_ARTISTS::primary_flag": [
      "yes"
    ],
    "OBJECT_MEDIA_SUB::media_sub_id": [
      "32",
      "35"
    ],
    "SERIES::series_end_year": [
      null
    ],
    "SERIES::series_name": [
      null
    ],
    "SERIES::series_start_year": [
      null
    ],
    "calc_accession_id": "B 1980.1566",
    "credit_line": "Gift of Saul P. Steinberg",
    "image_height": null,
    "image_width": null,
    "object_date": "1978",
    "object_depth": null,
    "object_height": null,
    "object_id": "176",
    "object_image_scan_filename": null,
    "object_medium": "Letterpress and lithography",
    "object_title": "A Glimpse of Thomas Traherne by Thomas Traherne",
    "object_width": null,
    "object_year_end": "1978",
    "object_year_start": "1978",
    "series_id": null
  }'''

  TEST_EXPECTED_SINGLE_ARTIST_XML = u'''<mods:mods xmlns:mods="http://www.loc.gov/mods/v3" ID="B1980.1566" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.loc.gov/mods/v3 http://www.loc.gov/standards/mods/futures/mods-3-4.xsd">
  <mods:titleInfo>
    <mods:title>A Glimpse of Thomas Traherne by Thomas Traherne</mods:title>
  </mods:titleInfo>
  <mods:name type="personal">
    <mods:namePart>Abbott, Berenice</mods:namePart>
    <mods:displayForm>Berenice Abbott</mods:displayForm>
    <mods:namePart type="date">1898-1991</mods:namePart>
    <mods:role>
      <mods:roleTerm type="text">artist</mods:roleTerm>
    </mods:role>
  </mods:name>
  <mods:originInfo>
    <mods:dateCreated encoding="w3cdtf" point="start">1978</mods:dateCreated>
    <mods:dateCreated encoding="w3cdtf" point="end">1978</mods:dateCreated>
  </mods:originInfo>
  <mods:physicalDescription>
    <mods:form type="material">Letterpress and lithography</mods:form>
    <mods:form type="technique">Letterpress</mods:form>
    <mods:form type="technique">Lithograph </mods:form>
  </mods:physicalDescription>
  <mods:note type="provenance">Gift of Saul P. Steinberg</mods:note>
  <mods:location>
    <mods:physicalLocation>Bell Art Gallery</mods:physicalLocation>
    <mods:holdingSimple>
      <mods:copyInformation>
        <mods:shelfLocator>Book</mods:shelfLocator>
      </mods:copyInformation>
    </mods:holdingSimple>
  </mods:location>
  <mods:identifier type="bell_accession_number">B 1980.1566</mods:identifier>
  <mods:identifier type="bell_object_id">176</mods:identifier>
</mods:mods>
'''

  TEST_BELL_JSON_ENTRY_MULTIPLE_ARTISTS = u'''
  {
    "ARTISTS::artist_alias": [
      null,
      null
    ],
    "ARTISTS::artist_birth_country_id": [
      null,
      null
    ],
    "ARTISTS::artist_birth_year": [
      "1933",
      null
    ],
    "ARTISTS::artist_death_year": [
      null,
      null
    ],
    "ARTISTS::artist_first_name": [
      "Ian",
      "Kevin"
    ],
    "ARTISTS::artist_last_name": [
      "Tyson",
      "Power"
    ],
    "ARTISTS::artist_lifetime": [
      "1933",
      null
    ],
    "ARTISTS::artist_middle_name": [
      null,
      null
    ],
    "ARTISTS::artist_nationality_name": [
      "English",
      null
    ],
    "ARTISTS::calc_artist_full_name": [
      "Ian Tyson",
      "Kevin Power"
    ],
    "ARTISTS::calc_nationality": [
      "English",
      null
    ],
    "ARTISTS::use_alias_flag": [
      null,
      null
    ],
    "MEDIA::object_medium_name": "Book",
    "MEDIA_SUB::sub_media_name": [
      "Embossing ",
      "Letterpress"
    ],
    "OBJECT_ARTISTS::artist_id": [
      "119",
      "129"
    ],
    "OBJECT_ARTISTS::artist_role": [
      null,
      null
    ],
    "OBJECT_ARTISTS::primary_flag": [
      "yes",
      "no"
    ],
    "OBJECT_MEDIA_SUB::media_sub_id": [
      "32",
      "20"
    ],
    "SERIES::series_end_year": [
      null
    ],
    "SERIES::series_name": [
      null
    ],
    "SERIES::series_start_year": [
      null
    ],
    "calc_accession_id": "B 1980.1551",
    "credit_line": "Gift of Saul P. Steinberg",
    "image_height": null,
    "image_width": null,
    "object_date": "1979",
    "object_depth": null,
    "object_height": "15.625",
    "object_id": "196",
    "object_image_scan_filename": null,
    "object_medium": "Letterpress and embossing",
    "object_title": "De Morandi",
    "object_width": "11.875",
    "object_year_end": "1979",
    "object_year_start": "1979",
    "series_id": null
  }'''

  TEST_EXPECTED_MULTIPLE_ARTISTS_XML = u'''<mods:mods xmlns:mods="http://www.loc.gov/mods/v3" ID="B1980.1551" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.loc.gov/mods/v3 http://www.loc.gov/standards/mods/futures/mods-3-4.xsd">
  <mods:titleInfo>
    <mods:title>De Morandi</mods:title>
  </mods:titleInfo>
  <mods:name type="personal">
    <mods:namePart>Tyson, Ian</mods:namePart>
    <mods:displayForm>Ian Tyson</mods:displayForm>
    <mods:namePart type="date">1933</mods:namePart>
    <mods:role>
      <mods:roleTerm type="text">artist</mods:roleTerm>
    </mods:role>
  </mods:name>
  <mods:name type="personal">
    <mods:namePart>Power, Kevin</mods:namePart>
    <mods:displayForm>Kevin Power</mods:displayForm>
    <mods:namePart type="date"/>
    <mods:role>
      <mods:roleTerm type="text">artist</mods:roleTerm>
    </mods:role>
  </mods:name>
  <mods:originInfo>
    <mods:dateCreated encoding="w3cdtf" point="start">1979</mods:dateCreated>
    <mods:dateCreated encoding="w3cdtf" point="end">1979</mods:dateCreated>
  </mods:originInfo>
  <mods:physicalDescription>
    <mods:form type="material">Letterpress and embossing</mods:form>
    <mods:form type="technique">Embossing </mods:form>
    <mods:form type="technique">Letterpress</mods:form>
  </mods:physicalDescription>
  <mods:note type="provenance">Gift of Saul P. Steinberg</mods:note>
  <mods:location>
    <mods:physicalLocation>Bell Art Gallery</mods:physicalLocation>
    <mods:holdingSimple>
      <mods:copyInformation>
        <mods:shelfLocator>Book</mods:shelfLocator>
      </mods:copyInformation>
    </mods:holdingSimple>
  </mods:location>
  <mods:identifier type="bell_accession_number">B 1980.1551</mods:identifier>
  <mods:identifier type="bell_object_id">196</mods:identifier>
</mods:mods>
'''

  # end class MakeModsFromBellJson_Test()




if __name__ == "__main__":
  unittest.TestCase.maxDiff = None  # allows error to show in long output
  unittest.main()
