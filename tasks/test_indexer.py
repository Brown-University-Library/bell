# -*- coding: utf-8 -*-

import json, os, pprint, unittest
import indexer

""" To run tests:
    - activate v-env
    - cd into bell_code dir
    - All tests:
      python ./tasks/test_indexer.py
    - Single test:
      python ./tasks/test_indexer.py Indexer_Test.test__build_metadata_only_solr_dict """


class Indexer_Test(unittest.TestCase):
    """ Tests indexer.py code. """

    test_original_data = {
        u'pid': u'test_123',
        u'item_data': {
            u'ARTISTS::artist_alias': [None],
            u'ARTISTS::artist_birth_country_id': [u'231'],
            u'ARTISTS::artist_birth_year': [u'1937'],
            u'ARTISTS::artist_death_year': [None],
            u'ARTISTS::artist_first_name': [u'William'],
            u'ARTISTS::artist_last_name': [u'Wiley'],
            u'ARTISTS::artist_lifetime': [u'1937'],
            u'ARTISTS::artist_middle_name': [u'T.'],
            u'ARTISTS::artist_nationality_name': [u'American'],
            u'ARTISTS::calc_artist_full_name': [u'William T. Wiley'],
            u'ARTISTS::calc_nationality': [u'American'],
            u'ARTISTS::use_alias_flag': [None],
            u'MEDIA::object_medium_name': u'Book',
            u'MEDIA_SUB::sub_media_name': [u'Etching ', u'Letterpress'],
            u'OBJECT_ARTISTS::artist_id': [u'120'],
            u'OBJECT_ARTISTS::artist_role': [None],
            u'OBJECT_ARTISTS::primary_flag': [u'yes'],
            u'OBJECT_MEDIA_SUB::media_sub_id': [u'32', u'23'],
            u'SERIES::series_end_year': [None],
            u'SERIES::series_name': [None],
            u'SERIES::series_start_year': [None],
            u'calc_accession_id': u'B 1979.1204',
            u'credit_line': u"Gift of Richard M. Rieser, Jr. '65",
            u'image_height': None,
            u'image_width': None,
            u'object_date': u'1977',
            u'object_depth': None,
            u'object_height': None,
            u'object_id': u'197',
            u'object_image_scan_filename': None,
            u'object_medium': u'Letterpress and etching',
            u'object_title': u'Suite of Daze',
            u'object_width': None,
            u'object_year_end': u'1977',
            u'object_year_start': u'1977',
            u'series_id': None
            }
        }

    test_expected_metadata_only_result = {
        u'accession_number_original': u'B 1979.1204',
        u'author_birth_date': [u'1937'],
        u'author_date': [u'1937'],
        u'author_death_date': [u''],
        u'author_description': [u'American'],
        u'author_display': [u'William T. Wiley'],
        u'author_names_first': [u'William'],
        u'author_names_last': [u'Wiley'],
        u'author_names_middle': [u'T.'],
        u'image_height': u'',
        u'image_width': u'',
        u'jp2_image_url': u'',
        u'location_physical_location': u'Bell Art Gallery',
        u'location_shelf_locator': u'Book',
        u'master_image_url': u'',
        u'note_provenance': u"Gift of Richard M. Rieser, Jr. '65",
        u'object_date': u'1977',
        u'object_depth': u'',
        u'object_height': u'',
        u'object_title': u'Suite of Daze',
        u'object_width': u'',
        u'origin_datecreated_end': u'1977',
        u'origin_datecreated_start': u'1977',
        u'physical_description_extent': [u''],
        u'physical_description_material': [u'Letterpress and etching'],
        u'physical_description_technique': [u'Etching ', u'Letterpress']
        }

    def test__build_metadata_only_solr_dict(self):
        """ Tests solr_dict creation. """
        expected = self.test_expected_metadata_only_result
        result = indexer.build_metadata_only_solr_dict( self.test_original_data )
        self.assertEquals( expected, result )

    # end class Indexer_Test()



if __name__ == "__main__":
  unittest.TestCase.maxDiff = None  # allows error to show in long output
  unittest.main()
