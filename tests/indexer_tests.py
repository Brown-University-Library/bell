# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import json, os, pprint, unittest
from bell_code.tasks import indexer
from bell_code import bell_logger

logger = bell_logger.setup_logger()


""" To run tests:
    - activate v-env
    - cd into bell_code dir
    - All tests in this file:
      python ./tests/indexer_tests.py
    - Single test:
      python ./tests/indexer_tests.py Indexer_Test.test__build_metadata_only_solr_dict """


class Indexer_Test(unittest.TestCase):
    """ Tests indexer.py code.
        Note, test_original_data is a mishmash of real data; it's just for testing. """

    sample_original_data = {
        'pid': 'bdr:10975',
        'item_data': {
            'ARTISTS::artist_alias': [None],
            'ARTISTS::artist_birth_country_id': ['231'],
            'ARTISTS::artist_birth_year': ['1937'],
            'ARTISTS::artist_death_year': [None],
            'ARTISTS::artist_first_name': ['William'],
            'ARTISTS::artist_last_name': ['Wiley'],
            'ARTISTS::artist_lifetime': ['1937'],
            'ARTISTS::artist_middle_name': ['T.'],
            'ARTISTS::artist_nationality_name': ['American'],
            'ARTISTS::calc_artist_full_name': ['William T. Wiley'],
            'ARTISTS::calc_nationality': ['American'],
            'ARTISTS::use_alias_flag': [None],
            'MEDIA::object_medium_name': 'Book',
            'MEDIA_SUB::sub_media_name': ['Etching ', 'Letterpress'],
            'OBJECT_ARTISTS::artist_id': ['120'],
            'OBJECT_ARTISTS::artist_role': [None],
            'OBJECT_ARTISTS::primary_flag': ['yes'],
            'OBJECT_MEDIA_SUB::media_sub_id': ['32', '23'],
            'SERIES::series_end_year': [None],
            'SERIES::series_name': [None],
            'SERIES::series_start_year': [None],
            'calc_accession_id': 'B 1979.1204',
            'credit_line': u"Gift of Richard M. Rieser, Jr. '65",
            'image_height': None,
            'image_width': None,
            'object_date': '1977',
            'object_depth': None,
            'object_height': None,
            'object_id': '197',
            'object_image_scan_filename': None,
            'object_medium': 'Letterpress and etching',
            'object_title': 'Suite of Daze',
            'object_width': None,
            'object_year_end': '1977',
            'object_year_start': '1977',
            'series_id': None
            }
        }

    sample_expected_metadata_only_result = {
        'accession_number_original': 'B 1979.1204',
        'author_birth_date': ['1937'],
        'author_date': ['1937'],
        'author_death_date': [''],
        'author_description': ['American'],
        'author_display': ['William T. Wiley'],
        'author_names_first': ['William'],
        'author_names_last': ['Wiley'],
        'author_names_middle': ['T.'],
        'image_height': '',
        'image_width': '',
        'jp2_image_url': '',
        'location_physical_location': 'Bell Art Gallery',
        'location_shelf_locator': 'Book',
        'master_image_url': '',
        'note_provenance': u"Gift of Richard M. Rieser, Jr. '65",
        'object_date': '1977',
        'object_depth': '',
        'object_height': '',
        'object_width': '',
        'origin_datecreated_end': '1977',
        'origin_datecreated_start': '1977',
        'physical_description_extent': [''],
        'physical_description_material': ['Letterpress and etching'],
        'physical_description_technique': ['Etching ', 'Letterpress'],
        'pid': 'bdr:10975',
        'title': 'Suite of Daze'
        }

    sample_expected_metadata_and_image_result = {
        'accession_number_original': 'B 1979.1204',
        'author_birth_date': ['1937'],
        'author_date': ['1937'],
        'author_death_date': [''],
        'author_description': ['American'],
        'author_display': ['William T. Wiley'],
        'author_names_first': ['William'],
        'author_names_last': ['Wiley'],
        'author_names_middle': ['T.'],
        'image_height': '',
        'image_width': '',
        'jp2_image_url': 'https://repository.library.brown.edu/fedora/objects/bdr:10975/datastreams/JP2/content',
        'location_physical_location': 'Bell Art Gallery',
        'location_shelf_locator': 'Book',
        'master_image_url': 'https://repository.library.brown.edu/fedora/objects/bdr:10975/datastreams/MASTER/content',
        'note_provenance': u"Gift of Richard M. Rieser, Jr. '65",
        'object_date': '1977',
        'object_depth': '',
        'object_height': '',
        'object_width': '',
        'origin_datecreated_end': '1977',
        'origin_datecreated_start': '1977',
        'physical_description_extent': [''],
        'physical_description_material': ['Letterpress and etching'],
        'physical_description_technique': ['Etching ', 'Letterpress'],
        'pid': 'bdr:10975',
        'title': 'Suite of Daze'
        }

    def test__build_metadata_only_solr_dict(self):
        """ Tests solr_dict creation. """
        idxr = indexer.Indexer( logger )
        expected_dct = self.sample_expected_metadata_only_result
        result_dct = idxr.build_metadata_only_solr_dict( self.sample_original_data['pid'], self.sample_original_data['item_data'] )
        # pprint.pprint( result )
        for ( result_key, result_value ) in result_dct.items():
            print '- result_key, `%s`' % result_key
            expected_value = expected_dct[result_key]
            self.assertEquals(
                expected_value, result_value )

    # end class Indexer_Test



if __name__ == "__main__":
  unittest.TestCase.maxDiff = None  # allows error to show in long output
  unittest.main()
