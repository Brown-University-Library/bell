# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import json, os, pprint, unittest
from bell_code.tasks import images
from bell_code import bell_logger

logger = bell_logger.setup_logger()


""" To run tests:
    - activate v-env
    - cd into bell_code dir
    - All tests in this file:
      python ./tests/images_tests.py
    - Single test:
      python ./tests/images_tests.py ImageAdder_Test.test__foo """


class ImageAdder_Test(unittest.TestCase):
    """ Tests indexer.py code.
        Note, test_original_data is a mishmash of real data; it's just for testing. """

    def test__create_temp_filenames(self):
        """ Tests master and jp2 paths and filenames. """
        adder = images.ImageAdder( logger )
        adder.MASTER_IMAGES_DIR_PATH = '/path/to/masters'
        adder.JP2_IMAGES_DIR_PATH = '/path/to/jp2s'
        ## spaces
        image_filename = 'abc def.ghi.tif'
        ( source_filepath, destination_filepath, master_filename_encoded, jp2_filename ) = adder.create_temp_filenames( image_filename )
        self.assertEquals( '/path/to/masters/abc def.ghi.tif', source_filepath )
        self.assertEquals( '/path/to/jp2s/abc_def.ghi.jp2', destination_filepath )  # no spaces
        self.assertEquals( 'abc%20def.ghi.tif', master_filename_encoded )  # for url access
        self.assertEquals( 'abc_def.ghi.jp2', jp2_filename ) # for url access
        # apostrophes
        image_filename = u"a'bc def.ghi.tif"
        ( source_filepath, destination_filepath, master_filename_encoded, jp2_filename ) = adder.create_temp_filenames( image_filename )
        self.assertEquals( u"/path/to/masters/a'bc def.ghi.tif", source_filepath )
        self.assertEquals( '/path/to/jp2s/a_bc_def.ghi.jp2', destination_filepath )  # no apostrophe
        self.assertEquals( 'a%27bc%20def.ghi.tif', master_filename_encoded )  # for url access
        self.assertEquals( 'a_bc_def.ghi.jp2', jp2_filename ) # for url access

    # end class ImageAdder_Test



if __name__ == "__main__":
  unittest.TestCase.maxDiff = None  # allows error to show in long output
  unittest.main()
