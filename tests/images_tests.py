# -*- coding: utf-8 -*-

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
        self.assertEquals(
            2, 3 )

    # end class ImageAdder_Test



if __name__ == "__main__":
  unittest.TestCase.maxDiff = None  # allows error to show in long output
  unittest.main()
