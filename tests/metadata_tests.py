# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import json, logging, os, pprint, unittest
from bell_code.utils import mods_builder
from bell_code.utils import logger_setup, mods_builder

logger = logging.getLogger( 'bell_logger' )
logger_setup.check_log_handler()


""" To run tests:
    - activate v-env
    - cd into bell_code dir
    - All tests in this file:
      python ./tests/metadata_tests.py
    - Single test:
      python ./tests/metadata_tests.py ModsBuilder_Test.test__foo """


class ModsBuilder_Test(unittest.TestCase):
    """ Tests indexer.py code.
        Note, test_original_data is a mishmash of real data; it's just for testing. """

    def setUp(self):
        self.MODS_SCHEMA_PATH = unicode( os.environ['BELL_TASKS_META__MODS_XSD_PATH'] )
        self.finished_xml = '''<mods:mods xmlns:mods="http://www.loc.gov/mods/v3" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.loc.gov/mods/v3 http://www.loc.gov/standards/mods/v3/mods-3-4.xsd">
  <mods:titleInfo>
    <mods:title>Place (Series) #512</mods:title>
  </mods:titleInfo>
  <mods:name type="personal">
    <mods:namePart>Jacobson, Bill</mods:namePart>
    <mods:displayForm>Bill Jacobson</mods:displayForm>
    <mods:namePart type="date">1955-</mods:namePart>
    <mods:role>
      <mods:roleTerm type="text">artist</mods:roleTerm>
    </mods:role>
  </mods:name>
  <mods:originInfo>
    <mods:dateCreated encoding="w3cdtf" point="start">2011</mods:dateCreated>
    <mods:dateCreated encoding="w3cdtf" point="end">2011</mods:dateCreated>
  </mods:originInfo>
  <mods:physicalDescription>
    <mods:form type="material">Pigment print</mods:form>
    <mods:form type="technique">Digital Print</mods:form>
  </mods:physicalDescription>
  <mods:note type="provenance">Gift of James H. Carey '53 by exchange</mods:note>
  <mods:location>
    <mods:physicalLocation>Bell Art Gallery</mods:physicalLocation>
    <mods:holdingSimple>
      <mods:copyInformation>
        <mods:shelfLocator>Photography</mods:shelfLocator>
      </mods:copyInformation>
    </mods:holdingSimple>
  </mods:location>
  <mods:identifier type="bell_accession_number">PH 2015.4.1</mods:identifier>
  <mods:identifier type="bell_object_id">6577</mods:identifier>
  <mods:typeOfResource>still image</mods:typeOfResource>
</mods:mods>'''

    def test__validate_xml(self):
        """ Tests xml agains xsd. """
        mb = mods_builder.ModsBuilder()
        self.assertEquals(
            'foo', mb._validate_mods( self.finished_xml, self.MODS_SCHEMA_PATH )
            )

    # end class ModsBuilder_Test



if __name__ == "__main__":
  unittest.TestCase.maxDiff = None  # allows error to show in long output
  unittest.main()
