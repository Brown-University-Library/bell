# -*- coding: utf-8 -*-

import os, pprint, unittest
from acc_num_to_pid import PidFinder


class MakeAccessionToPidDict_Test(unittest.TestCase):

  """ Tests class functions. """

  pid_finder = PidFinder()  # no state stored

  def test__run_studio_solr_query(self):
    """ Tests child-pids solr data. """
    bdr_collection_pid = os.environ.get( u'BELL_ANTP__COLLECTION_PID' )
    solr_root_url=os.environ.get( u'BELL_ANTP__SOLR_ROOT' )
    data_list = self.pid_finder._run_studio_solr_query( bdr_collection_pid, solr_root_url )
    dict_entry = data_list[0]
    # print u'- dict_entry...'; pprint.pprint( dict_entry )
    for key_name in [u'pid', u'rel_is_member_of_ssim']:  # just checking a few reliables
        self.assertTrue( key_name in dict_entry.keys(), u'error on key_name: %s'  % key_name )
    if bdr_collection_pid == u'bdr:10870':
        self.assertEqual( [u'bdr:10870'], dict_entry[u'rel_is_member_of_ssim'] )  # list because multiple collection-membership possible
    elif bdr_collection_pid == u'test:278':
        self.assertEqual( [u'test:278'], dict_entry[u'rel_is_member_of_ssim'] )

  def test___parse_solr_for_accession_number(self):
    """ Tests pulling accession-number from solr data. """
    bdr_collection_pid = os.environ.get( u'BELL_ANTP__COLLECTION_PID' )
    solr_root_url=os.environ.get( u'BELL_ANTP__SOLR_ROOT' )
    data_list = self.pid_finder._run_studio_solr_query( bdr_collection_pid, solr_root_url )
    pid_dict = self.pid_finder._parse_solr_for_accession_number( data_list )
    if bdr_collection_pid == u'bdr:10870':
        result = pid_dict[u'bdr:10997']
        self.assertEqual( u'DC 2011.3. c. ', result )
    elif bdr_collection_pid == u'test:278':
        result = pid_dict[u'test:973']
        self.assertEqual( u'PR 1946.479', result )

  def test___make_intersection_pid_dict(self):
    """ Tests set intersection and return-dict. """
    fedora_pid_list = [ 1, 2, 3 ]
    studio_solr_pid_list = [ 3, 4, 5 ]
    result = self.pid_finder._make_intersection_pid_dict( fedora_pid_list, studio_solr_pid_list )
    self.assertEqual( {1: u'inactive', 2: u'inactive', 3: u'active'}, result )



if __name__ == "__main__":
  unittest.TestCase.maxDiff = None  # allows error to show in long output
  unittest.main()
