# -*- coding: utf-8 -*-

import unittest
from acc_num_to_pid import PidFinder


class MakeAccessionToPidDict_Test(unittest.TestCase):

  """ Tests class functions. """

  pid_finder = PidFinder()  # no state stored

  def test___run_studio_solr_query(self):
    """ Tests get-pids-from-solr. """
    bdr_collection_pid = u'bdr:10870'
    data_list = self.pid_finder._run_studio_solr_query( bdr_collection_pid )
    entry = data_list[0]
    self.assertEquals( [u'identifier', u'mods_id_bell_accession_number_ssim', u'pid'], sorted(entry.keys()) )

  # def test___make_intersection_pid_dict(self):
  #   """ Tests set intersection and return-dict. """
  #   fedora_pid_list = [ 1, 2, 3 ]
  #   studio_solr_pid_list = [ 3, 4, 5 ]
  #   result = self.pid_finder._make_intersection_pid_dict( fedora_pid_list, studio_solr_pid_list )
  #   self.assertEquals( {1: u'inactive', 2: u'inactive', 3: u'active'}, result )




if __name__ == "__main__":
  unittest.TestCase.maxDiff = None  # allows error to show in long output
  unittest.main()
