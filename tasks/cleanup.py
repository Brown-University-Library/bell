# -*- coding: utf-8 -*-

from __future__ import unicode_literals

""" Removes old files from fedora.
    Old files are defined as bell bdr items that have accession-numbers
        which are no longer valid accession-numbers according to
        the most recent full data dump. (see `e__accession_number_to_pid_dict.json`)
    Flow:
    - validate that the original-data count matches the custom-solr doc-count.
    - validate that all original-data accession-numbers are in the bell custom solr-index.
    - validate that all original-data pids are in the bell custom solr-index.
    - validate that all original-data pids are in the bdr.
    - make list of valid bdr pids.
    - get complete list of fedora bdr pids.
    - make list of bdr pids to delete.
    - run deletion.
    """

import json, logging, os
import requests

logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s] %(levelname)s [%(module)s-%(funcName)s()::%(lineno)d] %(message)s',
    datefmt='%d/%b/%Y %H:%M:%S' )
logger = logging.getLogger(__name__)
logger.info( 'cleanup started' )


class Validator( object ):
    """ Runs validations. """

    def __init__( self ):
        self.SOURCE_ORIGINAL_DATA_JSON_PATH = os.environ.get( 'BELL_ANTP__OUTPUT_JSON_PATH' )
        self.bell_custom_solr_pids_url = 'https://library.brown.edu/search/solr_pub/bell/?q=*:*&fl=pid&wt=json&indent=2&start=0&rows=10000'
        self.bell_custom_solr_accession_numbers_url = 'https://library.brown.edu/search/solr_pub/bell/?q=*:*&fl=accession_number_original&wt=json&indent=2&start=0&rows=10000'

    def validate_counts( self ):
        """ Confirms count of entries in `e__accession_number_to_pid_dict.json`
                match the count of entries in the bell custom solr-index. """
        with open( self.SOURCE_ORIGINAL_DATA_JSON_PATH ) as f:
            source_dct = json.loads( f.read() )
        accession_to_pid_dct = source_dct['final_accession_pid_dict']
        original_data_count = len( accession_to_pid_dct )
        logger.debug( 'original_data_count, `{}`'.format(original_data_count) )
        r = requests.get( self.bell_custom_solr_pids_url )
        response_dct = r.json()
        solr_count = response_dct['response']['numFound']
        logger.debug( 'solr_count, `{}`'.format(solr_count) )
        assert original_data_count == solr_count
        return True




    # end class OldPidFinder()
