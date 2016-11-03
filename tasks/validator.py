# -*- coding: utf-8 -*-

from __future__ import unicode_literals

""" Holds code to validate bell bdr collection, as well as bell custom solr index. """

import json, logging, os, pprint, sets, time
import requests

logging.basicConfig(
    level=logging.DEBUG,
    filename='{}/bell.log'.format( os.environ['BELL_LOG_DIR'] ),
    format='[%(asctime)s] %(levelname)s [%(module)s-%(funcName)s()::%(lineno)d] %(message)s',
    datefmt='%d/%b/%Y %H:%M:%S' )
logger = logging.getLogger(__name__)
logger.info( 'cleanup started' )


class CustomSolrValidator( object ):
    """ Runs validations. """

    def __init__( self ):
        self.SOURCE_ORIGINAL_DATA_JSON_PATH = os.environ.get( 'BELL_ANTP__OUTPUT_JSON_PATH' )
        self.bell_custom_solr_pids_url = 'https://library.brown.edu/search/solr_pub/bell/?q=*:*&fl=pid&wt=json&indent=2&start=0&rows=10000'
        self.bell_custom_solr_accession_numbers_url = 'https://library.brown.edu/search/solr_pub/bell/?q=*:*&fl=accession_number_original&wt=json&indent=2&start=0&rows=10000'

    def validate_counts( self ):
        """ Confirms count of entries in `e__accession_number_to_pid_dict.json`
                match the count of entries in the bell custom solr-index.
            Called manually per README """
        ( validity, original_data_count ) = ( True, len(self.get_accession_to_pid_dct()) )  # init
        r = requests.get( self.bell_custom_solr_pids_url )
        response_dct = r.json()
        solr_count = response_dct['response']['numFound']
        logger.debug( 'solr_count, `{}`'.format(solr_count) ); logger.debug( 'original_data_count, `{}`'.format(original_data_count) )
        try:
            assert original_data_count == solr_count
        except Exception as e:
            validity = False
        logger.debug( 'validity, `{}`'.format(validity) )
        return validity

    def validate_accession_numbers( self ):
        """ Confirm all original-data accession-numbers are in the bell custom solr-index.
            Called manually per README. """
        solr_accession_number_list = self.make_solr_accession_number_list()
        accession_to_pid_dct = self.get_accession_to_pid_dct()
        validity = True
        for (accession_number, pid) in accession_to_pid_dct.items():
            try:
                assert accession_number in solr_accession_number_list
            except Exception as e:
                validity = False
                logger.error( 'accession_number, `{}` not found'.format(accession_number) )
        logger.debug( 'validity, `{}`'.format(validity) )
        return validity

    def validate_pids( self ):
        """ Confirm all original-data pids are in the bell custom solr-index.
            Called manually per README. """
        solr_pid_list = self.make_solr_pid_list()
        accession_to_pid_dct = self.get_accession_to_pid_dct()
        validity = True
        for (accession_number, pid) in accession_to_pid_dct.items():
            try:
                assert pid in solr_pid_list
            except Exception as e:
                validity = False
                logger.error( 'pid, `{}` not found'.format(pid) )
        logger.debug( 'validity, `{}`'.format(validity) )
        return validity

    ## helpers ##

    def get_accession_to_pid_dct( self ):
        """ Returns accession_to_pid_dct.
            Called by validate_counts(), validate_accession_numbers() """
        with open( self.SOURCE_ORIGINAL_DATA_JSON_PATH ) as f:
            source_dct = json.loads( f.read() )
        accession_to_pid_dct = source_dct['final_accession_pid_dict']
        return accession_to_pid_dct

    def make_solr_accession_number_list( self ):
        """ Returns list of accession numbers from bell custom solr.
            Called by validate_accession_numbers() """
        solr_accession_number_list = []
        r = requests.get( self.bell_custom_solr_accession_numbers_url )
        response_dct = r.json()
        entries = response_dct['response']['docs']
        for entry_dct in entries:
            ( key_label, val_accession_number ) = entry_dct.items()[0]  # there's only one
            solr_accession_number_list.append( val_accession_number )
        solr_accession_number_list = sorted( solr_accession_number_list )
        logger.debug( 'count solr_accession_number_list, `{}`'.format(len(solr_accession_number_list)) )
        # logger.debug( 'solr_accession_number_list, ```{}```'.format(pprint.pformat(solr_accession_number_list)) )
        return solr_accession_number_list

    def make_solr_pid_list( self ):
        """ Returns list of pids from bell custom solr.
            Called by validate_pids() """
        solr_pid_list = []
        r = requests.get( self.bell_custom_solr_pids_url )
        response_dct = r.json()
        entries = response_dct['response']['docs']
        for entry_dct in entries:
            ( key_label, val_pid ) = entry_dct.items()[0]  # there's only one
            solr_pid_list.append( val_pid )
        solr_pid_list = sorted( solr_pid_list )
        logger.debug( 'count solr_pid_list, `{}`'.format(len(solr_pid_list)) )
        # logger.debug( 'solr_pid_list, ```{}```'.format(pprint.pformat(solr_pid_list)) )
        return solr_pid_list

    # end class CustomSolrValidator()


## convenience runners ##


def run_validate_solr_counts():
    v = CustomSolrValidator()
    v.validate_counts()

def run_validate_solr_accession_numbers():
    v = CustomSolrValidator()
    v.validate_accession_numbers()

def run_validate_solr_pids():
    v = CustomSolrValidator()
    v.validate_pids()
