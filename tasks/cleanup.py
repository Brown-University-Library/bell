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
    - validate that all
    - validate that all original-data pids are in the bdr.
    - make list of valid bdr pids.
    - get complete list of fedora bdr pids.
    - make list of bdr pids to delete.
    - run deletion.
    - TODO update future flow...
        - run initial bdr validation
        - run bdr cleanup
        - run rest of bdr validation
        - update solr index
        - run solr validation
    """

import json, logging, os, pprint, time
import requests

logging.basicConfig(
    level=logging.DEBUG,
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


class BdrDeleter( object ):
    """ Manages BDR deletions. """

    def __init__( self ):
        self.SOURCE_ORIGINAL_DATA_JSON_PATH = os.environ['BELL_ANTP__OUTPUT_JSON_PATH']
        self.SEARCH_API_URL = os.environ['BELL_TASKS_CLNR__SEARCH_API_URL']
        self.BELL_COLLECTION_ID = os.environ['BELL_TASKS_CLNR__BELL_COLLECTION_ID']

    def make_pids_to_delete( self ):
        """ Saves list of pids to delete from the BDR.
            Called manuall per README. """
        logger.debug( 'starting make_pids_to_delete()' )
        # get source and bdr pids
        source_pids = self.prepare_source_pids()
        existing_bdr_pids = self.prepare_bdr_pids()
        # intersect lists
        logger.debug( 'ready to intersect lists' )
        # save pids to be deleted
        return

    ## helpers ##

    def prepare_source_pids( self ):
        """ Returns accession_to_pid_dct.
            Called by make_pids_to_delete() """
        with open( self.SOURCE_ORIGINAL_DATA_JSON_PATH ) as f:
            source_dct = json.loads( f.read() )
        accession_to_pid_dct = source_dct['final_accession_pid_dict']
        source_pids = []
        for ( key_accession_number, value_pid ) in accession_to_pid_dct.items():
            source_pids.append( value_pid )
        source_pids = sorted( source_pids )
        logger.debug( 'count source_pids, `{}`'.format(len(source_pids)) )
        logger.debug( 'source_pids (first 3), ```{}```'.format(pprint.pformat(source_pids[0:3])) )
        return source_pids

    def prepare_bdr_pids( self ):
        """ Returns list of bdr pids associated with the bell collection.
            Called by make_pids_to_delete() """
        logger.debug( 'starting prepare_bdr_pids()' )
        ( bdr_pids, start, rows, total_count ) = ( [], 0, 500, self.get_total_count() )
        while start <= total_count:
            queried_pids = self.query_bdr_solr( start, rows )
            for pid in queried_pids:
                bdr_pids.append( pid )
            start += 500
        bdr_pids = sorted( bdr_pids )
        logger.debug( 'bdr_pids count, `{}`'.format(len(bdr_pids)) )
        return bdr_pids

    def get_total_count( self ):
        """ Gets count of bdr pids for bell collection.
            Called by helper prepare_bdr_pids() """
        params = {
            'q': 'rel_is_member_of_ssim:"{}"'.format( self.BELL_COLLECTION_ID ),
            'wt': 'json', 'indent': '2', 'rows': '0'
            }
        r = requests.get( self.SEARCH_API_URL, params=params )
        dct = r.json()
        count = int( dct['response']['numFound'] )
        logger.debug( 'count, `{}`'.format(count) )
        return count

    def query_bdr_solr( self, start, rows ):
        """ Querys bdr searche api.
            Called by helper prepare_bdr_pids() """
        logger.debug( 'start, `{}`'.format(start) )
        time.sleep( 1 )
        queried_pids = []
        params = self.prep_looping_params( start, rows )
        r = requests.get( self.SEARCH_API_URL, params=params )
        dct = r.json()
        for entry in dct['response']['docs']:
            ( label_key, pid_value ) = entry.items()[0]  # entry example: `{ "pid": "bdr:650881" }`
            queried_pids.append( pid_value )
        return queried_pids

    def prep_looping_params( self, start, rows ):
        """ Build param dct.
            Called by helper query_bdr_solr. """
        params = {
            'q': 'rel_is_member_of_ssim:"{}"'.format( self.BELL_COLLECTION_ID ),
            'fl': 'pid',
            'start': start,
            'rows': rows,
            'wt': 'json', 'indent': '2' }
        return params

    # end class BdrDeleter()


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

def run_make_bdr_pids_to_delete():
    deleter = BdrDeleter()
    deleter.make_pids_to_delete()


## EOF
