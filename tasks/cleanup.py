# -*- coding: utf-8 -*-

from __future__ import unicode_literals

""" Removes old files from bell custom solr index and fedora.
    Old files are defined as bell bdr items that have accession-numbers
        which are no longer valid accession-numbers according to
        the most recent full data dump. (see `e__accession_number_to_pid_dict.json`)
    TODO: consider moving the solr-deletion code from indexer to here. """

import json, logging, os, pprint, sets, time
import requests

logging.basicConfig(
    level=logging.DEBUG,
    filename='{}/bell.log'.format( os.environ['BELL_LOG_DIR'] ),
    format='[%(asctime)s] %(levelname)s [%(module)s-%(funcName)s()::%(lineno)d] %(message)s',
    datefmt='%d/%b/%Y %H:%M:%S' )
logger = logging.getLogger(__name__)
logger.info( 'cleanup started' )


class BdrDeleter( object ):
    """ Manages BDR deletions. """

    def __init__( self ):
        self.SOURCE_ORIGINAL_DATA_JSON_PATH = os.environ['BELL_ANTP__OUTPUT_JSON_PATH']
        self.PIDS_TO_DELETE_SAVE_PATH = os.environ['BELL_TASKS_CLNR__PIDS_TO_DELETE_SAVE_PATH']
        self.SEARCH_API_URL = os.environ['BELL_TASKS_CLNR__SEARCH_API_URL']
        self.BELL_COLLECTION_ID = os.environ['BELL_TASKS_CLNR__BELL_COLLECTION_ID']
        self.BELL_ITEM_API_URL = os.environ['BELL_TASKS_CLNR__AUTH_API_URL']
        self.BELL_ITEM_API_IDENTITY = os.environ['BELL_TASKS_CLNR__AUTH_API_IDENTITY']
        self.BELL_ITEM_API_AUTHCODE = os.environ['BELL_TASKS_CLNR__AUTH_API_KEY']

    ## called by convenience runners ##

    def make_pids_to_delete( self ):
        """ Saves list of pids to delete from the BDR.
            Called manuall per README. """
        logger.debug( 'starting make_pids_to_delete()' )
        # get source and bdr pids
        source_pids = self.prepare_source_pids()
        existing_bdr_pids = self.prepare_bdr_pids()
        # intersect lists
        ( source_pids_not_in_bdr, bdr_pids_not_in_source ) = self.intersect_pids( source_pids, existing_bdr_pids )
        logger.debug( 'ready to save output' )
        # save pids to be deleted
        self.output_pids_to_delet_list( bdr_pids_not_in_source )
        return

    def delete_pid_via_bdr_item_api( self, pid ):
        """ Hits item-api to delete item from bdr. """
        payload = {
            'pid': pid,
            'identity': self.BELL_ITEM_API_IDENTITY,
            'authorization_code': self.BELL_ITEM_API_AUTHCODE }
        print '- item_api_url, `{}`'.format( self.BELL_ITEM_API_URL )
        print '- identity, `{}`'.format( self.BELL_ITEM_API_IDENTITY )
        print '- authcode, `{}`'.format( self.BELL_ITEM_API_AUTHCODE )
        r = requests.delete( self.BELL_ITEM_API_URL, data=payload, verify=False )
        self.logger.debug( 'deletion pid, `{pid}`; r.status_code, `{code}`'.format(pid=pid, code=r.status_code) )
        self.logger.debug( 'deletion pid, `{pid}`; r.content, ```{content}```'.format(pid=pid, content=r.content.decode('utf-8')) )
        self.track_bdr_deletion( pid, status_code )
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
        logger.debug( 'len(bdr_pids), `{}`'.format(len(bdr_pids)) )
        return bdr_pids

    def intersect_pids( self, source_pids, existing_bdr_pids):
        """ Runs set work.
            Called by make_pids_to_delete() """
        source_pids_not_in_bdr = list( sets.Set(source_pids) - sets.Set(existing_bdr_pids) )
        bdr_pids_not_in_source = list( sets.Set(existing_bdr_pids) - sets.Set(source_pids) )
        logger.debug( 'source_pids_not_in_bdr, {}'.format(source_pids_not_in_bdr) )
        logger.debug( 'bdr_pids_not_in_source, {}'.format(bdr_pids_not_in_source) )
        if len( source_pids_not_in_bdr ) > 0:
            raise Exception( 'ERROR: source pids found that are not in the BDR. Investigate.' )
        return ( source_pids_not_in_bdr, bdr_pids_not_in_source )

    def output_pids_to_delet_list( self, pid_list ):
        """ Saves json file.
            Called by grab_bdr_pids() """
        jsn = json.dumps( pid_list, indent=2, sort_keys=True )
        with open( self.PIDS_TO_DELETE_SAVE_PATH, 'w' ) as f:
            f.write( jsn )
        return

    def update_bdr_deletion_tracker( self, pid, status ):
        """ Tracks bdr deletions.
            Called by delete_pid_via_bdr_item_api() """
        return 'foo'

    ## sub-helpers ##

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
        logger.debug( 'solr numFound, `{}`'.format(count) )
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


def run_make_bdr_pids_to_delete():
    deleter = BdrDeleter()
    deleter.make_pids_to_delete()

def run_delete_single_pid_from_bdr( pid ):
    deleter = BdrDeleter()
    deleter.delete_pid_via_bdr_item_api( pid )


## EOF
