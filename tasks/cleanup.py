""" Removes old files from bell custom solr index (eventually) and fedora.
    Old files are defined as bell bdr items that have accession-numbers
        which are no longer valid accession-numbers according to
        the most recent full data dump. (see `e__accession_number_to_pid_dict.json`)
    TODO: move the solr-deletion code from indexer to here. """

import datetime, json, logging, os, pprint, time
import requests

logging.basicConfig(
    level=logging.DEBUG,
    filename=os.environ['BELL_LOG_FILENAME'],
    format='[%(asctime)s] %(levelname)s [%(module)s-%(funcName)s()::%(lineno)d] %(message)s',
    datefmt='%d/%b/%Y %H:%M:%S' )
logger = logging.getLogger(__name__)
logger.info( 'cleanup started' )


class BdrDeleter( object ):
    """ Manages BDR deletions. """

    def __init__( self ):
        self.DELETED_PIDS_TRACKER_PATH = os.environ['BELL_TASKS_CLNR__DELETED_PIDS_TRACKER_PATH']
        self.SEARCH_API_URL = os.environ['BELL_TASKS_CLNR__SEARCH_API_URL']
        self.BELL_COLLECTION_ID = os.environ['BELL_TASKS_CLNR__BELL_COLLECTION_ID']
        self.BELL_ITEM_API_URL = os.environ['BELL_TASKS_CLNR__PROD_AUTH_API_URL']
        self.BELL_ITEM_API_IDENTITY = os.environ['BELL_TASKS_CLNR__PROD_AUTH_API_IDENTITY']
        self.BELL_ITEM_API_AUTHCODE = os.environ['BELL_TASKS_CLNR__PROD_AUTH_API_KEY']

    ## called by convenience runners ##

    def make_pids_to_delete( self ):
        """ Saves list of pids to delete from the BDR.
            Called manuall per README. """
        logger.debug( 'starting make_pids_to_delete()' )
        # get source and bdr pids
        source_pids = self.prepare_source_pids() #simple list
        existing_bdr_pids = self.prepare_bdr_pids() #dict of information for all Bell pids in the BDR
        # intersect lists
        ( source_pids_not_in_bdr, bdr_pids_not_in_source ) = self.intersect_pids( source_pids, list(existing_bdr_pids.keys()) )
        logger.debug( 'ready to save output' )
        delete_pids_info = {}
        for pid in bdr_pids_not_in_source:
            delete_pids_info[pid] = existing_bdr_pids[pid]
        # save pids to be deleted
        self.output_pids_to_delete_list( delete_pids_info )
        return

    def delete_pid_via_bdr_item_api( self, pid ):
        """ Hits item-api to delete item from bdr.
        TODO: make this environment-aware so we can run against dev if desired.
        """
        payload = {
            'pid': pid,
            'identity': self.BELL_ITEM_API_IDENTITY,
            'authorization_code': self.BELL_ITEM_API_AUTHCODE }
        print('- item_api_url, `{}`'.format( self.BELL_ITEM_API_URL ))
        print('- identity, `{}`'.format( self.BELL_ITEM_API_IDENTITY ))
        print('- authcode, `{}`'.format( self.BELL_ITEM_API_AUTHCODE ))
        r = requests.delete( self.BELL_ITEM_API_URL, data=payload )
        logger.debug( 'deletion pid, `{pid}`; r.status_code, `{code}`'.format(pid=pid, code=r.status_code) )
        logger.debug( 'deletion pid, `{pid}`; r.content, ```{content}```'.format(pid=pid, content=r.content.decode('utf-8')) )
        self.track_bdr_deletion( pid, r.status_code )
        return

    ## helpers ##

    def prepare_source_pids( self ):
        """ Returns accession_to_pid_dct.
            Called by make_pids_to_delete() """
        with open( os.path.join('data', 'e__accession_number_to_pid_dict.json') ) as f:
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
        ( bdr_pids, start, rows, total_count ) = ( {}, 0, 500, self.get_total_count() )
        while start <= total_count:
            queried_pids = self.query_bdr_solr( start, rows )
            bdr_pids.update( queried_pids )
            start += 500
        logger.debug( 'len(bdr_pids), `{}`'.format(len(bdr_pids)) )
        return bdr_pids

    def intersect_pids( self, source_pids, existing_bdr_pids):
        """ Runs set work.
            Called by make_pids_to_delete() """
        source_pids_not_in_bdr = list( set(source_pids) - set(existing_bdr_pids) )
        bdr_pids_not_in_source = list( set(existing_bdr_pids) - set(source_pids) )
        logger.debug( 'source_pids_not_in_bdr, {}'.format(source_pids_not_in_bdr) )
        logger.debug( 'bdr_pids_not_in_source, {}'.format(bdr_pids_not_in_source) )
        if len( source_pids_not_in_bdr ) > 0:
            raise Exception( 'ERROR: source pids found that are not in the BDR. Investigate.' )
        return ( source_pids_not_in_bdr, bdr_pids_not_in_source )

    def output_pids_to_delete_list( self, pids_info ):
        """ Saves json file.
            Called by grab_bdr_pids() """
        data = {'datetime': str(datetime.datetime.now()), 'pids_to_delete': pids_info}
        jsn = json.dumps( data, indent=2, sort_keys=True )
        with open( os.path.join('data', 'm__bdr_delete_pids.json'), 'wt' ) as f:
            f.write( jsn )
        return

    def track_bdr_deletion( self, pid, status ):
        """ Tracks bdr deletions.
            Called by delete_pid_via_bdr_item_api() """
        with open( self.DELETED_PIDS_TRACKER_PATH ) as f:
            dct = json.loads( f.read() )
        if pid not in dct.keys():
            dct[pid] = []
        entry = { 'datetime': str( datetime.datetime.now() ), 'response_status_code': status }
        dct[pid].append( entry )
        with open( self.DELETED_PIDS_TRACKER_PATH, 'w' ) as f:
            f.write( json.dumps(dct, sort_keys=True, indent=2) )
        return

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
        """ Querys bdr search api.
            Called by helper prepare_bdr_pids() """
        logger.debug( 'start, `{}`'.format(start) )
        time.sleep( 1 )
        queried_pids = {}
        params = self.prep_looping_params( start, rows )
        r = requests.get( self.SEARCH_API_URL, params=params )
        dct = r.json()
        for entry in dct['response']['docs']:
            #entry: {'pid': 'bdr:123', 'rel_object_type_ssi': 'jp2'}
            d = {'object_type': entry['rel_object_type_ssi'],
                 'accession number': entry['mods_id_bell_accession_number_ssim'][0],
                 'title': entry.get('primary_title', ''),
                 'url': 'https://repository.library.brown.edu/studio/item/%s/' % entry['pid']}
            queried_pids[entry['pid']] = d
        return queried_pids

    def prep_looping_params( self, start, rows ):
        """ Build param dct.
            Called by helper query_bdr_solr. """
        params = {
            'q': 'rel_is_member_of_ssim:"{}"'.format( self.BELL_COLLECTION_ID ),
            'fl': 'pid,rel_object_type_ssi,mods_id_bell_accession_number_ssim,primary_title',
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
