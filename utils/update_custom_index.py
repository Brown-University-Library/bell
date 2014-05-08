# -*- coding: utf-8 -*-

""" Reindexes a list of pids.
    Iterates through them, updating custom index.
        - Checks:
            - accession number in correct place.
            - bell_meta datastream exists.
            - (future) existence of both jp2 and master if either exists
            - (future) initial stream check on jp2 and master image
    If list contains string 'all',
        - all bell collection pids will be reindexed (custom index).
        - any custom-index entries not in full-pid-list will be removed. """

import argparse, json, os, pprint, sys, time
import requests
from bell_code import bell_logger


def parse_args():
    """ Parses arguments when module called via __main__. """
    parser = argparse.ArgumentParser( description=u'Required: list of pids.' )
    parser.add_argument( u'--pids', u'-p', help=u'pid-list in json format, eg \'[ "foo", "bar" ]\'', required=True )
    args_dict = vars( parser.parse_args() )
    return args_dict


class Reindexer( object ):

    def __init__( self, kwargs ):
        self.logger = bell_logger.setup_logger()
        self.solr_root_url = kwargs['solr_root_url']
        print u'- self.solr_root_url, `%s`' % self.solr_root_url

    def reindex( self, pid_list ):
        print u'- pid_list...'; pprint.pprint( pid_list )


    # ## grab initial minimal solr data

    # def _run_bdr_solr_query( self, bdr_collection_pid, solr_root_url ):
    #     """ Returns _solr_ doc list.
    #         Example result: [ {pid:bdr123, identifier:[acc_num_a,other_num_b], mods_id_bell_accession_number_ssim:None_or_acc_num_a, other:...}, etc. ]
    #         Called by reindex_all_bdr(). """
    #     doc_list = []
    #     for i in range( 100 ):  # would handle 50,000 records
    #     # for i in range( 2 ):  # would handle 50,000 records
    #         data_dict = self.__query_solr( i, bdr_collection_pid, solr_root_url )
    #         docs = data_dict[u'response'][u'docs']
    #         doc_list.extend( docs )
    #         if not len( docs ) > 0:
    #             break
    #     # self.logger.info( u'in _run_studio_solr_query(); doc_list, %s' % pprint.pformat(doc_list) )
    #     return doc_list

    # def __query_solr( self, i, bdr_collection_pid, solr_root_url ):
    #     """ Queries solr for iterating start-row.
    #         Returns results dict.
    #         Called by _run_bdr_solr_query() """
    #     new_start = i * 500  # for solr start=i parameter (cool, eh?)
    #     # new_start = i * 2  # for solr start=i parameter (cool, eh?)
    #     params = {
    #         u'q': u'rel_is_member_of_ssim:"%s"' % bdr_collection_pid,
    #         u'fl': u'pid,mods_id_bell_accession_number_ssim,primary_title',
    #         u'rows': 500, u'start': new_start, u'wt': u'json' }
    #     r = requests.get( solr_root_url, params=params, verify=False )
    #     self.logger.info( u'in __query_solr(); r.url, %s' % r.url )
    #     data_dict = json.loads( r.content.decode(u'utf-8', u'replace') )
    #     time.sleep( .1 )
    #     # self.logger.info( u'in __query_solr(); data_dict, %s' % pprint.pformat(data_dict) )
    #     return data_dict

    # ## validate accession number

    # def _run_accession_number_validation( self, solr_query_docs ):
    #     """ Takes list of dicts from solr query.
    #         Ensures each entry has an accession number.
    #         Raises an exception and prints list of entries that don't have accession-numbers in expected places.
    #         Returns True on success.
    #         Called by reindex_all_bdr() """
    #     bad_apples = []
    #     counter = 0
    #     for item_dict in solr_query_docs:
    #         counter += 1
    #         if item_dict.get( u'mods_id_bell_accession_number_ssim', None ) == None:
    #             bad_apples.append( item_dict )
    #         elif len( item_dict[u'mods_id_bell_accession_number_ssim'] ) == 0:  # empty list
    #             bad_apples.append( item_dict )
    #     print u'%s items checked; bad apples...' % counter; pprint.pprint( bad_apples )
    #     if len( bad_apples ) > 0:
    #         return False
    #     else:
    #         return True

    # ## grab all accession_numbers from custom-solr

    # def grab custom_solr_accession_numbers( self ):
    #     """ Returns a list of accession_numbers from the bell custom-solr index. """
    #     # params = {
    #     #     u'q': u'rel_is_member_of_ssim:"%s"' % bdr_collection_pid,
    #     #     u'fl': u'pid,mods_id_bell_accession_number_ssim,primary_title',
    #     #     u'rows': 500, u'start': new_start, u'wt': u'json' }
    #     params = {
    #         u'q': u'rel_is_member_of_ssim:"%s"' % bdr_collection_pid,
    #         u'fl': u'pid,mods_id_bell_accession_number_ssim,primary_title',
    #         u'rows': 500, u'start': new_start, u'wt': u'json' }
    #     r = requests.get( custom_solr_root_url, params=params, verify=False )

# solr_query_docs = self._run_bdr_solr_query( bdr_collection_pid, solr_root_url )
# accession_number_validation = self._run_accession_number_validation( solr_query_docs )
# pprint.pprint( accession_number_validation )



if __name__ == u'__main__':
    """ Assumes env is activated.
        ( 'UCI' used as a namespace prefix for this 'updated_custom_index.py' file. ) """
    reindexer = Reindexer( kwargs={u'solr_root_url': os.environ.get(u'BELL_UCI__SOLR_ROOT')} )
    args = parse_args()
    pid_list = json.loads( args[u'pids'] )
    if pid_list == [u'all']:  # get all collection pids
        pid_list = reindexer.grab_all_collection_pids( kwargs={u'bdr_collection_pid': os.environ.get(u'BELL_UCI__COLLECTION_PID')} )
    reindexer.reindex( pid_list )
