# -*- coding: utf-8 -*-

from __future__ import unicode_literals

""" Creates a json list of item-pids for bell collection pid.
    To run:
    - activate virtual environment
    - run this script. """

import datetime, json, os, pprint, time
import requests


class ItemPidMaker( object ):

    def __init__( self, BELL_COLLECTION_PID, API_ROOT_URL ):
        self.BELL_COLLECTION_PID = BELL_COLLECTION_PID
        self.SEARCH_URL = '%s/search/' % API_ROOT_URL
        self.ITEM_URL = '%s/items/' % API_ROOT_URL

    def make_pid_list( self ):
        """ Controls pid build. """
        doc_list = self._make_initial_doc_list()
        enhanced_doc_list = self._enhance_doc_list( doc_list )
        return_dict = self._make_return_dict( enhanced_doc_list, self._count_images(enhanced_doc_list) )
        return return_dict

    def _make_initial_doc_list( self ):
        """ Runs loop for solr queries.
            Called by make_pid_list(). """
        doc_list = []
        for i in range( 100 ):  # would handle 50,000 records
            data_dict = self.__query_solr( i, self.BELL_COLLECTION_PID, self.SEARCH_URL )
            docs = data_dict['response']['docs']
            doc_list.extend( docs )
            if not len( docs ) > 0:
                break
        return doc_list

    def __query_solr( self, i, bell_collection_pid, solr_root_url ):
        """ Queries solr for iterating start-row.
            Returns results dict.
            Called by _make_initial_doc_list(). """
        new_start = i * 500  # for solr start=i parameter (cool, eh?)
        params = {
            'q': 'rel_is_member_of_ssim:"%s"' % bell_collection_pid,
            'fl': 'pid,mods_id_bell_accession_number_ssim,primary_title,datastreams_ss',
            'rows': 500, 'start': new_start, 'wt': 'json' }
        r = requests.get( solr_root_url, params=params, verify=False )
        data_dict = json.loads( r.content.decode('utf-8', 'replace') )
        # data_dict['item_api_url'] = '%s%s'
        time.sleep( .1 )
        return data_dict

    def _enhance_doc_list( self, doc_list ):
        """ Updates labels, datastream info, and item-api.
            Called by make_pid_list(). """
        enhanced_doc_list = []
        for item in doc_list:
            new_item = {
                'pid': item['pid'],
                'accession_number': item['mods_id_bell_accession_number_ssim'],
                'has_jp2': self.__add_jp2_info( item ),
                'item_api_url': '%s%s/' % ( self.ITEM_URL, item['pid'] ) }
            enhanced_doc_list.append( new_item )
        return enhanced_doc_list

    def __add_jp2_info( self, item ):
        """ Returns has_jp2 = True or False.
            Called by _enhance_doc_list(). """
        datastream_info = json.loads( item['datastreams_ss'] )
        has_jp2 = True if 'JP2' in datastream_info.keys() else False
        return has_jp2

    def _count_images( self, cleaned_doc_list ):
        """ Returns count of items with images.
            Called by make_pid_list(). """
        count = 0
        for item in cleaned_doc_list:
            if item['has_jp2'] == True:
                count += 1
        return count

    def _make_return_dict( self, enhanced_doc_list, image_count ):
        """ Builds and returns final dict.
            Called by make_pid_list(). """
        return_dict = {
            'datetime': unicode( datetime.datetime.now() ),
            'count_items': len( enhanced_doc_list ),
            'count_images': image_count,
            'items': enhanced_doc_list }
        return return_dict

    # end class ItemPidMaker()




if __name__ == '__main__':
    BELL_COLLECTION_PID = os.environ['BELL_ONEOFF__BELL_COLLECTION_PID']
    API_ROOT_URL = os.environ['BELL_ONEOFF__API_ROOT_URL']
    maker = ItemPidMaker( BELL_COLLECTION_PID, API_ROOT_URL )
    pids = maker.make_pid_list()
    print json.dumps( pids, sort_keys=True, indent=2 )
