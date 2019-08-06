import datetime, json, os, pprint, sys, time
import logging
import requests
from bell_utils import DATA_DIR


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] %(levelname)s [%(module)s-%(funcName)s()::%(lineno)d] %(message)s',
                    datefmt='%d/%b/%Y %H:%M:%S')


class PidFinder:
    """ Handles creation of an accession_number-to-pid-info dict, saved as a json file.
        Purpose: map accession numbers in the Bell data to the PID of a BDR object.

            This is one of the essential files that should exist before doing almost any bell processing,
                 because the source bell data contains no pid info, and it is essential to know whether a bell item
                 needs to create or update bdr data.
        """

    def __init__(self, env='dev'):
        self.bell_acc_num_data_path = os.path.join(DATA_DIR, 'c__accession_number_to_data_dict.json')
        self.output_json_path = os.path.join(DATA_DIR, 'e1__accession_number_to_pid_dict.json')
        if env == 'prod':
            self.bdr_collection_pid = os.environ['BELL_ANTP__PROD_COLLECTION_PID']
            self.bdr_api_url = os.environ['BELL_ANTP__PROD_SOLR_ROOT']
        else:
            self.bdr_collection_pid = os.environ['BELL_ANTP__DEV_COLLECTION_PID']
            self.bdr_api_url = os.environ['BELL_ANTP__DEV_SOLR_ROOT']

    def create_acc_num_to_pid_map( self ):
        """ CONTROLLER.
            - Calls repo solr (500 rows at a time) to get all bell items
            - Creates an accession-number-to-pid dict from above
            - Creates an accesstion-number-to-pid dict from submitted data and saves to json file
            - Example produced data: { acc_num_1: {pid:bdr_123, title:abc}, acc_num_2: {pid:None, title:None}, etc. } """
        bdr_bell_docs = self._fetch_all_bdr_bell_records( self.bdr_collection_pid, self.bdr_api_url )
        bdr_accession_number_data = self._make_bdr_accession_number_data( bdr_bell_docs )
        bell_accession_number_data = self._load_bell_accession_number_data( self.bell_acc_num_data_path )
        final_accession_dict = self._make_final_accession_number_dict( bell_accession_number_data, bdr_accession_number_data )
        self._output_json( final_accession_dict, self.output_json_path )

    def _fetch_all_bdr_bell_records( self, bdr_collection_pid, bdr_api_url ):
        """ Returns _solr_ doc list.
            Example solr url: 'https://solr-url/?q=rel_is_member_of_ssim:"collection:pid"&start=x&rows=y&fl=pid,mods_id_bell_accession_number_ssim'
            Example result: [ {pid:bdr123, identifier:[acc_num_a,other_num_b], mods_id_bell_accession_number_ssim:None_or_acc_num_a, other:...}, etc. ] """
        doc_list = []
        for i in range( 100 ):  # would handle 50,000 records
            data_dict = self.__query_bdr( i, bdr_collection_pid, bdr_api_url )
            docs = data_dict['response']['docs']
            doc_list.extend( docs )
            if not len( docs ) > 0:
                break
        return doc_list

    def __query_bdr( self, i, bdr_collection_pid, bdr_api_url ):
        new_start = i * 500  # for solr start=i parameter (cool, eh?)
        params = {
            'q': 'rel_is_member_of_ssim:"%s"' % bdr_collection_pid,
            'fl': 'pid,mods_id_bell_accession_number_ssim,mods_id_bell_object_id_ssim',
            'rows': 500,
            'start': new_start,
        }
        r = requests.get(bdr_api_url, params=params)
        if not r.ok:
            raise Exception(f'{r.status_code} - {r.content.decode("utf8")}')
        data_dict = json.loads( r.content.decode('utf-8') )
        return data_dict

    def _make_bdr_accession_number_data( self, bdr_bell_docs ):
        accnum_data = {}
        for solr_doc in bdr_bell_docs:
            pid = solr_doc['pid']
            try:
                accession_number = solr_doc['mods_id_bell_accession_number_ssim'][0]  # accession-numbers are in solr as a single-item list
                accnum_data[accession_number] = solr_doc
            except KeyError:
                raise Exception(f'no accession number in BDR: {solr_doc}')
        return accnum_data

    def _load_bell_accession_number_data( self, bell_acc_num_data_path ):
        with open( bell_acc_num_data_path ) as f:
            accession_number_data = json.loads( f.read() )
        return accession_number_data['items']

    def _make_final_accession_number_dict( self, bell_accession_number_data, bdr_accession_number_data ):
        """ Takes source accession-number list, and bdr accession-number-to-pid dict
            Creates and returns an accession-number-to-pid dict from the source bell data.
            Example: { acc_num_1: bdr_123, acc_num_2: None } """
        final_accession_pid_dict = {}
        for accession_number in sorted(bell_accession_number_data.keys()):
            if accession_number in bdr_accession_number_data:
                final_accession_pid_dict[accession_number] = bdr_accession_number_data[accession_number]['pid']
            else:
                #loop through all the BDR records, seeing if we can find a match
                for bdr_accession_number, bdr_data in bdr_accession_number_data.items():
                    if accession_number.replace(' ', '') == bdr_accession_number.replace(' ', ''):
                        final_accession_pid_dict[accession_number] = bdr_data['pid']
                    elif bell_accession_number_data[accession_number]['object_id'] == bdr_accession_number_data[bdr_accession_number]['mods_id_bell_object_id_ssim'][0]:
                        final_accession_pid_dict[accession_number] = bdr_data['pid']
                if accession_number not in final_accession_pid_dict:
                    final_accession_pid_dict[accession_number] = None
        mapped_bdr_pids = [p for p in final_accession_pid_dict.values() if p is not None]
        unique_mapped_bdr_pids = set(mapped_bdr_pids)
        if len(unique_mapped_bdr_pids) != len(mapped_bdr_pids):
            print(f'***duplicate bdr pids mapped to accession numbers')
            pid_counts = {}
            for pid in mapped_bdr_pids:
                #https://stackoverflow.com/a/37934666
                try:
                    pid_counts[pid] += 1
                except KeyError:
                    pid_counts[pid] = 1
            for pid, count in pid_counts.items():
                if count > 1:
                    print(f'  {pid}: {count}')
        return final_accession_pid_dict

    def _output_json( self, final_accession_pid_dict, output_json_path ):
        output_dict = {
            'count': self.__run_output_counts( final_accession_pid_dict ),
            'datetime': str( datetime.datetime.now() ),
            'final_accession_pid_dict': final_accession_pid_dict }
        jstring = json.dumps( output_dict, sort_keys=True, indent=2 )
        with open( output_json_path, 'w' ) as f:
            f.write( jstring )

    def __run_output_counts( self, final_accession_pid_dict ):
        """ Takes final dict.
            Creates and returns output counts dict.
            Called by self._output_json() """
        count_items = len( final_accession_pid_dict )
        count_pids = 0
        count_null = 0
        for (accession_number, pid) in final_accession_pid_dict.items():
            if pid == None:
                count_null += 1
            else:
                count_pids += 1
        return { 'count_items': count_items, 'count_pids': count_pids, 'count_null': count_null }

    # end class PidFinder()


## runners ##

def run_create_acc_num_to_pid_map(env='prod'):
    pid_finder = PidFinder(env)
    pid_finder.create_acc_num_to_pid_map()


if __name__ == '__main__':
    """ Assumes env is activated.
        ( 'ANTP' used as a namespace prefix for this 'acc_num_to_pid.py' file. ) """
    run_create_acc_num_to_pid_map()
