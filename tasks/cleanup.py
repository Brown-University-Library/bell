# -*- coding: utf-8 -*-

import os, sys
from bell_code import bell_logger
from bell_code.tasks import task_manager


def delete_jp2( data ):
    """ Cleans up created derivative. """
    logger = bell_logger.setup_logger()
    ( item_data_dict, jp2_path, pid ) = ( data[u'item_data'], data[u'jp2_path'], data[u'pid'] )
    assert jp2_path[-4:] == u'.jp2'
    os.remove( jp2_path )
    task_manager.determine_next_task(
        unicode(sys._getframe().f_code.co_name),
        data={ u'item_data': item_data_dict, u'pid': pid },
        logger=logger
        )
    return


class BdrPidMaker( object ):
    """ Manages preparation of pid-list from bdr api. """

    def __init__( self, logger ):
        self.logger = logger
        self.COLLECTION_PID = unicode( os.environ[u'BELL_TASKS_IDXR__COLLECTION_PID'] )
        self.SEARCH_API_ROOT_URL = u'%s/search/' % unicode( os.environ[u'BELL_TASKS_IDXR__BDR_API_ROOT_URL'] )
        self.OUTPUT_PATH = unicode( os.environ[u'BELL_TASKS_IDXR__BDR_PIDS_JSON_PATH'] )

    def grab_bdr_pids( self ):
        """ Creates a pid list from the given collection-pid.
            Called when __main__ detects the pid-list [u'all']  """
        pid_list = []
        for i in range( 100 ):  # would handle 50,000 records
            data_dict = self.query_solr( i, self.COLLECTION_PID )
            ( pid_list, docs ) = self.update_pid_list( data_dict, pid_list )
            if not len( docs ) > 0:
                break
        self.logger.debug( u'in indexer.BdrPidMaker.grab_bdr_pids(); pid_list, %s' % pprint.pformat(pid_list[0:10]) )
        self.output_list( sorted(pid_list) )

    def query_solr( self, i, collection_pid ):
        """ Queries solr for iterating start-row.
            Returns results dict.
            Called by grab_bdr_pids() """
        time.sleep( .5 )
        new_start = i * 500  # for solr start=i parameter (cool, eh?)
        params = {
            u'q': u'rel_is_member_of_ssim:"%s"' % collection_pid,
            u'fl': u'pid,mods_id_bell_accession_number_ssim,primary_title',
            u'rows': 500, u'start': new_start, u'wt': u'json' }
        r = requests.get( self.SEARCH_API_ROOT_URL, params=params, verify=False )
        self.logger.info( u'in utils.update_custom_index._query_solr(); r.url, %s' % r.url )
        data_dict = json.loads( r.content.decode(u'utf-8', u'replace') )
        self.logger.info( u'in indexer.BdrPidMaker.query_solr(); data_dict, %s' % pprint.pformat(data_dict) )
        return data_dict

    def update_pid_list( self, data_dict, pid_list ):
        """ Updates pid_list with new set of data from solr query.
            Called by grab_bdr_pids() """
        docs = data_dict[u'response'][u'docs']
        for doc in docs:
            pid = doc[u'pid']
            pid_list.append( pid )
        return ( pid_list, docs )

    def output_list( self, pid_list ):
        """ Saves json file.
            Called by grab_bdr_pids() """
        jsn = json.dumps( pid_list, indent=2, sort_keys=True )
        with open( self.OUTPUT_PATH, u'w' ) as f:
            f.write( jsn )
        return

    # end class BdrPidMaker
