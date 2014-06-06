# -*- coding: utf-8 -*-

""" Forces rebuild of thumbnails.
    To run:
    - activate virtual environment
    - run this script. """

import json, os, pprint, time
import redis, requests, rq


class ThumbnailRebuilder( object ):

    ## get pids ##

    def get_pid_list( self, bell_collection_pid, solr_root_url ):
        """ Returns list of pids for giving collection-pid. """
        doc_list = self._run_studio_solr_query( bell_collection_pid, solr_root_url )
        pid_list = self._build_pidlist( doc_list )
        # pprint.pprint( pid_list )

    def _run_studio_solr_query( self, bdr_collection_pid, solr_root_url ):
        """ Returns _solr_ doc list.
            Example solr url: 'https://solr-url/?q=rel_is_member_of_ssim:"collection:pid"&start=x&rows=y&fl=pid,mods_id_bell_accession_number_ssim,primary_title'
            Example result: [ {pid:bdr123, identifier:[acc_num_a,other_num_b], mods_id_bell_accession_number_ssim:None_or_acc_num_a, other:...}, etc. ]
            Called by get_pid_list(). """
        doc_list = []
        for i in range( 100 ):  # would handle 50,000 records
            data_dict = self.__query_solr( i, bdr_collection_pid, solr_root_url )
            docs = data_dict[u'response'][u'docs']
            doc_list.extend( docs )
            if not len( docs ) > 0:
                break
        # self.logger.info( u'in _run_studio_solr_query(); doc_list, %s' % pprint.pformat(doc_list) )
        return doc_list

    def __query_solr( self, i, bell_collection_pid, solr_root_url ):
        """ Queries solr for iterating start-row.
            Returns results dict.
            Called by self._run_studio_solr_query() """
        new_start = i * 500  # for solr start=i parameter (cool, eh?)
        params = {
            u'q': u'rel_is_member_of_ssim:"%s"' % bell_collection_pid,
            u'fl': u'pid,mods_id_bell_accession_number_ssim,primary_title,datastreams_ss',
            u'rows': 500, u'start': new_start, u'wt': u'json' }
        r = requests.get( solr_root_url, params=params, verify=False )
        data_dict = json.loads( r.content.decode(u'utf-8', u'replace') )
        time.sleep( .1 )
        return data_dict

    def _build_pidlist( self, doc_list ):
        """ Returns list of pids for docs that contain a jp2 datastream.
            Called by get_pid_list(). """
        # print u'- len(doc_list), `%s`' % len( doc_list )
        pid_list = []
        for doc in doc_list:
            jstring = doc.get( u'datastreams_ss' )
            if jstring:
                d = json.loads( jstring )
            if d.get( u'JP2' ) == {u"mimeType": u"image/jp2"}:
                pid_list.append( doc[u'pid'] )
        # print u'- len(pid_list), `%s`' % len( pid_list )
        return pid_list


    ## enqueue jobs ##

    def enqueue_job( self, pid ):
        thumbnail_q = rq.Queue( u'thumbnails', connection=redis.Redis() )
        thumbnail_q.enqueue_call(
            func=u'thumbnail_creator.thumbnails.create_thumbnail',
            kwargs={ u'pid': pid, u'force': True }
            )
        return

    # end class ThumbnailRebuilder()




if __name__ == u'__main__':
    bell_collection_pid = os.environ[u'BJD_TEMP_BELL_COLLECTION_PID']
    solr_root_url = os.environ[u'BJD_TEMP_BELL_SOLR_ROOT_URL'] + u'/search/'
    rebuilder = ThumbnailRebuilder()
    pid_list = rebuilder.get_pid_list( bell_collection_pid, solr_root_url )
    # for pid in pid_list:
    #     rebuilder.enqueue_job( pid )