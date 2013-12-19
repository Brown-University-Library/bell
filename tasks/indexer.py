# -*- coding: utf-8 -*-

import bell_logger


def index_metadata_only( item_dict, pid ):
    """ Indexes bell-custom-solr index using just basic item-dict data and pid; no need to check image info.
        Commonly called after fedora_metadata_only_builder.run__create_fedora_metadata_object() task. """
    logger = bell_logger.setup_logger()
    logger.info( u'in indexer.index_metadata_only(); acc_num , %s; pid, %s; processing will go here.' % (item_dict[u'calc_accession_id'], pid) )
    return
