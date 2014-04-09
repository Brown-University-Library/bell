# -*- coding: utf-8 -*-

import os
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
