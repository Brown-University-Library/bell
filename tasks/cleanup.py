# -*- coding: utf-8 -*-

import os
from bell_code import bell_logger


def delete_jp2( item_data_dict, jp2_path, pid ):
    """ Cleans up created derivative. """
    logger = bell_logger.setup_logger()
    assert jp2_path[-4:] == u'.jp2'
    os.remove( jp2_path )
    task_manager.determine_next_task(
        unicode(sys._getframe().f_code.co_name),
        data={ u'item_data': item_data_dict, u'pid': pid },
        logger=logger
        )
    return
