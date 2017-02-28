# -*- coding: utf-8 -*-

from __future__ import unicode_literals

""" Calls tasks.metadata.py functions to do work. """

import json, logging, os
from bell_code.tasks import metadata


logger = logging.getLogger(__name__)
if not logging._handlers:  # true when module accessed by queue-jobs
    worker_config_dct = json.loads( os.environ['BELL_LOG_CONFIG_JSON'] )
    logging.config.dictConfig( worker_config_dct )



def update_metadata():
    """ Updates existing metadata for a given pid and accession-number.
        Assumes object already exists.
        Called manually. """
    ( ACCESSION_NUMBER, PID ) = (
        unicode(os.environ['BELL_ONEOFF_UPDATE_METADATA__ACCESSION_NUMBER']),
        unicode(os.environ['BELL_ONEOFF_UPDATE_METADATA__PID']) )
    metadata_updater = metadata.MetadataUpdater()
    metadata_updater.update_object_metadata( ACCESSION_NUMBER, PID )
    return




if __name__ == '__main__':
    update_metadata()
