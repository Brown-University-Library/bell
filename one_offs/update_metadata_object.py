# -*- coding: utf-8 -*-

from __future__ import unicode_literals

""" Calls tasks.metadata.py functions to do work. """

import json, logging, os
from bell_code.tasks import metadata
from bell_code.utils import logger_setup


logger = logging.getLogger( 'bell_logger' )
logger_setup.check_log_handler()


def update_metadata():
    """ Updates existing metadata for a given pid and accession-number.
        Assumes object already exists.
        Called manually. """
    logger.debug( 'starting caller' )
    ( ACCESSION_NUMBER, PID ) = (
        unicode(os.environ['BELL_ONEOFF_UPDATE_METADATA__ACCESSION_NUMBER']),
        unicode(os.environ['BELL_ONEOFF_UPDATE_METADATA__PID']) )
    metadata_updater = metadata.MetadataUpdater()
    metadata_updater.update_object_metadata( ACCESSION_NUMBER, PID )
    return




if __name__ == '__main__':
    update_metadata()
