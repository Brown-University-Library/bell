# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import logging, logging.handlers, os


def setup_logger():
    """ Configures a logger to write to console & <filename>. """
    formatter = logging.Formatter( '[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s' )
    logger = logging.getLogger('bell_logger')
    logger.setLevel(logging.DEBUG)
    filename = '%s/bell.log' % os.environ.get('BELL_LOG_DIR')
    file_handler = logging.handlers.RotatingFileHandler( filename, maxBytes=(2*1024*1024), backupCount=2 )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    # console_handler = logging.StreamHandler()
    # console_handler.setLevel(logging.DEBUG)
    # console_formatter = logging.Formatter('%(levelname)s - %(message)s')
    # console_handler.setFormatter(console_formatter)
    # logger.addHandler(console_handler)
    return logger
