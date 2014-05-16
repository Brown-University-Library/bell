# -*- coding: utf-8 -*-

import logging, logging.handlers, os

def setup_logger():
    """ Configures a logger to write to console & <filename>. """
    formatter = logging.Formatter( u'[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s' )
    logger = logging.getLogger(u'bell_logger')
    logger.setLevel(logging.DEBUG)
    filename = u'%s/bell.log' % os.environ.get(u'BELL_LOG_DIR')
    file_handler = logging.handlers.RotatingFileHandler( filename, maxBytes=(5*1024*1024), backupCount=2 )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    # console_handler = logging.StreamHandler()
    # console_handler.setLevel(logging.DEBUG)
    # console_formatter = logging.Formatter(u'%(levelname)s - %(message)s')
    # console_handler.setFormatter(console_formatter)
    # logger.addHandler(console_handler)
    return logger
