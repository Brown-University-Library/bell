# -*- coding: utf-8 -*-

from __future__ import unicode_literals

""" Configures logger if needed. """

import json, logging, logging.config, os


def check_log_handler():
    if not logging._handlers:  # true when module accessed by queue-jobs
        worker_config_dct = json.loads( os.environ['BELL_LOG_CONFIG_JSON'] )
        logging.config.dictConfig( worker_config_dct )
        return
