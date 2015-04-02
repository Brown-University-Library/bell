# -*- coding: utf-8 -*-

""" Calls tasks.images.py functions to do work. """

# import imghdr, json, logging, os, pprint
import os
import logging.handlers
import requests
from bell_code import bell_logger
from bell_code.tasks import images

logger = bell_logger.setup_logger()


def make_jp2_and_overwrite_existing_image():
    """ Ingests an image.
        Assumes metadata object already exists.
        Note, run on dev server for jp2 kakadu creation.
        Note, tasks.images.ImageAdder() needs its own set of environmental variables.
        Called manually. """
    ( RAW_MASTER_FILENAME, ACCESSION_NUMBER, PID ) = (
        unicode(os.environ[u'BELL_ONEOFF_ADD_IMAGE__RAW_MASTER_FILENAME']),
        unicode(os.environ[u'BELL_ONEOFF_ADD_IMAGE__ACCESSION_NUMBER']),
        unicode(os.environ[u'BELL_ONEOFF_ADD_IMAGE__PID']) )
    filename_dct = {
        RAW_MASTER_FILENAME: {u'accession_number': ACCESSION_NUMBER, u'pid': PID} }
    adder = images.ImageAdder( logger )
    adder.add_image( filename_dct )
    return




if __name__ == u'__main__':
    make_jp2_and_overwrite_existing_image()
