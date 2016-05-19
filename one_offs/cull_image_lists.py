# -*- coding: utf-8 -*-

from __future__ import unicode_literals

""" Creates a `data/g__images_to_process_CULLED.json` file, with a subset of the `data/g__images_to_process.json` info. """

import datetime, json, logging, os, pprint


## set up file logger
LOG_PATH = '{log_dir}/bell.log'.format( log_dir=unicode(os.environ['BELL_LOG_DIR']) )
logging.basicConfig(
    filename=LOG_PATH, level=logging.DEBUG,
    format='[%(asctime)s] %(levelname)s [%(module)s-%(funcName)s()::%(lineno)d] %(message)s', datefmt='%d/%b/%Y %H:%M:%S' )
logger = logging.getLogger(__name__)


## constants
NEW_IMAGE_LISTS_JSON_PATH = unicode( os.environ['BELL_TASKS_IMGS__IMAGES_TO_PROCESS_OUTPUT_PATH'] )
OLD_IMAGE_LISTS_JSON_PATH = unicode( os.environ['BELL_ONEOFF__OLD_IMAGES_TO_PROCESS_OUTPUT_PATH'] )
OUTPUT_JSON_PATH = unicode( os.environ['BELL_ONEOFF__CULLED_IMAGES_TO_PROCESS_OUTPUT_PATH'] )


def cull_image_lists():
    """ Creates a data/g__images_to_process_CULLED.json file,
            with a subset of the data/g__images_to_process.json info.
        Use-case:
        - bunch of images are ingested.
        - some new images neeed to be ingested causing steps to be re-run -- but now most of the images that have already
          been ingested will now be listed as needing to be updated.
        - the 'culled' list are the new image-lists to use -- they can either replace the existing 'g__images_to_process.json',
          or the environmental variable pointing to that file can instead point to the CULLED file.
        Called manually. """
    logger.debug( 'starting' )
    images_already_processed = grab_old_data()
    ( preculled_images_to_add, preculled_images_to_update ) = grab_new_data()
    logger.debug( 'about to get new_images_to_add' )
    new_images_to_add = perform_cull( images_already_processed, preculled_images_to_add )
    logger.debug( 'about to get new_images_to_update' )
    new_images_to_update = perform_cull( images_already_processed, preculled_images_to_update )
    output_data( new_images_to_add, new_images_to_update )
    return


def grab_old_data():
    """ Loads up pre-existing image lists.
        Called by cull_image_lists() """
    with open( OLD_IMAGE_LISTS_JSON_PATH ) as f:
        dct = json.loads( f.read() )
    images_already_added = dct['lst_images_to_add']
    images_already_updated = dct['lst_images_to_update']
    images_already_processed = images_already_added + images_already_updated
    logger.debug( 'images_already_processed, ```{}```'.format(pprint.pformat(images_already_processed)) )
    return images_already_processed


def grab_new_data():
    """ Loads up new, pre-culled image-lists.
        Called by cull_image_lists() """
    with open( NEW_IMAGE_LISTS_JSON_PATH ) as f:
        dct = json.loads( f.read() )
    preculled_images_to_add = dct['lst_images_to_add']
    preculled_images_to_update = dct['lst_images_to_update']
    logger.debug( 'returning `{}` images-to-add, and `{}` images-to-update'.format(len(preculled_images_to_add), len(preculled_images_to_update)) )
    return ( preculled_images_to_add, preculled_images_to_update )


def perform_cull( images_already_processed, preculled_images ):
    """ Returns list of images that does not include any images in `images_already_processed`.
        Called by cull_image_lists() """
    culled_images = []
    skipped_preculled_entries = []
    for entry_dct in preculled_images:
        if entry_dct in images_already_processed:
            skipped_preculled_entries.append( entry_dct )
        else:
            culled_images.append( entry_dct )
    logger.debug( 'returning `{}` culled images; skipping `{}` already-ingested images'.format(len(culled_images), len(skipped_preculled_entries)) )
    return culled_images


def output_data( images_to_add, images_to_update ):
    """ Saves json file.
        Called by cull_image_lists() """
    data = {
        'datetime': unicode( datetime.datetime.now() ),
        'count_images': 'irrelevant',
        'count_images_processed': len( images_to_add ) + len( images_to_update ),
        'count_images_to_add': len( images_to_add ),
        'count_images_to_update': len( images_to_update ),
        'lst_images_to_add': images_to_add,
        'lst_images_to_update': images_to_update }
    jsn = json.dumps( data, indent=2, sort_keys=True )
    with open( OUTPUT_JSON_PATH, 'w' ) as f:
        f.write( jsn )
    return



if __name__ == '__main__':
    cull_image_lists()
