# -*- coding: utf-8 -*-

import json, os, pprint, sys, urllib
import requests

from bell_code import bell_logger
from bell_code.tasks.fedora_parts_builder import ImageBuilder
from bell_code.tasks import task_manager


class Task( object ):
    """ Updates fedora metadata (NOT REALLY, YET) and adds an image. """

    def __init__( self ):
        self.accession_number = None
        self.logger = None

    def update_existing_metadata_and_create_image( self,
        MASTER_IMAGES_DIR_PATH, MASTER_IMAGES_DIR_URL, JP2_IMAGES_DIR_PATH, JP2_IMAGES_DIR_URL,
        pid, item_data_dict, API_URL, logger=None
        ):
        """ CONTROLLER
            Note: item_data_dict is a json entry from foundation/acc_num_to_data.py json """
        #Store logger if exists
        if logger:
            self.logger = logger
        #Store accession number
        self.accession_number = item_data_dict[u'calc_accession_id']  # for logging
        #
        #Setup builders
        image_builder = ImageBuilder( logger )
        #
        #Create jp2
        master_filename_raw = item_data_dict[u'object_image_scan_filename']  # includes spaces
        master_filename_utf8 = master_filename_raw.encode( u'utf-8' )
        master_filename_encoded = urllib.quote( master_filename_utf8 ).decode( u'utf-8' )
        source_filepath = u'%s/%s' % ( MASTER_IMAGES_DIR_PATH, master_filename_raw )
        self.logger.info( u'in fedora_metadata_updater_and_image_builder.update_existing_metadata_and_create_image(); source_filepath, %s' % source_filepath )
        temp_jp2_filename = master_filename_raw.replace( u' ', u'_' )
        jp2_filename = temp_jp2_filename[0:-4] + u'.jp2'
        destination_filepath = u'%s/%s' % ( JP2_IMAGES_DIR_PATH, jp2_filename )
        self.logger.info( u'in fedora_metadata_updater_and_image_builder.update_existing_metadata_and_create_image(); destination_filepath, %s' % destination_filepath )
        image_builder.create_jp2( source_filepath, destination_filepath )
        print u'- jp2 created.'
        #
        #Prepare api call
        master_url = u'%s/%s' % ( MASTER_IMAGES_DIR_URL, master_filename_encoded )
        jp2_url = u'%s/%s' % ( JP2_IMAGES_DIR_URL, jp2_filename )
        params = { u'pid': pid }
        params[u'content_streams'] = json.dumps([
            { u'dsID': u'MASTER', u'url': master_url },
            { u'dsID': u'JP2', u'url': jp2_url }
            ])
        print u'api params prepared'
        #
        #Make api call
        r = requests.put( API_URL, data=params, verify=False )
        print u'http put executed'
        self.logger.debug( u'in fedora_metadata_updater_and_image_builder.update_existing_metadata_and_create_image(); r.status_code, %s' % r.status_code )
        self.logger.debug( u'in fedora_metadata_updater_and_image_builder.update_existing_metadata_and_create_image(); r.content, %s' % r.content.decode(u'utf-8') )
        #
        #Read response
        resp = r.json()
        self.logger.debug( u'in fedora_metadata_updater_and_image_builder.update_existing_metadata_and_create_image(); api_response, %s' % resp )
        #
        #Update logging
        print u'- done.'
        self._update_task_tracker( message=u'save_successful' )
        print u'response: `%s` logged' % resp
        #
        #Set next task
        task_manager.determine_next_task(
            unicode(sys._getframe().f_code.co_name),
            data={ u'item_data': item_data_dict, u'jp2_path': destination_filepath, u'pid': pid },
            logger=logger
            )
        print u'- next task set.'
        # done
        return

    def _update_task_tracker( self, message ):
        """ Updates redis 'bell:tracker' accession number entry. """
        try:
            from tasks import task_manager
            task_manager.update_tracker( key=self.accession_number, message=message )
        except Exception as e:
            print u'- in fedora_metadata_updater_and image_builder.Task._update_task_tracker(); exception: %s' % unicode(repr(e))
            pass
        return

    def _print_settings( self,
        MASTER_IMAGES_DIR_PATH, MASTER_IMAGES_DIR_URL, JP2_IMAGES_DIR_PATH, JP2_IMAGES_DIR_URL,
        API_URL
        ):
        """ Outputs settings derived from environmental variables for development. """
        print u'- settings...'
        print u'- MASTER_IMAGES_DIR_PATH: %s' % MASTER_IMAGES_DIR_PATH
        print u'- MASTER_IMAGES_DIR_URL: %s' % MASTER_IMAGES_DIR_URL
        print u'- JP2_IMAGES_DIR_PATH: %s' % JP2_IMAGES_DIR_PATH
        print u'- JP2_IMAGES_DIR_URL: %s' % JP2_IMAGES_DIR_URL
        print u'- API_URL: %s' % API_URL
        print u'---'
        return

    # end class Task()


def run__update_existing_metadata_and_create_image( data ):
    """ Takes data-dict; example { item_dict: {title: the-title, acc-num: the-acc-num} }.
        Instantiates Task() instance & calls update_existing_metadata_and_create_image().
        Called by task_manager.determine_next_task(). """
    logger = bell_logger.setup_logger()
    logger.info( u'in fedora_metadata_updater_and_image_builder.run__update_existing_metadata_and_create_image(); starting.' )
    print u'in fedora_metadata_and_image_builder.run__update_existing_metadata_and_create_image(); acc_num is: %s' % data[u'item_dict'][u'calc_accession_id']
    MASTER_IMAGES_DIR_PATH = unicode( os.environ.get(u'BELL_FMAIB__MASTER_IMAGES_DIR_PATH') )
    MASTER_IMAGES_DIR_URL = unicode( os.environ.get(u'BELL_FMAIB__MASTER_IMAGES_DIR_URL'))
    JP2_IMAGES_DIR_PATH = unicode( os.environ.get(u'BELL_FMAIB__JP2_IMAGES_DIR_PATH') )
    JP2_IMAGES_DIR_URL = unicode( os.environ.get(u'BELL_FMAIB__JP2_IMAGES_DIR_URL') )
    API_URL = unicode( os.environ.get(u'BELL_FMUAIB__PRIVATE_API_URL') )
    mods_schema_path = os.path.abspath( u'./lib/mods-3-4.xsd' )
    task = Task()
    task._print_settings(
        MASTER_IMAGES_DIR_PATH, MASTER_IMAGES_DIR_URL, JP2_IMAGES_DIR_PATH, JP2_IMAGES_DIR_URL,
        API_URL
        )
    task.update_existing_metadata_and_create_image(
        MASTER_IMAGES_DIR_PATH, MASTER_IMAGES_DIR_URL, JP2_IMAGES_DIR_PATH, JP2_IMAGES_DIR_URL,
        data[u'pid'], data[u'item_dict'], API_URL, logger
        )
    print u'in edora_metadata_updater_and_image_builder.run__update_existing_metadata_and_create_image(); acc_num is: %s; item ingested' % data[u'item_dict'][u'calc_accession_id']
    return




if __name__ == u'__main__':
    """ __main__ used for development-only.
        Note: data values are a mishmash from various records -- this is for testing. """
    data = {
        # u'pid': u'TBD',
        u'pid': u'test:227864',
        u'handler': u'add_new_item_with_image',
        u'item_dict': {
            u'ARTISTS::artist_alias': [None],
            u'ARTISTS::artist_birth_country_id': [u'230'],
            u'ARTISTS::artist_birth_year': [u'1930'],
            u'ARTISTS::artist_death_year': [u'1993'],
            u'ARTISTS::artist_first_name': [u'Elisabeth'],
            u'ARTISTS::artist_last_name': [u'Frink'],
            u'ARTISTS::artist_lifetime': [u'1930-1993'],
            u'ARTISTS::artist_middle_name': [None],
            u'ARTISTS::artist_nationality_name': [u'British'],
            u'ARTISTS::calc_artist_full_name': [u'Elisabeth Frink'],
            u'ARTISTS::calc_nationality': [u'British'],
            u'ARTISTS::use_alias_flag': [None],
            u'MEDIA::object_medium_name': u'Book',
            u'MEDIA_SUB::sub_media_name': [u'Letterpress', u'Lithograph'],
            u'OBJECT_ARTISTS::artist_id': [u'105'],
            u'OBJECT_ARTISTS::artist_role': [None],
            u'OBJECT_ARTISTS::primary_flag': [u'yes'],
            u'OBJECT_MEDIA_SUB::media_sub_id': [u'32', u'35'],
            u'SERIES::series_end_year': None,
            u'SERIES::series_name': None,
            u'SERIES::series_start_year': None,
            # u'calc_accession_id': u'B 1980.1562',
            u'calc_accession_id': u'B 2013.1',
            u'credit_line': u'Gift of Saul P. Steinberg',
            u'image_height': None,
            u'image_width': None,
            u'object_date': u'1968',
            u'object_depth': None,
            u'object_height': None,
            u'object_id': u'182',
            u'object_image_scan_filename': u'Frink B_1968.1562.tif',  # not real scan for this test acc-num
            u'object_medium': u'Letterpress and lithography',
            # u'object_title': u"Aesop's Fables",
            u'object_title': u'Istanbul Accordian...',
            u'object_width': None,
            u'object_year_end': u'1968',
            u'object_year_start': u'1968',
            u'series_id': None}
        }
    run__update_existing_metadata_and_create_image( data )
