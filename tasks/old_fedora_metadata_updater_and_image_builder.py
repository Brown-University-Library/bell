# -*- coding: utf-8 -*-

from __future__ import unicode_literals

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
        self.accession_number = item_data_dict['calc_accession_id']  # for logging
        #
        #Setup builders
        image_builder = ImageBuilder( logger )
        #
        #Create jp2
        master_filename_raw = item_data_dict['object_image_scan_filename']  # includes spaces
        master_filename_utf8 = master_filename_raw.encode( 'utf-8' )
        master_filename_encoded = urllib.quote( master_filename_utf8 ).decode( 'utf-8' )
        source_filepath = '%s/%s' % ( MASTER_IMAGES_DIR_PATH, master_filename_raw )
        self.logger.info( 'in fedora_metadata_updater_and_image_builder.update_existing_metadata_and_create_image(); source_filepath, %s' % source_filepath )
        temp_jp2_filename = master_filename_raw.replace( ' ', '_' )
        jp2_filename = temp_jp2_filename[0:-4] + '.jp2'
        destination_filepath = '%s/%s' % ( JP2_IMAGES_DIR_PATH, jp2_filename )
        self.logger.info( 'in fedora_metadata_updater_and_image_builder.update_existing_metadata_and_create_image(); destination_filepath, %s' % destination_filepath )
        image_builder.create_jp2( source_filepath, destination_filepath )
        print '- jp2 created.'
        #
        #Prepare api call
        master_url = '%s/%s' % ( MASTER_IMAGES_DIR_URL, master_filename_encoded )
        jp2_url = '%s/%s' % ( JP2_IMAGES_DIR_URL, jp2_filename )
        params = { 'pid': pid }
        params['content_streams'] = json.dumps([
            { 'dsID': 'MASTER', 'url': master_url },
            { 'dsID': 'JP2', 'url': jp2_url }
            ])
        print 'api params prepared'
        #
        #Make api call
        r = requests.put( API_URL, data=params, verify=False )
        print 'http put executed'
        self.logger.debug( 'in fedora_metadata_updater_and_image_builder.update_existing_metadata_and_create_image(); r.status_code, %s' % r.status_code )
        self.logger.debug( 'in fedora_metadata_updater_and_image_builder.update_existing_metadata_and_create_image(); r.content, %s' % r.content.decode('utf-8') )
        #
        #Read response
        resp = r.json()
        self.logger.debug( 'in fedora_metadata_updater_and_image_builder.update_existing_metadata_and_create_image(); api_response, %s' % resp )
        #
        #Update logging
        print '- done.'
        self._update_task_tracker( message='save_successful' )
        print 'response: `%s` logged' % resp
        #
        #Set next task
        task_manager.determine_next_task(
            unicode(sys._getframe().f_code.co_name),
            data={ 'item_data': item_data_dict, 'jp2_path': destination_filepath, 'pid': pid },
            logger=logger
            )
        print '- next task set.'
        # done
        return

    def _update_task_tracker( self, message ):
        """ Updates redis 'bell:tracker' accession number entry. """
        try:
            from tasks import task_manager
            task_manager.update_tracker( key=self.accession_number, message=message )
        except Exception as e:
            print '- in fedora_metadata_updater_and image_builder.Task._update_task_tracker(); exception: %s' % unicode(repr(e))
            pass
        return

    def _print_settings( self,
        MASTER_IMAGES_DIR_PATH, MASTER_IMAGES_DIR_URL, JP2_IMAGES_DIR_PATH, JP2_IMAGES_DIR_URL,
        API_URL
        ):
        """ Outputs settings derived from environmental variables for development. """
        print '- settings...'
        print '- MASTER_IMAGES_DIR_PATH: %s' % MASTER_IMAGES_DIR_PATH
        print '- MASTER_IMAGES_DIR_URL: %s' % MASTER_IMAGES_DIR_URL
        print '- JP2_IMAGES_DIR_PATH: %s' % JP2_IMAGES_DIR_PATH
        print '- JP2_IMAGES_DIR_URL: %s' % JP2_IMAGES_DIR_URL
        print '- API_URL: %s' % API_URL
        print '---'
        return

    # end class Task()


def run__update_existing_metadata_and_create_image( data ):
    """ Takes data-dict; example { item_dict: {title: the-title, acc-num: the-acc-num} }.
        Instantiates Task() instance & calls update_existing_metadata_and_create_image().
        Called by task_manager.determine_next_task(). """
    logger = bell_logger.setup_logger()
    logger.info( 'in fedora_metadata_updater_and_image_builder.run__update_existing_metadata_and_create_image(); starting.' )
    print 'in fedora_metadata_and_image_builder.run__update_existing_metadata_and_create_image(); acc_num is: %s' % data['item_dict']['calc_accession_id']
    MASTER_IMAGES_DIR_PATH = unicode( os.environ.get('BELL_FMAIB__MASTER_IMAGES_DIR_PATH') )
    MASTER_IMAGES_DIR_URL = unicode( os.environ.get('BELL_FMAIB__MASTER_IMAGES_DIR_URL'))
    JP2_IMAGES_DIR_PATH = unicode( os.environ.get('BELL_FMAIB__JP2_IMAGES_DIR_PATH') )
    JP2_IMAGES_DIR_URL = unicode( os.environ.get('BELL_FMAIB__JP2_IMAGES_DIR_URL') )
    API_URL = unicode( os.environ.get('BELL_FMUAIB__PRIVATE_API_URL') )
    mods_schema_path = os.path.abspath( './lib/mods-3-4.xsd' )
    task = Task()
    task._print_settings(
        MASTER_IMAGES_DIR_PATH, MASTER_IMAGES_DIR_URL, JP2_IMAGES_DIR_PATH, JP2_IMAGES_DIR_URL,
        API_URL
        )
    task.update_existing_metadata_and_create_image(
        MASTER_IMAGES_DIR_PATH, MASTER_IMAGES_DIR_URL, JP2_IMAGES_DIR_PATH, JP2_IMAGES_DIR_URL,
        data['pid'], data['item_dict'], API_URL, logger
        )
    print 'in edora_metadata_updater_and_image_builder.run__update_existing_metadata_and_create_image(); acc_num is: %s; item ingested' % data['item_dict']['calc_accession_id']
    return




if __name__ == '__main__':
    """ __main__ used for development-only.
        Note: data values are a mishmash from various records -- this is for testing. """
    data = {
        # 'pid': 'TBD',
        'pid': 'test:227864',
        'handler': 'add_new_item_with_image',
        'item_dict': {
            'ARTISTS::artist_alias': [None],
            'ARTISTS::artist_birth_country_id': ['230'],
            'ARTISTS::artist_birth_year': ['1930'],
            'ARTISTS::artist_death_year': ['1993'],
            'ARTISTS::artist_first_name': ['Elisabeth'],
            'ARTISTS::artist_last_name': ['Frink'],
            'ARTISTS::artist_lifetime': ['1930-1993'],
            'ARTISTS::artist_middle_name': [None],
            'ARTISTS::artist_nationality_name': ['British'],
            'ARTISTS::calc_artist_full_name': ['Elisabeth Frink'],
            'ARTISTS::calc_nationality': ['British'],
            'ARTISTS::use_alias_flag': [None],
            'MEDIA::object_medium_name': 'Book',
            'MEDIA_SUB::sub_media_name': ['Letterpress', 'Lithograph'],
            'OBJECT_ARTISTS::artist_id': ['105'],
            'OBJECT_ARTISTS::artist_role': [None],
            'OBJECT_ARTISTS::primary_flag': ['yes'],
            'OBJECT_MEDIA_SUB::media_sub_id': ['32', '35'],
            'SERIES::series_end_year': None,
            'SERIES::series_name': None,
            'SERIES::series_start_year': None,
            # 'calc_accession_id': 'B 1980.1562',
            'calc_accession_id': 'B 2013.1',
            'credit_line': 'Gift of Saul P. Steinberg',
            'image_height': None,
            'image_width': None,
            'object_date': '1968',
            'object_depth': None,
            'object_height': None,
            'object_id': '182',
            'object_image_scan_filename': 'Frink B_1968.1562.tif',  # not real scan for this test acc-num
            'object_medium': 'Letterpress and lithography',
            # 'object_title': u"Aesop's Fables",
            'object_title': 'Istanbul Accordian...',
            'object_width': None,
            'object_year_end': '1968',
            'object_year_start': '1968',
            'series_id': None}
        }
    run__update_existing_metadata_and_create_image( data )
