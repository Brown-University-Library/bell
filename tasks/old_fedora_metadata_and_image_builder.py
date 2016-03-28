# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import json, os, pprint, sys

extra_path = os.path.abspath( './' )  # adds bell_code to path
sys.path.append( extra_path )
import bell_logger

import bell_logger
from bdrcmodels.models import JP2Image
from eulfedora.server import Repository
from fedora_parts_builder import ImageBuilder, IRBuilder, ModsBuilder, RightsBuilder
from tasks import task_manager


class Task( object ):
    """ Handles creation of a fedora metadata-only object. """

    def __init__( self ):
        self.accession_number = None
        self.logger = None

    def add_metadata_and_image( self,
        FEDORA_ADMIN_URL, FEDORA_ADMIN_USERNAME, FEDORA_ADMIN_PASSWORD,
        COLLECTION_PID, MASTER_IMAGES_DIR_PATH, MASTER_IMAGES_DIR_URL, JP2_IMAGES_DIR_PATH, JP2_IMAGES_DIR_URL,
        item_data_dict, mods_schema_path, logger=None
        ):
        """ CONTROLLER
            Note: item_data_dict is a json entry from foundation/acc_num_to_data.py json """
        # print 'starting try...'
        # try:

        #Store logger if exists
        if logger:
            self.logger = logger
        #Store accession number
        self.accession_number = item_data_dict['calc_accession_id']  # for logging
        #
        #Setup builders
        image_builder = ImageBuilder( logger )
        ir_builder = IRBuilder()
        mods_builder = ModsBuilder()
        rights_builder = RightsBuilder()
        #
        #Instantiate repo connection
        #Note: doesn't actually try to connect until it has to
        repo = Repository( root=FEDORA_ADMIN_URL, username=FEDORA_ADMIN_USERNAME, password=FEDORA_ADMIN_PASSWORD )
        print '- repo connection instantiated.'
        #
        #Instantiate new-obj
        #Purpose: this will be the object we'll build, then ingest
        new_obj = repo.get_object( type=JP2Image )  # this image model _will_ handle either tiff or jpg
        print '- new_obj instantiated.'
        #
        #Instantiate collection object
        coll_obj = repo.get_object( pid=COLLECTION_PID )
        print '- coll_obj instantiated.'
        #
        #Get/reserve a pid
        #Note: after obtaining pid, new_obj.pid is a value, not a reference
        new_pid = new_obj.pid()
        new_obj.pid = new_pid
        print '- new_obj.pid obtained.'
        #
        #Assign collection object
        new_obj.owning_collection = coll_obj
        print '- collection object assigned'
        #
        #Build rights object
        #Note: builds via bdrxml
        rights_object = rights_builder.build_rights_object()
        print '- rights object built.'
        #
        #Assign rights object
        new_obj.rightsMD.content = rights_object
        print '- rights object assigned.'
        #
        #Build ir object
        ir_object = ir_builder.build_ir_object()
        print '- ir object built.'
        #
        #Assign ir object
        new_obj.irMD.content = ir_object
        print '- ir object assigned.'
        #
        #Build mods object
        #Example returned data: { 'data: mods_object, 'accession_number': accession_number }
        mods_object_dict = mods_builder.build_mods_object( item_data_dict, mods_schema_path, 'return_object' )  # or 'return_string'
        mods_object = mods_object_dict['data']
        print '- mods object built.'
        #
        #Assign mods object
        new_obj.mods.content = mods_object
        print '- mods object assigned.'
        #
        #Update default admin fields
        new_obj.label = mods_object.title
        new_obj.owner = 'Bell Gallery'
        print '- default admin fields updated.'
        #
        #
        #
        #Update master datastream and rels-int
        master_filename = item_data_dict['object_image_scan_filename']
        ( file_url, dsID, mime_type ) = image_builder.build_master_datastream_vars(
            filename=master_filename, image_dir_url=MASTER_IMAGES_DIR_URL )
        new_obj = image_builder.update_object_datastream( new_obj, dsID, file_url, mime_type )
        new_obj = image_builder.update_newobj_relsint(
            new_obj=new_obj, filename=master_filename, dsID=dsID )
        print '- master datastream and rels-int fields updated.'
        #
        #
        #
        #Create jp2
        source_filepath = '%s/%s' % ( MASTER_IMAGES_DIR_PATH, master_filename )
        self.logger.info( 'in fedora_metadata_and_image_builder.add_metadata_and_image(); source_filepath, %s' % source_filepath )
        temp_jp2_filename = master_filename.replace( ' ', '_' )
        jp2_filename = temp_jp2_filename[0:-4] + '.jp2'
        destination_filepath = '%s/%s' % ( JP2_IMAGES_DIR_PATH, jp2_filename )
        self.logger.info( 'in fedora_metadata_and_image_builder.add_metadata_and_image(); destination_filepath, %s' % destination_filepath )
        image_builder.create_jp2( source_filepath, destination_filepath )
        print '- jp2 created.'
        #
        #
        #
        #Update jp2 datastream and rels-int
        ( file_url, dsID, mime_type ) = image_builder.build_jp2_datastream_vars(
            filename=jp2_filename, image_dir_url=JP2_IMAGES_DIR_URL )
        new_obj = image_builder.update_object_datastream( new_obj, dsID, file_url, mime_type )
        new_obj = image_builder.update_newobj_relsint(
            new_obj=new_obj, filename=jp2_filename, dsID=dsID )
        print '- jp2 datastream and rels-int fields updated.'
        #
        #
        #
        #Save to fedora
        self._save_to_fedora( new_obj )
        print '- saved to fedora.'
        #
        #Update logging
        print '- done.'
        self._update_task_tracker( message='new_pid:%s' % new_pid )
        #
        #Set next task
        task_manager.determine_next_task(
            unicode(sys._getframe().f_code.co_name),
            data={ 'item_data': item_data_dict, 'pid': new_pid },
            logger=logger
            )
        print '- next task set.'
        return

    def _save_to_fedora( self, new_obj ):
        """ Saves object to fedora. """
        try:
          new_obj.save()
          self._update_task_tracker( message='save_successful' )
        except Exception as f:  # except DigitalObjectSaveFailure as f
          error_message = 'error on save: %s' % unicode(repr(f))
          print 'ERROR: %s' % error_message
          self._update_task_tracker( message=error_message )
        return

    def _update_task_tracker( self, message ):
        """ Updates redis 'bell:tracker' accession number entry. """
        try:
            from tasks import task_manager
            task_manager.update_tracker( key=self.accession_number, message=message )
        except Exception as e:
            print '- in fedora_metadata_only_builder.Task._update_task_tracker(); exception: %s' % unicode(repr(e))
            pass
        return

    def _print_settings( self,
        FEDORA_ADMIN_URL, FEDORA_ADMIN_USERNAME, FEDORA_ADMIN_PASSWORD,
        COLLECTION_PID, MASTER_IMAGES_DIR_PATH, MASTER_IMAGES_DIR_URL, JP2_IMAGES_DIR_PATH, JP2_IMAGES_DIR_URL
        ):
        """ Outputs settings derived from environmental variables for development. """
        print '- settings...'
        print '- FEDORA_ADMIN_URL: %s' % FEDORA_ADMIN_URL
        print '- FEDORA_ADMIN_USERNAME: %s' % FEDORA_ADMIN_USERNAME
        print '- FEDORA_ADMIN_PASSWORD: %s' % FEDORA_ADMIN_PASSWORD
        print '- COLLECTION_PID: %s' % COLLECTION_PID
        print '- MASTER_IMAGES_DIR_PATH: %s' % MASTER_IMAGES_DIR_PATH
        print '- MASTER_IMAGES_DIR_URL: %s' % MASTER_IMAGES_DIR_URL
        print '- JP2_IMAGES_DIR_PATH: %s' % JP2_IMAGES_DIR_PATH
        print '- JP2_IMAGES_DIR_URL: %s' % JP2_IMAGES_DIR_URL
        print '---'
        return

    # end class Task()




def run__add_metadata_and_image( data ):
    """ Takes data-dict; example { item_dict: {title: the-title, acc-num: the-acc-num} }.
        Instantiates Task() instance & calls add_metadata_and_image().
        Called by task_manager.determine_next_task(). """
    logger = bell_logger.setup_logger()
    logger.info( 'in fedora_metadata_and_image_builder.run__add_metadata_and_image(); starting.' )
    print '- in fedora_metadata_and_image_builder.run__add_metadata_and_image(); acc_num is: %s' % data['item_dict']['calc_accession_id']
    FEDORA_ADMIN_URL = unicode( os.environ.get('BELL_FMAIB__FEDORA_ADMIN_URL') )
    FEDORA_ADMIN_USERNAME = unicode( os.environ.get('BELL_FMAIB__FEDORA_ADMIN_USERNAME') )
    FEDORA_ADMIN_PASSWORD = unicode( os.environ.get('BELL_FMAIB__FEDORA_ADMIN_PASSWORD') )
    COLLECTION_PID = unicode( os.environ.get('BELL_FMAIB__COLLECTION_PID') )
    MASTER_IMAGES_DIR_PATH = unicode( os.environ.get('BELL_FMAIB__MASTER_IMAGES_DIR_PATH') )
    MASTER_IMAGES_DIR_URL = unicode( os.environ.get('BELL_FMAIB__MASTER_IMAGES_DIR_URL'))
    JP2_IMAGES_DIR_PATH = unicode( os.environ.get('BELL_FMAIB__JP2_IMAGES_DIR_PATH') )
    JP2_IMAGES_DIR_URL = unicode( os.environ.get('BELL_FMAIB__JP2_IMAGES_DIR_URL') )
    mods_schema_path = os.path.abspath( './lib/mods-3-4.xsd' )
    task = Task()
    task._print_settings(
        FEDORA_ADMIN_URL, FEDORA_ADMIN_USERNAME, FEDORA_ADMIN_PASSWORD,
        COLLECTION_PID, MASTER_IMAGES_DIR_PATH, MASTER_IMAGES_DIR_URL, JP2_IMAGES_DIR_PATH, JP2_IMAGES_DIR_URL
        )
    task.add_metadata_and_image(
        FEDORA_ADMIN_URL, FEDORA_ADMIN_USERNAME, FEDORA_ADMIN_PASSWORD,
        COLLECTION_PID, MASTER_IMAGES_DIR_PATH, MASTER_IMAGES_DIR_URL, JP2_IMAGES_DIR_PATH, JP2_IMAGES_DIR_URL,
        data['item_dict'], mods_schema_path, logger
        )
    print '- in fedora_metadata_and_image_builder.run__add_metadata_and_image(); acc_num is: %s; item ingested' % data['item_dict']['calc_accession_id']
    return




if __name__ == '__main__':
    """ __main__ used for development-only."""
    data = {
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
            'calc_accession_id': 'B 1980.1562',
            'credit_line': 'Gift of Saul P. Steinberg',
            'image_height': None,
            'image_width': None,
            'object_date': '1968',
            'object_depth': None,
            'object_height': None,
            'object_id': '182',
            # 'object_image_scan_filename': 'Frink B_1968.1562.tif',
            'object_image_scan_filename': 'Foglia PH_2008.1.jpg',
            'object_medium': 'Letterpress and lithography',
            'object_title': u"Aesop's Fables",
            'object_width': None,
            'object_year_end': '1968',
            'object_year_start': '1968',
            'series_id': None}
        }
    run__add_metadata_and_image( data )
