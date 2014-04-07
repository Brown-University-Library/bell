# -*- coding: utf-8 -*-

import json, os, pprint, sys

# extra_path = os.path.abspath( u'./' )  # adds bell_code to path
# sys.path.append( extra_path )
import bell_logger

from bdrcmodels.models import JP2Image
from eulfedora.server import Repository
from fedora_parts_builder import ImageBuilder, IRBuilder, ModsBuilder, RightsBuilder
from tasks import task_manager


class Task( object ):
    """ Updates fedora metadata and adds an image. """

    def __init__( self ):
        self.accession_number = None
        self.logger = None

    def update_existing_metadata_and_create_image( self,
        FEDORA_ADMIN_URL, FEDORA_ADMIN_USERNAME, FEDORA_ADMIN_PASSWORD,
        COLLECTION_PID, MASTER_IMAGES_DIR_PATH, MASTER_IMAGES_DIR_URL, JP2_IMAGES_DIR_PATH, JP2_IMAGES_DIR_URL,
        item_data_dict, mods_schema_path, logger=None
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
        ir_builder = IRBuilder()
        mods_builder = ModsBuilder()
        rights_builder = RightsBuilder()
        #
        #Instantiate repo connection
        #Note: doesn't actually try to connect until it has to
        repo = Repository( root=FEDORA_ADMIN_URL, username=FEDORA_ADMIN_USERNAME, password=FEDORA_ADMIN_PASSWORD )
        print u'- repo connection instantiated.'
        #
        #Get an instance of the object to update
        cur_obj = repo.get_object( pid=item_data_dict[u'pid'] )
        print u'- cur_obj instantiated.'
        #
        #Build rights object
        #Note: builds via bdrxml
        rights_object = rights_builder.build_rights_object()
        print u'- rights object built.'
        #
        #Assign rights object
        cur_obj.rightsMD.content = rights_object
        print u'- rights object assigned.'
        #
        #Build ir object
        ir_object = ir_builder.build_ir_object()
        print u'- ir object built.'
        #
        #Assign ir object
        cur_obj.irMD.content = ir_object
        print u'- ir object assigned.'
        #
        #Build mods object
        #Example returned data: { u'data: mods_object, u'accession_number': accession_number }
        mods_object_dict = mods_builder.build_mods_object( item_data_dict, mods_schema_path, u'return_object' )  # or u'return_string'
        mods_object = mods_object_dict[u'data']
        print u'- mods object built.'
        #
        #Assign mods object
        cur_obj.mods.content = mods_object
        print u'- mods object assigned.'
        #
        #Update default admin fields
        cur_obj.label = mods_object.title
        cur_obj.owner = u'Bell Gallery'
        print u'- default admin fields updated.'
        #
        #
        #
        #Update master datastream and rels-int
        master_filename = item_data_dict[u'object_image_scan_filename']
        ( file_url, dsID, mime_type ) = image_builder.build_master_datastream_vars(
            filename=master_filename, image_dir_url=MASTER_IMAGES_DIR_URL )
        cur_obj = image_builder.update_object_datastream( cur_obj, dsID, file_url, mime_type )
        cur_obj = image_builder.update_newobj_relsint(
            new_obj=cur_obj, filename=master_filename, dsID=dsID )
        print u'- master datastream and rels-int fields updated.'
        #
        #
        #
        #Create jp2
        source_filepath = u'%s/%s' % ( MASTER_IMAGES_DIR_PATH, master_filename )
        self.logger.info( u'in fedora_metadata_updater_and_image_builder.update_existing_metadata_and_create_image(); source_filepath, %s' % source_filepath )
        temp_filename = master_filename.replace( u' ', u'_' )
        jp2_filename = temp_filename[0:-4] + u'.jp2'
        destination_filepath = u'%s/%s' % ( JP2_IMAGES_DIR_PATH, jp2_filename )
        self.logger.info( u'in fedora_metadata_updater_and_image_builder.update_existing_metadata_and_create_image(); destination_filepath, %s' % destination_filepath )
        image_builder.create_jp2( source_filepath, destination_filepath )
        print u'- jp2 created.'
        #
        #
        #
        #Update jp2 datastream and rels-int
        ( file_url, dsID, mime_type ) = image_builder.build_jp2_datastream_vars(
            filename=jp2_filename, image_dir_url=JP2_IMAGES_DIR_URL )
        cur_obj = image_builder.update_object_datastream( cur_obj, dsID, file_url, mime_type )
        cur_obj = image_builder.update_newobj_relsint(
            new_obj=cur_obj, filename=jp2_filename, dsID=dsID )
        print u'- jp2 datastream and rels-int fields updated.'
        #
        #
        #
        #Save to fedora
        self._save_to_fedora( cur_obj )
        print u'- saved to fedora.'
        #
        #Update logging
        print u'- done.'
        self._update_task_tracker( message=u'new_pid:%s' % new_pid )
        #
        #Set next task
        task_manager.determine_next_task(
            unicode(sys._getframe().f_code.co_name),
            data={ u'item_data': item_data_dict, u'pid': new_pid },
            logger=logger
            )
        print u'- next task set.'
        return

    def _save_to_fedora( self, cur_obj ):
        """ Saves object to fedora. """
        try:
          cur_obj.save()
          self._update_task_tracker( message=u'save_successful' )
        except Exception as f:  # except DigitalObjectSaveFailure as f
          error_message = u'error on save: %s' % unicode(repr(f))
          print u'ERROR: %s' % error_message
          self._update_task_tracker( message=error_message )
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
        FEDORA_ADMIN_URL, FEDORA_ADMIN_USERNAME, FEDORA_ADMIN_PASSWORD,
        COLLECTION_PID, MASTER_IMAGES_DIR_PATH, MASTER_IMAGES_DIR_URL, JP2_IMAGES_DIR_PATH, JP2_IMAGES_DIR_URL
        ):
        """ Outputs settings derived from environmental variables for development. """
        print u'- settings...'
        print u'- FEDORA_ADMIN_URL: %s' % FEDORA_ADMIN_URL
        print u'- FEDORA_ADMIN_USERNAME: %s' % FEDORA_ADMIN_USERNAME
        print u'- FEDORA_ADMIN_PASSWORD: %s' % FEDORA_ADMIN_PASSWORD
        print u'- COLLECTION_PID: %s' % COLLECTION_PID
        print u'- MASTER_IMAGES_DIR_PATH: %s' % MASTER_IMAGES_DIR_PATH
        print u'- MASTER_IMAGES_DIR_URL: %s' % MASTER_IMAGES_DIR_URL
        print u'- JP2_IMAGES_DIR_PATH: %s' % JP2_IMAGES_DIR_PATH
        print u'- JP2_IMAGES_DIR_URL: %s' % JP2_IMAGES_DIR_URL
        print u'---'
        return

    # end class Task()




def run__update_existing_metadata_and_create_image( data ):
    """ Takes data-dict; example { item_dict: {title: the-title, acc-num: the-acc-num} }.
        Instantiates Task() instance & calls add_metadata_and_image().
        Called by task_manager.determine_next_task(). """
    logger = bell_logger.setup_logger()
    logger.info( u'in fedora_metadata_and_image_builder.run__update_existing_metadata_and_create_image(); starting.' )
    print u'in fedora_metadata_and_image_builder.run__update_existing_metadata_and_create_image(); acc_num is: %s' % data[u'item_dict'][u'calc_accession_id']
    FEDORA_ADMIN_URL = unicode( os.environ.get(u'BELL_FMAIB__FEDORA_ADMIN_URL') )
    FEDORA_ADMIN_USERNAME = unicode( os.environ.get(u'BELL_FMAIB__FEDORA_ADMIN_USERNAME') )
    FEDORA_ADMIN_PASSWORD = unicode( os.environ.get(u'BELL_FMAIB__FEDORA_ADMIN_PASSWORD') )
    COLLECTION_PID = unicode( os.environ.get(u'BELL_FMAIB__COLLECTION_PID') )
    MASTER_IMAGES_DIR_PATH = unicode( os.environ.get(u'BELL_FMAIB__MASTER_IMAGES_DIR_PATH') )
    MASTER_IMAGES_DIR_URL = unicode( os.environ.get(u'BELL_FMAIB__MASTER_IMAGES_DIR_URL'))
    JP2_IMAGES_DIR_PATH = unicode( os.environ.get(u'BELL_FMAIB__JP2_IMAGES_DIR_PATH') )
    JP2_IMAGES_DIR_URL = unicode( os.environ.get(u'BELL_FMAIB__JP2_IMAGES_DIR_URL') )
    mods_schema_path = os.path.abspath( u'./lib/mods-3-4.xsd' )
    task = Task()
    task._print_settings(
        FEDORA_ADMIN_URL, FEDORA_ADMIN_USERNAME, FEDORA_ADMIN_PASSWORD,
        COLLECTION_PID, MASTER_IMAGES_DIR_PATH, MASTER_IMAGES_DIR_URL, JP2_IMAGES_DIR_PATH, JP2_IMAGES_DIR_URL
        )
    task.add_metadata_and_image(
        FEDORA_ADMIN_URL, FEDORA_ADMIN_USERNAME, FEDORA_ADMIN_PASSWORD,
        COLLECTION_PID, MASTER_IMAGES_DIR_PATH, MASTER_IMAGES_DIR_URL, JP2_IMAGES_DIR_PATH, JP2_IMAGES_DIR_URL,
        data[u'pid'], data[u'item_dict'], mods_schema_path, logger
        )
    print u'fedora_metadata_and_image_builder.run__update_existing_metadata_and_create_image(); acc_num is: %s; item ingested' % data[u'item_dict'][u'calc_accession_id']
    return




if __name__ == u'__main__':
    """ __main__ used for development-only."""
    data = {
        u'pid': u'TBD',
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
            u'calc_accession_id': u'B 1980.1562',
            u'credit_line': u'Gift of Saul P. Steinberg',
            u'image_height': None,
            u'image_width': None,
            u'object_date': u'1968',
            u'object_depth': None,
            u'object_height': None,
            u'object_id': u'182',
            # u'object_image_scan_filename': u'Frink B_1968.1562.tif',
            u'object_image_scan_filename': u'Foglia PH_2008.1.jpg',
            u'object_medium': u'Letterpress and lithography',
            u'object_title': u"Aesop's Fables",
            u'object_width': None,
            u'object_year_end': u'1968',
            u'object_year_start': u'1968',
            u'series_id': None}
        }
    run__update_existing_metadata_and_create_image( data )
