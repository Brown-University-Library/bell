# -*- coding: utf-8 -*-

import json, os, pprint, sys

extra_path = os.path.abspath( u'./' )  # adds bell_code to path
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
        # print u'starting try...'
        # try:

        #Store logger if exists
        if logger:
            self.logger = logger
        #Store accession number
        self.accession_number = item_data_dict[u'calc_accession_id']  # for logging
        # print u'- in fedora_metadata_only_builder.Task.create_fedora_metadata_object(); accession_number, %s' % self.accession_number
        #
        #Setup builders
        image_builder = ImageBuilder()
        ir_builder = IRBuilder()
        mods_builder = ModsBuilder()
        rights_builder = RightsBuilder()
        #
        #Instantiate repo connection
        #Note: doesn't actually try to connect until it has to
        repo = Repository( root=FEDORA_ADMIN_URL, username=FEDORA_ADMIN_USERNAME, password=FEDORA_ADMIN_PASSWORD )
        print u'- repo connection instantiated.'
        #
        #Instantiate new-obj
        #Purpose: this will be the object we'll build, then ingest
        new_obj = repo.get_object( type=JP2Image )  # this image model _will_ handle either tiff or jpg
        print u'- new_obj instantiated.'
        #
        #Instantiate collection object
        coll_obj = repo.get_object( pid=COLLECTION_PID )
        print u'- coll_obj instantiated.'
        #
        #Get/reserve a pid
        #Note: after obtaining pid, new_obj.pid is a value, not a reference
        new_pid = new_obj.pid()
        new_obj.pid = new_pid
        print u'- new_obj.pid obtained.'
        #
        #Assign collection object
        new_obj.owning_collection = coll_obj
        print u'- collection object assigned'
        #
        #Build rights object
        #Note: builds via bdrxml
        rights_object = rights_builder.build_rights_object()
        print u'- rights object built.'
        #
        #Assign rights object
        new_obj.rightsMD.content = rights_object
        print u'- rights object assigned.'
        #
        #Build ir object
        ir_object = ir_builder.build_ir_object()
        print u'- ir object built.'
        #
        #Assign ir object
        new_obj.irMD.content = ir_object
        print u'- ir object assigned.'
        #
        #Build mods object
        #Example returned data: { u'data: mods_object, u'accession_number': accession_number }
        mods_object_dict = mods_builder.build_mods_object( item_data_dict, mods_schema_path, u'return_object' )  # or u'return_string'
        mods_object = mods_object_dict[u'data']
        print u'- mods object built.'
        #
        #Assign mods object
        new_obj.mods.content = mods_object
        print u'- mods object assigned.'
        #
        #Update default admin fields
        new_obj.label = mods_object.title
        new_obj.owner = u'Bell Gallery'
        print u'- default admin fields updated.'
        #
        #
        #
        #Update master datastream and rels-int
        master_filename = data[u'item_dict'][u'object_image_scan_filename']
        ( file_url, dsID, mime_type ) = image_builder.build_master_datastream_vars(
            filename=master_filename, image_dir_url=MASTER_IMAGES_DIR_URL )
        new_obj = image_builder.update_object_datastream( new_obj, dsID, file_url, mime_type )
        new_obj = image_builder.update_newobj_relsint(
            new_obj=new_obj, filename=master_filename, dsID=dsID )
        #
        #
        #
        #Create jp2
        source_filepath = u'%s/%s' % ( MASTER_IMAGES_DIR_PATH, master_filename )
        temp_filename = master_filename.replace( u' ', u'_' )
        jp2_filename = temp_filename[0:-4] + u'.jp2'
        destination_filepath = u'%s/%s' % ( JP2_IMAGES_DIR_PATH, jp2_filename )
        image_builder.create_jp2( source_filepath, destination_filepath )
        #
        #
        #
        #Update jp2 datastream and rels-int
        ( file_url, dsID, mime_type ) = image_builder.build_jp2_datastream_vars(
            filename=jp2_filename, image_dir_url=JP2_IMAGES_DIR_URL )
        new_obj = image_builder.update_object_datastream( new_obj, dsID, file_url, mime_type )
        new_obj = image_builder.update_newobj_relsint(
            new_obj=new_obj, filename=jp2_filename, dsID=dsID )
        #
        #
        #
        #Save to fedora
        self._save_to_fedora( new_obj )
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

        # except Exception as e:
        #     error_message = u'in Task.create_fedora_metadata_object(); exception: %s' % unicode(repr(e))
        #     self.logger.info( error_message )
        #     raise Exception( error_message )

    def _save_to_fedora( self, new_obj ):
        """ Saves object to fedora. """
        try:
          new_obj.save()
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
            print u'- in fedora_metadata_only_builder.Task._update_task_tracker(); exception: %s' % unicode(repr(e))
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




def run__add_metadata_and_image( data ):
    """ Takes data-dict; example { item_dict: {title: the-title, acc-num: the-acc-num} }.
        Instantiates Task() instance & calls add_metadata_and_image().
        Called by task_manager.determine_next_task(). """
    logger = bell_logger.setup_logger()
    logger.info( u'in fedora_metadata_and_image_builder.run__add_metadata_and_image(); starting.' )
    print u'- in fedora_metadata_and_image_builder.run__add_metadata_and_image(); acc_num is: %s' % data[u'item_dict'][u'calc_accession_id']
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
        data[u'item_dict'], mods_schema_path, logger
        )
    print u'- in fedora_metadata_and_image_builder.run__add_metadata_and_image(); acc_num is: %s; item ingested' % data[u'item_dict'][u'calc_accession_id']
    return




if __name__ == u'__main__':
    """ __main__ used for development-only."""
    data = {
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
            u'object_image_scan_filename': u'Frink B_1968.1562.tif',
            u'object_medium': u'Letterpress and lithography',
            u'object_title': u"Aesop's Fables",
            u'object_width': None,
            u'object_year_end': u'1968',
            u'object_year_start': u'1968',
            u'series_id': None}
        }
    run__add_metadata_and_image( data )
