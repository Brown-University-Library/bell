# -*- coding: utf-8 -*-

import json, os, pprint

from bdrcmodels.models import CommonMetadataDO
from eulfedora.server import Repository
from fedora_parts_builder import IRBuilder, ModsBuilder, RightsBuilder


class Task( object ):
    """ Handles creation of a fedora metadata-only object. """

    def __init__( self ):
        self.accession_number = None  # updated after mods is built; used for logging.

    def create_fedora_metadata_object( self,
        FEDORA_ADMIN_URL, FEDORA_ADMIN_USERNAME, FEDORA_ADMIN_PASSWORD,
        COLLECTION_PID,
        item_data_dict, mods_schema_path
        ):
        """ CONTROLLER
            Note: item_data_dict is a json entry from foundation/acc_num_to_data.py json """
        print u'starting try...'
        try:
            #Store accession number
            self.accession_number = item_data_dict[u'calc_accession_id']  # for logging
            #
            #Setup builders
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
            new_obj = repo.get_object( type=CommonMetadataDO )
            print u'- new_obj instantiated.'
            #
            #Instantiate collection object
            coll_obj = repo.get_object( pid=COLLECTION_PID )
            print u'- coll_obj instantiated.'
            #
            #Get/reserve a pid
            #Note: after obtaining pid, new_obj.pid is a value, not a reference
            new_pid = new_obj.pid()
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
            #Note: builds via ???
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
            #Save to fedora
            self._save_to_fedora( new_obj )
            print u'- saved to fedora.'
            return
            #
            #Update logging
            print u'- done.'
            self._update_task_tracker( message=u'ingestion_successful' )
        except Exception as e:
            error_message = u'- in Task.create_fedora_metadata_object(); exception: %s' % unicode(repr(e))
            # self._update_task_tracker( message=error_message )
            raise Exception( error_message )

    def _save_to_fedora( self, new_obj ):
        """ Saves object to fedora. """
        try:
          new_obj.save()
          # pass
          ## update task-log
          self._update_task_tracker( message=u'save_successful' )
        # except DigitalObjectSaveFailure as f:
        except Exception as f:
          error_message = u'error on save: %s' % unicode(repr(f))
          print u'ERROR: %s' % error_message
          self._update_task_tracker( message=error_message )
        return

    def _update_task_tracker( self, message ):
        """ Will update redis 'bell:tracker' entry. """
        try:
            from tasks import task_manager
            task_manager.update_tracker( key=self.accession_number, message=message )
        except Exception as e:
            print u'- in fedora_metadata_only_builder.Task._update_task_tracker(); exception: %s' % unicode(repr(e))
            pass

    def _print_settings( self,
        BELL_FMOB__FEDORA_ADMIN_URL,
        BELL_FMOB__FEDORA_ADMIN_USERNAME,
        BELL_FMOB__FEDORA_ADMIN_PASSWORD,
        BELL_FMOB__COLLECTION_PID
        ):
        """ Outputs settings derived from environmental variables for development. """
        print u'- settings...'
        print u'- BELL_FMOB__FEDORA_ADMIN_URL: %s' % BELL_FMOB__FEDORA_ADMIN_URL
        print u'- BELL_FMOB__FEDORA_ADMIN_USERNAME: %s' % BELL_FMOB__FEDORA_ADMIN_USERNAME
        print u'- BELL_FMOB__FEDORA_ADMIN_PASSWORD: %s' % BELL_FMOB__FEDORA_ADMIN_PASSWORD
        print u'- BELL_FMOB__COLLECTION_PID: %s' % BELL_FMOB__COLLECTION_PID
        print u'---'
        return

    # end class Task()


def run__create_fedora_metadata_object( item_dict ):
    """ Instantiates Task() instance & calls create_fedora_metadata_object(). """
    print u'- in fedora_metadata_only_builder.run__create_fedora_metadata_object(); acc_num is: %s' % item_dict[u'calc_accession_id']
    FEDORA_ADMIN_URL=unicode( os.environ.get(u'BELL_FMOB__FEDORA_ADMIN_URL') )
    FEDORA_ADMIN_USERNAME=unicode( os.environ.get(u'BELL_FMOB__FEDORA_ADMIN_USERNAME') )
    FEDORA_ADMIN_PASSWORD=unicode( os.environ.get(u'BELL_FMOB__FEDORA_ADMIN_PASSWORD') )
    COLLECTION_PID=unicode( os.environ.get(u'BELL_FMOB__COLLECTION_PID') )
    mods_schema_path = os.path.abspath( u'./lib/mods-3-4.xsd' )
    task = Task()
    task.create_fedora_metadata_object(
        FEDORA_ADMIN_URL, FEDORA_ADMIN_USERNAME, FEDORA_ADMIN_PASSWORD,
        COLLECTION_PID,
        item_dict, mods_schema_path
        )
    print u'- in fedora_metadata_only_builder.run__create_fedora_metadata_object(); acc_num is: %s; item ingested' % item_dict[u'calc_accession_id']
    return




if __name__ == u'__main__':
    """ __main__ used for development, and as caller documentation.
        Assumes env is activated & cwd is './bell_code'.
        ( 'FMOB' used as a namespace prefix for this 'fedora_metadata_only_builder.py' file. ) """
    #prep...
    FEDORA_ADMIN_URL=unicode( os.environ.get(u'BELL_FMOB__FEDORA_ADMIN_URL') )
    FEDORA_ADMIN_USERNAME=unicode( os.environ.get(u'BELL_FMOB__FEDORA_ADMIN_USERNAME') )
    FEDORA_ADMIN_PASSWORD=unicode( os.environ.get(u'BELL_FMOB__FEDORA_ADMIN_PASSWORD') )
    COLLECTION_PID=unicode( os.environ.get(u'BELL_FMOB__COLLECTION_PID') )
    with open( os.path.abspath(u'./tasks/test_data/raw_source_single_artist.json') ) as f:
        item_data_dict = json.loads( f.read() )
    mods_schema_path = os.path.abspath( u'./lib/mods-3-4.xsd' )
    #work...
    task = Task()
    task._print_settings(
        FEDORA_ADMIN_URL, FEDORA_ADMIN_USERNAME, FEDORA_ADMIN_PASSWORD,
        COLLECTION_PID
        )
    task.create_fedora_metadata_object(
        FEDORA_ADMIN_URL, FEDORA_ADMIN_USERNAME, FEDORA_ADMIN_PASSWORD,
        COLLECTION_PID,
        item_data_dict, mods_schema_path
        )
