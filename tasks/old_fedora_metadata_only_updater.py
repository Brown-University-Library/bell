# -*- coding: utf-8 -*-

from __future__ import unicode_literals

# JUST COPIED; UPDATE!

# import json, os, pprint, sys
# import bell_logger
# from bdrcmodels.models import CommonMetadataDO
# from eulfedora.server import Repository
# from fedora_parts_builder import IRBuilder, ModsBuilder, RightsBuilder
# from tasks import task_manager


# class Task( object ):
#     """ Updates fedora metadata-only object. """

#     def __init__( self ):
#         self.accession_number = None
#         self.logger = None

#     def update_existing_metadata_object( self,
#         FEDORA_ADMIN_URL, FEDORA_ADMIN_USERNAME, FEDORA_ADMIN_PASSWORD,
#         COLLECTION_PID,
#         item_data_dict, mods_schema_path, logger=None
#         ):
#         """ CONTROLLER
#             Note: item_data_dict is a json entry from foundation/acc_num_to_data.py json """
#         print 'starting try...'
#         try:
#             #Store logger if exists
#             if logger:
#                 self.logger = logger
#             #Store accession number
#             self.accession_number = item_data_dict['calc_accession_id']  # for logging
#             # print '- in fedora_metadata_only_builder.Task.create_fedora_metadata_object(); accession_number, %s' % self.accession_number
#             #
#             #Setup builders
#             ir_builder = IRBuilder()
#             mods_builder = ModsBuilder()
#             rights_builder = RightsBuilder()
#             #
#             #Instantiate repo connection
#             #Note: doesn't actually try to connect until it has to
#             repo = Repository( root=FEDORA_ADMIN_URL, username=FEDORA_ADMIN_USERNAME, password=FEDORA_ADMIN_PASSWORD )
#             print '- repo connection instantiated.'
#             #
#             #Get current object
#             #Purpose: this will be the object we'll update, then save
#             HEREzz
#             new_obj = repo.get_object( type=CommonMetadataDO )
#             print '- new_obj instantiated.'
#             #
#             #Instantiate collection object
#             coll_obj = repo.get_object( pid=COLLECTION_PID )
#             print '- coll_obj instantiated.'
#             #
#             #Get/reserve a pid
#             #Note: after obtaining pid, new_obj.pid is a value, not a reference
#             new_pid = new_obj.pid()
#             new_obj.pid = new_pid
#             print '- new_obj.pid obtained.'
#             #
#             #Assign collection object
#             new_obj.owning_collection = coll_obj
#             print '- collection object assigned'
#             #
#             #Build rights object
#             #Note: builds via bdrxml
#             rights_object = rights_builder.build_rights_object()
#             print '- rights object built.'
#             #
#             #Assign rights object
#             new_obj.rightsMD.content = rights_object
#             print '- rights object assigned.'
#             #
#             #Build ir object
#             ir_object = ir_builder.build_ir_object()
#             print '- ir object built.'
#             #
#             #Assign ir object
#             new_obj.irMD.content = ir_object
#             print '- ir object assigned.'
#             #
#             #Build mods object
#             #Example returned data: { 'data: mods_object, 'accession_number': accession_number }
#             mods_object_dict = mods_builder.build_mods_object( item_data_dict, mods_schema_path, 'return_object' )  # or 'return_string'
#             mods_object = mods_object_dict['data']
#             print '- mods object built.'
#             #
#             #Assign mods object
#             new_obj.mods.content = mods_object
#             print '- mods object assigned.'
#             #
#             #Update default admin fields
#             new_obj.label = mods_object.title
#             new_obj.owner = 'Bell Gallery'
#             print '- default admin fields updated.'
#             #
#             #Save to fedora
#             self._save_to_fedora( new_obj )
#             print '- saved to fedora.'
#             #
#             #Update logging
#             print '- done.'
#             self._update_task_tracker( message='new_pid:%s' % new_pid )
#             #
#             #Set next task
#             task_manager.determine_next_task(
#                 unicode(sys._getframe().f_code.co_name),
#                 data={ 'item_data': item_data_dict, 'pid': new_pid },
#                 logger=logger
#                 )
#             print '- next task set.'
#         except Exception as e:
#             error_message = 'in Task.create_fedora_metadata_object(); exception: %s' % unicode(repr(e))
#             self.logger.info( error_message )
#             raise Exception( error_message )

#     def _save_to_fedora( self, new_obj ):
#         """ Saves object to fedora. """
#         try:
#           new_obj.save()
#           self._update_task_tracker( message='save_successful' )
#         except Exception as f:  # except DigitalObjectSaveFailure as f
#           error_message = 'error on save: %s' % unicode(repr(f))
#           print 'ERROR: %s' % error_message
#           self._update_task_tracker( message=error_message )
#         return

#     def _update_task_tracker( self, message ):
#         """ Updates redis 'bell:tracker' accession number entry. """
#         try:
#             from tasks import task_manager
#             task_manager.update_tracker( key=self.accession_number, message=message )
#         except Exception as e:
#             print '- in fedora_metadata_only_builder.Task._update_task_tracker(); exception: %s' % unicode(repr(e))
#             pass
#         return

#     def _print_settings( self,
#         BELL_FMOB__FEDORA_ADMIN_URL,
#         BELL_FMOB__FEDORA_ADMIN_USERNAME,
#         BELL_FMOB__FEDORA_ADMIN_PASSWORD,
#         BELL_FMOB__COLLECTION_PID
#         ):
#         """ Outputs settings derived from environmental variables for development. """
#         print '- settings...'
#         print '- BELL_FMOB__FEDORA_ADMIN_URL: %s' % BELL_FMOB__FEDORA_ADMIN_URL
#         print '- BELL_FMOB__FEDORA_ADMIN_USERNAME: %s' % BELL_FMOB__FEDORA_ADMIN_USERNAME
#         print '- BELL_FMOB__FEDORA_ADMIN_PASSWORD: %s' % BELL_FMOB__FEDORA_ADMIN_PASSWORD
#         print '- BELL_FMOB__COLLECTION_PID: %s' % BELL_FMOB__COLLECTION_PID
#         print '---'
#         return

#     # end class Task()


# def run__update_existing_metadata_object( data ):
#     """ Instantiates Task() instance & calls create_fedora_metadata_object().
#         Called by task_manager.determine_next_task(). """
#     1/0
#     logger = bell_logger.setup_logger()
#     logger.info( 'in fedora_metadata_only_builder.run__create_fedora_metadata_object(); starting.' )
#     print '- in fedora_metadata_only_builder.run__create_fedora_metadata_object(); acc_num is: %s' % data['item_dict']['calc_accession_id']
#     FEDORA_ADMIN_URL=unicode( os.environ.get('BELL_FMOB__FEDORA_ADMIN_URL') )
#     FEDORA_ADMIN_USERNAME=unicode( os.environ.get('BELL_FMOB__FEDORA_ADMIN_USERNAME') )
#     FEDORA_ADMIN_PASSWORD=unicode( os.environ.get('BELL_FMOB__FEDORA_ADMIN_PASSWORD') )
#     COLLECTION_PID=unicode( os.environ.get('BELL_FMOB__COLLECTION_PID') )
#     mods_schema_path = os.path.abspath( './lib/mods-3-4.xsd' )
#     task = Task()
#     task.update_existing_metadata_object(
#         FEDORA_ADMIN_URL, FEDORA_ADMIN_USERNAME, FEDORA_ADMIN_PASSWORD,
#         COLLECTION_PID,
#         data['item_dict'], mods_schema_path, logger
#         )
#     print '- in fedora_metadata_only_builder.run__create_fedora_metadata_object(); acc_num is: %s; item ingested' % data['item_dict']['calc_accession_id']
#     return




# if __name__ == '__main__':
#     """ __main__ used for development, and as caller documentation.
#         Assumes env is activated & cwd is './bell_code'.
#         ( 'FMOB' used as a namespace prefix for this 'fedora_metadata_only_builder.py' file. ) """
#     #prep...
#     FEDORA_ADMIN_URL=unicode( os.environ.get('BELL_FMOB__FEDORA_ADMIN_URL') )
#     FEDORA_ADMIN_USERNAME=unicode( os.environ.get('BELL_FMOB__FEDORA_ADMIN_USERNAME') )
#     FEDORA_ADMIN_PASSWORD=unicode( os.environ.get('BELL_FMOB__FEDORA_ADMIN_PASSWORD') )
#     COLLECTION_PID=unicode( os.environ.get('BELL_FMOB__COLLECTION_PID') )
#     with open( os.path.abspath('./tasks/test_data/raw_source_single_artist.json') ) as f:
#         item_data_dict = json.loads( f.read() )
#     mods_schema_path = os.path.abspath( './lib/mods-3-4.xsd' )
#     #work...
#     task = Task()
#     task._print_settings(
#         FEDORA_ADMIN_URL, FEDORA_ADMIN_USERNAME, FEDORA_ADMIN_PASSWORD,
#         COLLECTION_PID
#         )
#     task.update_existing_metadata_object(
#         FEDORA_ADMIN_URL, FEDORA_ADMIN_USERNAME, FEDORA_ADMIN_PASSWORD,
#         COLLECTION_PID,
#         item_data_dict, mods_schema_path
#         )
