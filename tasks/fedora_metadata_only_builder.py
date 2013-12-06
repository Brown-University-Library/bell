# -*- coding: utf-8 -*-

import os, pprint

from bdrcmodels.models import CommonMetadataDO
from eulfedora.server import Repository
from fedora_parts_builder import IRBuilder, RightsBuilder


class Task( object ):
    """ Handles creation of a fedora metadata-only object. """

    def __init__( self ):
        pass

    def create_fedora_metadata_object( self,
        FEDORA_ADMIN_URL, FEDORA_ADMIN_USERNAME, FEDORA_ADMIN_PASSWORD,
        COLLECTION_PID,
        ):
        """ CONTROLLER """
        print u'starting try...'
        try:
            #Setup builders
            right_builder = RightsBuilder()
            ir_builder = IRBuilder()
            #
            #Instantiate repo connection
            #Note: doesn't actually try to connect until it has to
            # repo = Repository( root=FEDORA_ADMIN_URL, username=FEDORA_ADMIN_USERNAME, password=FEDORA_ADMIN_PASSWORD )
            print u'- repo connection instantiated. (TURNED_OFF)'
            #
            #Instantiate new-obj
            #Purpose: this will be the object we'll build, then ingest
            # new_obj = repo.get_object( type=CommonMetadataDO )
            print u'- new_obj instantiated. (TURNED_OFF)'
            #
            #Instantiate collection object
            # coll_obj = repo.get_object( pid=self.COLLECTION_PID )
            print u'- coll_obj instantiated. (TURNED_OFF)'
            #Get/reserve a pid
            #Note: after obtaining pid, new_obj.pid is a value, not a reference
            # new_pid = new_obj.pid()
            print u'- new_obj.pid obtained. (TURNED_OFF)'
            #
            #Assign collection object
            # new_obj.owning_collection = coll_obj
            print u'- collection object assigned (TURNED_OFF)'
            #
            #Build rights object
            #Note: builds via bdrxml
            rights_object = right_builder.build_rights_object()
            print u'- rights object built.'
            #
            #Assign rights object
            # new_obj.rightsMD.content = rights_obj
            print u'- rights object assigned. (TURNED_OFF)'
            #
            #Build ir object
            #Note: builds via ???
            ir_object = ir_builder.build_ir_object()
            print u'- ir object built.'
            #
            #Assign ir object
            # new_obj.irMD.content = ir_object
            print u'- ir object assigned. (TURNED_OFF)'
            #
            1/0

            ## create mods
            new_mods_obj = eulxml.xmlmap.load_xmlobject_from_string( item_dict[u'XBDR_mods_xml'], Mods )
            new_obj.mods.content = new_mods_obj
            assert new_mods_obj.title == new_obj.mods.content.title  # just documentation; cool, eh?
            ## update default admin fields
            new_obj.label = new_mods_obj.title
            new_obj.owner = u'Bell Gallery'
            ## save/ingest to fedora
            print u'about to save to fedora...'
            _save_to_fedora( new_obj, task_log ); print u'save to fedora complete...'

        except Exception as e:
            error_message = u'- in Task.create_fedora_metadata_object(); error on ingest: %s' % repr(e).decode(u'utf-8', u'replace')
            print u'ERROR: %s' % error_message

        pass

    # def _instantiate_repo_connection( self, FEDORA_ADMIN_URL, FEDORA_ADMIN_USERNAME, FEDORA_ADMIN_PASSWORD ):
    #     """ Sets up connection to repository. """
    #     repo = Repository( FEDORA_ADMIN_URL, FEDORA_ADMIN_USERNAME, FEDORA_ADMIN_PASSWORD )
    #     # assert u'dev' in repo.fedora_root  # TODO: REMOVE when ready for production
    #     return repo

    def _print_settings( self,
        BELL_FMOB__FEDORA_ADMIN_URL,
        BELL_FMOB__FEDORA_ADMIN_USERNAME,
        BELL_FMOB__FEDORA_ADMIN_PASSWORD,
        BELL_FMOB__COLLECTION_PID,
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




#######################

    def OLD_ingest_new_metadata_objects(self):

        """ Iterates through new metadata, creates brand-new fedora objects with mods, rights, & ir data, and saves to fedora.
            """

        ## setup

        bh = BellHelper()

        def _load_function_data():
          """ Loads source-data from previous Task.
              Loads log-data from current Task.
              Returns current_task_obj for subsequent log-saves. """
          ## get most recent metadata json
          t = Task.objects.get( name=u'add_mods_to_metadata', project=u'BELL201305' )
          data_list = json.loads( t.output )
          assert sorted(data_list[0].keys())[0:2] == [u'ARTISTS::artist_alias', u'ARTISTS::artist_birth_country_id']
          ## get log
          current_task_obj = Task.objects.get( name=u'ingest_new_metadata_objects', project=u'BELL201305' )
          task_log = {} if (len(current_task_obj.output) == 0) else json.loads( current_task_obj.output )
          return ( data_list, task_log, current_task_obj )

        def _check_previous_ingestion( item_dict, task_log, current_task_obj ):
          """ Checks source-data and task-log-data for previous ingestions. """
          accession_number = item_dict[u'calc_accession_id']
          ## check source data
          if item_dict[u'XBDR_previously_ingested']:
            skip_message = u'skipping; was in previous task metadata with pid %s' % item_dict[u'XBDR_previously_ingested']
            if u'accession_number' in task_log.keys():
              task_log[u'accession_number'][now] = skip_message
            else:
              task_log[accession_number] = { now: skip_message }
            current_task_obj.output = json.dumps( task_log, indent=2, sort_keys=True ); current_task_obj.save(); print u'save-in-source-check'
            return True
          ## check current-task log
          if accession_number in task_log.keys():
            if not u'previously_ingested' in task_log[accession_number].keys():
              task_log[accession_number][u'previously_ingested'] = None
            if task_log[accession_number][u'previously_ingested']:
              task_log[accession_number][now] = u'skipping; was in task_log with pid %s' % task_log[accession_number][u'previously_ingested']
              current_task_obj.output = json.dumps( task_log, indent=2, sort_keys=True ); current_task_obj.save(); print u'save-in-log-check'
              return True
          return False

        def _save_to_fedora( new_obj, task_log ):
          """ Saves to fedora & updates task_log. """
          try:
            new_obj.save()
            # pass
            ## update task-log
            task_log[accession_number] = { now: u'ingestion successful' }
            task_log[accession_number][u'previously_ingested'] = new_obj.pid
          # except DigitalObjectSaveFailure as f:
          except Exception as f:
            error_message = u'error on ingestion: %s' % repr(f).decode(u'utf-8', u'replace')
            print u'ERROR: %s' % error_message
            task_log[u'error_%s' % unicode(now)] = error_message
          return

        ## end of setup -- now work

        ## get data
        data_list, task_log, current_task_obj = _load_function_data()

        ## instantiate repo connection
        repo = Repository(
          root=self.FEDORA_ADMIN_URL,
          username=self.FEDORA_ADMIN_USERNAME,
          password=self.FEDORA_ADMIN_PASSWORD )
        # assert u'dev' in repo.fedora_root  # TODO: REMOVE when ready for production

        ## prep the collection object
        coll_obj = repo.get_object( pid=self.COLLECTION_PID )
        # assert u'test' in coll_obj.pid  # TODO: REMOVE when ready for production

        ## make rights object
        rights_obj = bh.build_rights_object()
        assert unicode(type(rights_obj)) == u"<class 'bdrxml.rights.Rights'>"
        assert u'Bell Gallery' in rights_obj.holder.name  # name includes an email address

        ## make ir object
        ir_obj = bh.build_ir_object()
        assert unicode(type(ir_obj)) == u"<class 'bdrxml.irMetadata.IR'>"
        assert u'Bell Gallery' == ir_obj.depositor_name

        ## iterate through, skipping previously ingested 3
        for i,item_dict in enumerate( data_list ):

          print u'i is: %s' % i
          ## break for testing
          if i > 2:
            break

          time.sleep( .2 )
          now = unicode( datetime.datetime.now() )
          accession_number = item_dict[u'calc_accession_id']
          print u'accession_number is: %s' % accession_number

          if _check_previous_ingestion( item_dict, task_log, current_task_obj ):
            print u'- whoa, skipping this one'
            continue

          try:
            ## instantiate new-obj
            print u'starting try...'
            new_obj = repo.get_object( type=CommonMetadataDO ); print u'new_obj instantiated...'
            new_obj.pid = new_obj.pid()  # function gets a new one, then new_obj.pid is a value not a reference
            print u'new_obj.pid obtained...'
            ## assign collection object
            new_obj.owning_collection = coll_obj
            ## assign rights object
            new_obj.rightsMD.content = rights_obj
            ## update and assign ir object
            ir_obj.date = datetime.date.today()
            new_obj.irMD.content = ir_obj
            ## create mods
            new_mods_obj = eulxml.xmlmap.load_xmlobject_from_string( item_dict[u'XBDR_mods_xml'], Mods )
            new_obj.mods.content = new_mods_obj
            assert new_mods_obj.title == new_obj.mods.content.title  # just documentation; cool, eh?
            ## update default admin fields
            new_obj.label = new_mods_obj.title
            new_obj.owner = u'Bell Gallery'
            ## save/ingest to fedora
            print u'about to save to fedora...'
            _save_to_fedora( new_obj, task_log ); print u'save to fedora complete...'

          except Exception as e:
            error_message = u'error on ingestion-loop: %s' % repr(e).decode(u'utf-8', u'replace')
            print u'ERROR: %s' % error_message
            task_log[u'error_%s' % unicode(now)] = error_message

          finally:
            ## save logging - yes, each time for crash-robustness
            current_task_obj.output = json.dumps( task_log, indent=2, sort_keys=True ); current_task_obj.save(); print u'save-in-finally'

        print current_task_obj.output
        return

        # end def ingest_new_metadata_objects()




if __name__ == u'__main__':
    """ Assumes env is activated.
        ( 'FMOB' used as a namespace prefix for this 'fedora_metadata_only_builder.py' file. ) """
    # pprint.pprint( os.environ.__dict__ )
    FEDORA_ADMIN_URL=unicode( os.environ.get(u'BELL_FMOB__FEDORA_ADMIN_URL') )
    FEDORA_ADMIN_USERNAME=unicode( os.environ.get(u'BELL_FMOB__FEDORA_ADMIN_USERNAME') )
    FEDORA_ADMIN_PASSWORD=unicode( os.environ.get(u'BELL_FMOB__FEDORA_ADMIN_PASSWORD') )
    COLLECTION_PID=unicode( os.environ.get(u'BELL_FMOB__COLLECTION_PID') )
    task = Task()
    task._print_settings(
        FEDORA_ADMIN_URL, FEDORA_ADMIN_USERNAME, FEDORA_ADMIN_PASSWORD,
        COLLECTION_PID
        )
    task.create_fedora_metadata_object(
        FEDORA_ADMIN_URL, FEDORA_ADMIN_USERNAME, FEDORA_ADMIN_PASSWORD,
        COLLECTION_PID
        )
