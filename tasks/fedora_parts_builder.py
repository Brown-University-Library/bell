# -*- coding: utf-8 -*-

import datetime, os, pprint

from bdrxml import irMetadata, rights


class IRBuilder( object ):
    """ Handles ir creation via bdrxml. """

    def build_ir_object( self ):
        ''' Creates basic bell ir object.
            Called by bell_2013_05.ingest_new_metadata_objects()
            'ir_obj.date' and if necessary, 'ir_obj.filename', will be set dynamically '''
        obj = irMetadata.make_ir()
        obj.depositor_name = u'Bell Gallery'
        assert unicode(repr(type(obj))) == u"<class 'bdrxml.irMetadata.IR'>", unicode(repr(type(obj)))
        return obj

    # end class IRBuilder()


class RightsBuilder( object ):
    """ Handles rights creation via bdrxml. """

    def __init__( self ):
        self.context_dict = {
            u'repo_manager': {
                u'id': u'rts001', u'username': u'BROWN:DEPARTMENT:LIBRARY:REPOSITORY', u'cclass': u'REPOSITORY MGR',
                u'discover': True, u'display': True, u'modify': True, u'delete': True },
            u'bell_gallery': {
                u'id': u'rts002', u'username': u'Bell Gallery', u'cclass': u'GENERAL PUBLIC',
                u'discover': True, u'display': True, u'modify': True, u'delete': True },
            u'general_public': {
                u'id': u'rts003', u'username': u'BDR_PUBLIC', u'cclass': u'GENERAL PUBLIC',
                u'discover': True, u'display': True, u'modify': False, u'delete': False },
            }

    def build_rights_object( self ):
        """ CONTROLLER.
            Returns basic bell rights object created via bdrxml. """
        obj = rights.make_rights()
        obj.category = u'COPYRIGHTED'
        obj = self._add_holder_data( obj )
        obj = self._add_rights_context( obj, u'repo_manager' )
        obj = self._add_rights_context( obj, u'bell_gallery' )
        obj = self._add_rights_context( obj, u'general_public' )
        obj.date = datetime.date.today()
        assert unicode(repr(type(obj))) == "<class 'bdrxml.rights.Rights'>", unicode(repr(type(obj)))
        return obj

    def _add_holder_data( self, obj ):
        """ Returns object with holder.name and holder.context_ids """
        obj.create_holder()
        obj.holder.name = u'Contact Bell Gallery for details: <http://www.brown.edu/campus-life/arts/bell-gallery/about/contact-information>'
        obj.holder.context_ids = u'rts001 rts002 rts003'
        return obj

    def _add_rights_context( self, obj, rights_holder ):
        """ Returns object with a rights-context """
        rc = rights.Context()
        rc.id = self.context_dict[rights_holder][u'id']
        rc.usertype = u'GROUP'
        rc.username = self.context_dict[rights_holder][u'username']
        rc.cclass = self.context_dict[rights_holder][u'cclass']
        rc.discover = self.context_dict[rights_holder][u'discover']
        rc.display = self.context_dict[rights_holder][u'display']
        rc.modify = self.context_dict[rights_holder][u'modify']
        rc.delete = self.context_dict[rights_holder][u'delete']
        obj.ctext.append( rc )
        return obj

    # end class RightsBuilder()

