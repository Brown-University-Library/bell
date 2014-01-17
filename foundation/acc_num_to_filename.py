# -*- coding: utf-8 -*-

import json, os, pprint
import gspread  # <https://github.com/burnash/gspread>


class FilenameLinker( object ):
    """ Handles creation of an accession_number-to-filename dict, saved as a json file.
        Purpose: This is one of three essential files that should exist before doing almost any bell processing.
                 It converts google-doc filename spreadsheet data into json data for easy processing and viewing.
        if __name__... at bottom indicates how to run this script. """

    def __init__( self ):
        self.logger = None

    def make_linked_json( self, G_EMAIL, G_PASSWORD, JSON_OUTPUT_PATH ):
        wk_sheet = self._get_worksheet( G_EMAIL, G_PASSWORD )
        ( col_1, col_2 ) = self._get_columns( wk_sheet )
        dic = {}
        for i in range( len(col_1) ):
            ( key, value ) = self._get_key_value( col_1[i], col_2[i] )
            dic[ key ] = value
        jstring = json.dumps( dic, sort_keys=True, indent=2 )
        with open( JSON_OUTPUT_PATH, u'w' ) as f:
            f.write( jstring )
        return

    def _get_worksheet( self, G_EMAIL, G_PASSWORD ):
        """ Returns worksheet. """
        gc = gspread.login( G_EMAIL, G_PASSWORD )  # gc == 'gspread_client'
        sp_sheet = gc.open( u'bell_tracker' )
        wk_sheet = sp_sheet.worksheet( u'overview' )
        return wk_sheet

    def _get_columns( self, wk_sheet ):
        """ Returns columns without header row. """
        tmp_col_1 = wk_sheet.col_values( 1 )  # not zero-indexed -- this is column A
        tmp_col_2 = wk_sheet.col_values( 2 )
        assert len(tmp_col_1) == len(tmp_col_2), Exception( u'column-length mismatch' )
        col_1 = tmp_col_1[ 1: ]  # ignore header row
        col_2 = tmp_col_2[ 1: ]
        return ( col_1, col_2 )

    def _get_key_value( self, col_1_data, col_2_data ):
        """ Returns cleaned key/value. """
        key = col_1_data.strip().decode( u'utf-8', u'replace' )
        value = col_2_data
        if type(value) == str:
            value = value.strip().decode( u'utf-8', u'replace' )
        return ( key, value )

    def _print_settings( self, G_EMAIL, B, JSON_OUTPUT_PATH ):
        """ Outputs settings derived from environmental variables for development. """
        print u'- settings...'
        print u'- G_EMAIL: %s' % G_EMAIL
        print u'- G_PASSWORD: %s' % G_PASSWORD
        print u'- JSON_OUTPUT_PATH: %s' % JSON_OUTPUT_PATH
        print u'---'
        return

    # end class FilenameLinker()




if __name__ == u'__main__':
    """ Assumes env is activated.
        ( 'ANTF' used as a namespace prefix for this 'acc_num_to_filename.py' file. ) """
    G_EMAIL=os.environ.get( u'BELL_ANTF__G_MAIL' )
    G_PASSWORD=os.environ.get( u'BELL_ANTF__G_PASSWORD' )
    JSON_OUTPUT_PATH=os.environ.get( u'BELL_ANTF__JSON_OUTPUT_PATH' )
    linker = FilenameLinker()
    linker._print_settings(
        G_EMAIL, G_PASSWORD, JSON_OUTPUT_PATH )
    linker.make_linked_json(
        G_EMAIL, G_PASSWORD, JSON_OUTPUT_PATH )
