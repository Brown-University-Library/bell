# -*- coding: utf-8 -*-

"""
Checks filenames against metadata.
- checks stripped file-filename against stripped metadata-filename.
- checks the stripped file-filename with and without its extension against the stripped metadata-filename.
"""

import datetime, glob, json, os, pprint
import logging.handlers
from bell_code import bell_logger

logger = bell_logger.setup_logger()


class Checker( object ):

    def __init__( self ):
        self.METADATA_PATH = unicode( os.environ[u'BELL_UTILS__JSON_METADATA_PATH'] )
        self.FILENAMES_PATH = unicode( os.environ[u'BELL_UTILS__JSON_FILENAMES_PATH'] )

    def check_filenames( self ):
        """ Checks filenames against metadata. """
        ( metadata_dct, filenames_dct ) = self.loadup()
        filenames_subset = self.filter_filenames( filenames_dct, [ u'.22', u'.tif', u'.tiff' ] )  # from manual inspection of filenames json
        metadata_subset_dct = self.filter_metadata( metadata_dct )
        compare_dct = self.compare( filenames_subset, metadata_subset_dct )
        return

    def loadup( self ):
        """ Reads json and returns dicts.
            Called by check_filenames() """
        with open( self.METADATA_PATH ) as f:
            metadata_dct = json.loads( f.read() )
        with open( self.FILENAMES_PATH ) as f2:
            filenames_dct = json.loads( f2.read() )
        logger.debug( u'in one_offs.check_filenames_against_metadata.Checker.loadup(); metadata_dct.keys(), `%s`' % sorted(metadata_dct.keys()) )
        logger.debug( u'in one_offs.check_filenames_against_metadata.Checker.loadup(); filenames_dct.keys(), `%s`' % sorted(filenames_dct.keys()) )
        return ( metadata_dct, filenames_dct )

    def filter_filenames( self, filenames_dct, extensions_list ):
        """ Returns a subset of all directory filenames (stripped).
            Called by check_filenames() """
        logger.debug( u'in one_offs.check_filenames_against_metadata.Checker.filter_filenames(); extensions_list, `%s`' % extensions_list )
        filenames_subset = []
        for filename in filenames_dct[u'filelist']:
            parts = filename.split( u'.' )
            ext = parts[ -1 ]
            if u'.'+ext in extensions_list:
                filenames_subset.append( filename.strip() )
            else:
                logger.debug( u'in one_offs.check_filenames_against_metadata.Checker.filter_filenames(); skipped filename, `%s`' % filename )
        return filenames_subset

    def filter_metadata( self, metadata_dct ):
        """ Returns a simple dict of (stripped) filenames and accession-numbers to make matching easier.
            Called by check_filenames() """
        metadata_subset_dct = {}
        entries = metadata_dct[u'items']
        for (key, val) in entries.items():
            filename = val[u'object_image_scan_filename']
            # logger.debug( u'in one_offs.check_filenames_against_metadata.Checker.filter_metadata(); filename, `%s`' % filename )
            if filename:
                accession_num = key
                metadata_subset_dct[ filename.strip() ] = accession_num
        logger.debug( u'in one_offs.check_filenames_against_metadata.Checker.filter_metadata(); metadata_subset_dct, `%s`' % pprint.pformat(metadata_subset_dct) )
        return metadata_subset_dct

    def compare( self, filenames_subset, metadata_subset_dct ):
        """ Runs comparison.
            Called by check_filenames() """
        ( count_found, count_not_found, found_lst, not_found_lst, metadata_filenames ) = ( 0, 0, [], [], metadata_subset_dct.keys() )
        for filename in filenames_subset:
            if (filename in metadata_filenames) or (filename[ : filename.rfind(u'.') ] in metadata_filenames):
                count_found += 1
                found_lst.append( filename )
            else:
                count_not_found += 1
                not_found_lst.append( filename )
        compare_dct = { u'count_found': count_found, u'count_not_found': count_not_found, u'found_lst': found_lst, u'not_found_lst': not_found_lst }
        logger.debug( u'in one_offs.check_filenames_against_metadata.Checker.compare(); compare_dct, `%s`' % pprint.pformat(compare_dct) )
        return compare_dct

    # end class Checker



if __name__ == u'__main__':
    checker = Checker()
    checker.check_filenames()
