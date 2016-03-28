# -*- coding: utf-8 -*-

from __future__ import unicode_literals

"""
Merges a metadata json subset file into a complete json metadata file.
- outputs to a new complete json file.
- example use-case:
  - J.C. provides full metadata file.
  - I find issue with metadata filenames for, say, 50 images.
  - J.C. fixes filenames in db and exports the 50 metadata records.
  - I want to merge the 50 updated metadata-records into the complete set of medata records.
"""

import datetime, json, os, pprint
import logging.handlers
from bell_code import bell_logger

logger = bell_logger.setup_logger()


class Merger( object ):

    def __init__( self ):
        self.SOURCE_FULL_JSON_METADATA_PATH = unicode( os.environ['BELL_ONEOFF__SOURCE_FULL_JSON_METADATA_PATH'] )
        self.SOURCE_SUBSET_JSON_METADATA_PATH = unicode( os.environ['BELL_ONEOFF__SOURCE_SUBSET_JSON_METADATA_PATH'] )
        self.OUTPUT_PATH = unicode( os.environ['BELL_ONEOFF__OUTPUT_FULL_JSON_METADATA_PATH'] )

    def merge_data( self ):
        """ Merges subset into full set; outputs new full set. """
        ( initial_full_dct, subset_dct ) = self.loadup()
        key_val_lst = sorted( subset_dct['items'].items() )
        for (accession_num, item_dct) in key_val_lst:
            initial_full_dct['items'][accession_num] = item_dct
        jsn = json.dumps( initial_full_dct, indent=2, sort_keys=True )
        with open( self.OUTPUT_PATH, 'w' ) as f:
            f.write( jsn )
        return

    def loadup( self ):
        """ Reads json and returns dicts.
            Called by merge_data() """
        with open( self.SOURCE_FULL_JSON_METADATA_PATH ) as f:
            metadata_dct = json.loads( f.read() )
        with open( self.SOURCE_SUBSET_JSON_METADATA_PATH ) as f2:
            subset_dct = json.loads( f2.read() )
        logger.debug( 'in one_offs.merge_subset_json_into_complete_json.Merger.loadup(); metadata_dct item_count, `%s`' % len(metadata_dct['items']) )
        logger.debug( 'in one_offs.merge_subset_json_into_complete_json.Merger.loadup(); subset_dct item_count, `%s`' % len(subset_dct['items']) )
        return ( metadata_dct, subset_dct )

    # end class Merger


# class Checker( object ):

#     def __init__( self ):
#         self.METADATA_PATH = unicode( os.environ['BELL_ONEOFF__JSON_METADATA_PATH'] )
#         self.FILENAMES_PATH = unicode( os.environ['BELL_ONEOFF__JSON_FILENAMES_PATH'] )
#         # self.OUTPUT_PATH = unicode( os.environ['BELL_ONEOFF__JSON_MATCH_PATH'] )

#     def check_filenames( self ):
#         """ Checks filenames against metadata. """
#         ( metadata_dct, filenames_dct ) = self.loadup()
#         filenames_subset = self.filter_filenames( filenames_dct, [ '.22', '.tif', '.tiff' ] )  # from manual inspection of filenames json
#         metadata_subset_dct = self.filter_metadata( metadata_dct )
#         compare_dct = self.compare( filenames_subset, metadata_subset_dct )
#         return

#     def loadup( self ):
#         """ Reads json and returns dicts.
#             Called by check_filenames() """
#         with open( self.METADATA_PATH ) as f:
#             metadata_dct = json.loads( f.read() )
#         with open( self.FILENAMES_PATH ) as f2:
#             filenames_dct = json.loads( f2.read() )
#         logger.debug( 'in one_offs.check_filenames_against_metadata.Checker.loadup(); metadata_dct.keys(), `%s`' % sorted(metadata_dct.keys()) )
#         logger.debug( 'in one_offs.check_filenames_against_metadata.Checker.loadup(); filenames_dct.keys(), `%s`' % sorted(filenames_dct.keys()) )
#         return ( metadata_dct, filenames_dct )

#     def filter_filenames( self, filenames_dct, extensions_list ):
#         """ Returns a subset of all directory filenames (stripped).
#             Called by check_filenames() """
#         logger.debug( 'in one_offs.check_filenames_against_metadata.Checker.filter_filenames(); extensions_list, `%s`' % extensions_list )
#         filenames_subset = []
#         for filename in filenames_dct['filelist']:
#             parts = filename.split( '.' )
#             ext = parts[ -1 ]
#             if '.'+ext in extensions_list:
#                 filenames_subset.append( filename.strip() )
#             else:
#                 logger.debug( 'in one_offs.check_filenames_against_metadata.Checker.filter_filenames(); skipped filename, `%s`' % filename )
#         return filenames_subset

#     def filter_metadata( self, metadata_dct ):
#         """ Returns a simple dict of (stripped) filenames and accession-numbers to make matching easier.
#             Called by check_filenames() """
#         metadata_subset_dct = {}
#         entries = metadata_dct['items']
#         for (key, val) in entries.items():
#             filename = val['object_image_scan_filename']
#             # logger.debug( 'in one_offs.check_filenames_against_metadata.Checker.filter_metadata(); filename, `%s`' % filename )
#             if filename:
#                 accession_num = key
#                 metadata_subset_dct[ filename.strip() ] = accession_num
#         logger.debug( 'in one_offs.check_filenames_against_metadata.Checker.filter_metadata(); metadata_subset_dct, `%s`' % pprint.pformat(metadata_subset_dct) )
#         return metadata_subset_dct

#     def compare( self, filenames_subset, metadata_subset_dct ):
#         """ Runs comparison.
#             Called by check_filenames() """
#         ( count_found, count_not_found, found_lst, not_found_lst, metadata_filenames ) = ( 0, 0, [], [], metadata_subset_dct.keys() )
#         for filename in filenames_subset:
#             if (filename in metadata_filenames) or (filename[ : filename.rfind('.') ] in metadata_filenames):
#                 count_found += 1
#                 found_lst.append( filename )
#             else:
#                 count_not_found += 1
#                 not_found_lst.append( filename )
#         compare_dct = { 'count_found': count_found, 'count_not_found': count_not_found, 'found_lst': found_lst, 'not_found_lst': not_found_lst }
#         logger.debug( 'in one_offs.check_filenames_against_metadata.Checker.compare(); compare_dct, `%s`' % pprint.pformat(compare_dct) )
#         return compare_dct

#     # end class Checker



if __name__ == '__main__':
    merger = Merger()
    merger.merge_data()
