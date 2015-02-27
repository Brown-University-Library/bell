# -*- coding: utf-8 -*-

"""
Produces a listing of all images in given folder.
- Reference: https://developer.github.com/v3/gists/#create-a-gist
"""

import datetime, glob, json, os, pprint
import logging.handlers
from bell_code import bell_logger

logger = bell_logger.setup_logger()


class ImageLister( object ):

    def __init__( self ):
        self.DIRECTORY_PATH = unicode( os.environ[u'BELL_ONEOFF__IMAGE_DIRECTORY_PATH'] )
        self.OUTPUT_PATH = unicode( os.environ[u'BELL_ONEOFF__IMAGE_DIRECTORY_JSON_PATH'] )

    def list_images( self ):
        """ Produces a json list of image file-names. """
        logger.debug( u'in one_offs.make_image_list.ImageLister.list_images(); starting' )
        non_dir_list = self.make_file_list()
        extension_types = self.make_extension_types( non_dir_list )
        directory_info_dict = self.build_response( non_dir_list, extension_types )
        self.output_listing( directory_info_dict )
        pprint.pprint( directory_info_dict )
        return directory_info_dict

    def make_file_list( self ):
        """ Returns sorted filelist.
            Called by list_images() """
        initial_list = glob.glob( self.DIRECTORY_PATH + u'/*' )  # includes any directories
        non_dir_list = [value for value in initial_list if os.path.isfile(value) == True]
        non_dir_list.sort( key=unicode.lower )
        logger.debug( u'in one_offs.make_image_list.ImageLister.list_images(); make_file_list, `%s`' % pprint.pformat(non_dir_list) )
        return non_dir_list

    def make_extension_types( self, non_dir_list ):
        """ Returns dict of extension-types & counts.
            Called by list_images() """
        extension_types = {}
        for entry in non_dir_list:
            extension_position = entry.rfind( u'.' )
            file_extension = u'no_extension' if (extension_position == -1) else entry[ extension_position: ]
            if not file_extension in extension_types:
                extension_types[file_extension] = 1
            else:
                extension_types[file_extension] = extension_types[file_extension] + 1  # the count
        logger.debug( u'in one_offs.make_image_list.ImageLister.make_extension_types(); extension_types, `%s`' % pprint.pformat(extension_types) )
        return extension_types

    def build_response( self, non_dir_list, extension_types ):
        """ Returns directory-info-dict.
            Called by list_images() """
        directory_info_dict =  {
            u'count_filelist': len( non_dir_list ),
            u'date_time': unicode( datetime.datetime.now() ),
            u'directory_path': self.DIRECTORY_PATH,
            u'extension_types': extension_types,
            u'filelist': non_dir_list, }
        logger.debug( u'in one_offs.make_image_list.ImageLister.build_response(); directory_info_dict, `%s`' % pprint.pformat(directory_info_dict) )
        return directory_info_dict

    def output_listing( self, directory_info_dict ):
        """ Saves json file.
            Called by list_images() """
        jsn = json.dumps( directory_info_dict, indent=2, sort_keys=True )
        with open( self.OUTPUT_PATH, u'w' ) as f:
            f.write( jsn )
        return

    # end class ImageLister



if __name__ == u'__main__':
    lister = ImageLister()
    lister.list_images()
