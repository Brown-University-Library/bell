"""
Produces a listing of all images in given folder.
Saves to json.
"""

from collections import OrderedDict
import datetime, glob, imghdr, json, os, pprint
import logging
from bell_utils import DATA_DIR


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG,
                    format='[%(asctime)s] %(levelname)s [%(module)s-%(funcName)s()::%(lineno)d] %(message)s',
                    datefmt='%d/%b/%Y %H:%M:%S')


class ImageLister:

    def __init__( self ):
        self.DIRECTORY_PATH = os.environ['BELL_UTILS__IMAGE_DIRECTORY_PATH']
        self.OUTPUT_PATH = os.path.join(DATA_DIR, 'd1__bell_images_listing.json')

    def list_images( self ):
        """ Produces a json list of image file-names. """
        logger.debug( 'in one_offs.make_image_list.ImageLister.list_images(); starting' )
        non_dir_list = self.make_file_list()
        extension_types = self.make_extension_types( non_dir_list )
        discrepancies = self._make_discrepancies_list( non_dir_list[:] )
        directory_info_dict = self.build_response( non_dir_list, extension_types, discrepancies )
        self.output_listing( directory_info_dict )
        pprint.pprint( directory_info_dict )
        return directory_info_dict

    def _make_discrepancies_list( self, filepaths ):
        discrepancies = OrderedDict()
        for f in filepaths:
            imghdr_type = imghdr.what(os.path.join(self.DIRECTORY_PATH, f))
            if 'tif' in f:
                if imghdr_type != 'tiff':
                    discrepancies[f] = imghdr_type or 'unknown'
            elif 'jpeg' in f or 'jpg' in f:
                if imghdr_type != 'jpeg':
                    discrepancies[f] = imghdr_type or 'unknown'
            else:
                if not imghdr_type:
                    discrepancies[f] = imghdr_type or 'unknown'
        return discrepancies

    def make_file_list( self ):
        """ Returns sorted filelist.
            Called by list_images() """
        logger.debug('DIRECTORY_PATH: %s' % self.DIRECTORY_PATH)
        initial_list = glob.glob( self.DIRECTORY_PATH + '/*' )  # includes any directories
        logger.debug( 'initial_list: %s' % initial_list )
        non_dir_list = [value for value in initial_list if os.path.isfile(value) == True]
        logger.debug( 'non_dir_list: %s' % non_dir_list )
        filenames = []
        for path in non_dir_list:
            parts = path.split( '/' )
            filename = parts[-1]
            filenames.append( filename )
        filenames.sort( key=str.lower )
        logger.debug( 'in one_offs.make_image_list.ImageLister.make_file_list(); filenames, `%s`' % pprint.pformat(filenames) )
        return filenames

    def make_extension_types( self, non_dir_list ):
        """ Returns dict of extension-types & counts.
            Called by list_images() """
        extension_types = {}
        for entry in non_dir_list:
            extension_position = entry.rfind( '.' )
            file_extension = 'no_extension' if (extension_position == -1) else entry[ extension_position: ]
            if not file_extension in extension_types:
                extension_types[file_extension] = 1
            else:
                extension_types[file_extension] = extension_types[file_extension] + 1  # the count
        logger.debug( 'in one_offs.make_image_list.ImageLister.make_extension_types(); extension_types, `%s`' % pprint.pformat(extension_types) )
        return extension_types

    def build_response( self, non_dir_list, extension_types, discrepancies ):
        """ Returns directory-info-dict.
            Called by list_images() """
        directory_info_dict =  {
            'count_filelist': len( non_dir_list ),
            'date_time': str( datetime.datetime.now() ),
            'discrepancies': discrepancies,
            'extension_types': extension_types,
            'filelist': non_dir_list, }
        logger.debug( 'in one_offs.make_image_list.ImageLister.build_response(); directory_info_dict, `%s`' % pprint.pformat(directory_info_dict) )
        return directory_info_dict

    def output_listing( self, directory_info_dict ):
        """ Saves json file.
            Called by list_images() """
        jsn = json.dumps( directory_info_dict, indent=2, sort_keys=True )
        with open( self.OUTPUT_PATH, 'w' ) as f:
            f.write( jsn )
        return

    # end class ImageLister



if __name__ == '__main__':
    lister = ImageLister()
    lister.list_images()
