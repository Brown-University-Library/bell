# -*- coding: utf-8 -*-

"""
Produces a listing of all images in given folder.
- Reference: https://developer.github.com/v3/gists/#create-a-gist
"""

import glob, os, pprint
import logging.handlers
from bell_code import bell_logger

logger = bell_logger.setup_logger()


class ImageLister( object ):

    def __init__( self ):
        self.DIRECTORY_PATH = unicode( os.environ[u'BELL_ONEOFF__IMAGE_DIRECTORY_PATH'] )
        self.LEGIT_EXTENSIONS = [ u'*' ]

    def list_images( self ):
        """ Produces a json list of image file-names. """
        logger.debug( u'in one_offs.make_image_list.ImageLister.list_images(); log test' )
        initial_list = glob.glob( self.DIRECTORY_PATH + u'/*' )  # includes any directories
        logger.debug( u'in one_offs.make_image_list.ImageLister.list_images(); initial_list, `%s`' % pprint.pformat(initial_list) )
        non_dir_list = sorted( [value for value in initial_list if os.path.isfile(value) == True] )
        logger.debug( u'in one_offs.make_image_list.ImageLister.list_images(); non_dir_list, `%s`' % pprint.pformat(non_dir_list) )
        # final_list = []
        # for entry in non_dir_list:
        #     for extension in self.LEGIT_EXTENSIONS:
        #         extension_len = len( extension )
        #         logger.debug( u'in one_offs.make_image_list.ImageLister.list_images(); entry, `%s`' % entry )
        #         logger.debug( u'in one_offs.make_image_list.ImageLister.list_images(); extension_len, `%s`' % extension_len )
        #         logger.debug( u'in one_offs.make_image_list.ImageLister.list_images(); extension_len calc, `%s`' % extension_len * -1 )
        #         logger.debug( u'in one_offs.make_image_list.ImageLister.list_images(); match text, `%s`' % entry[ extension_len * -1 ] )
        #         if entry[ extension_len * -1: ] == extension:
        #             final_list.append( entry )
        #             break
        # logger.debug( u'in one_offs.make_image_list.ImageLister.list_images(); final_list, `%s`' % pprint.pformat(final_list) )
        return final_list

    # end class ImageLister



if __name__ == u'__main__':
    lister = ImageLister()
    lister.list_images()


  # def makeImagesListing(self, paths_list=None ):
  #   '''TODO: make the work of this into a webservice'''
  #   import datetime, glob, os
  #   bh = BellHelper()
  #   ## source data
  #   if paths_list == None:
  #     paths_list = [
  #       { u'path': u'/mnt/systems189/bell_gallery', u'extension': '*' },
  #       { u'path': u'/home/bdiana/share2v02/bell_tiffs/TIFF', u'extension': '*' } ]
  #   assert type(paths_list) == list
  #   assert type(paths_list[0]) == dict
  #   assert paths_list[0].keys() == [u'path', u'extension']
  #   assert type(paths_list[0][u'path']) == unicode
  #   assert paths_list[0][u'path'][-1] != u'/'  # path shouldn't end in '/'
  #   rd = result_dict = {
  #     u'combined_listing': {
  #       u'combined_count': u'init',
  #       u'combined_file_list': []
  #       },
  #     u'combined_filetype_listing': {
  #       u'tool_info': {},
  #       u'files': {},
  #       u'file_types': {}
  #       },
  #     u'directory_listing': [
  #       { u'count_filtered_filelist': 0,
  #         u'date_time': u'',
  #         u'directory_path': u'',
  #         u'extension_filter': u'',
  #         u'extension_types': { u'.tiff': 3, u'.xml': 4 },
  #         u'filtered_filelist': [],
  #         },
  #       ]
  #     }
  #   ## directory listing
  #   rd[u'directory_listing'].remove( rd[u'directory_listing'][0] )  # above entry for documentation
  #   for p in paths_list:
  #     directory = p[u'path']
  #     extension = p[u'extension']
  #     ## get list of files
  #     initial_list = glob.glob( directory + u'/*%s' % extension )  # includes any directories
  #     non_dir_list = [value for value in initial_list if os.path.isfile(value) == True ]
  #     non_dir_list.sort( key=unicode.lower )
  #     ## extension types
  #     extension_types = {}
  #     if extension == u'*':
  #       for entry in non_dir_list:
  #         extension_position = entry.rfind( u'.' )
  #         if extension_position == -1:  # extension not found
  #           file_extension = u'no_extension'
  #         else:  # extension found
  #           file_extension = entry[ extension_position: ]
  #         if not file_extension in extension_types:
  #           extension_types[file_extension] = 1
  #         else:
  #           extension_types[file_extension] = extension_types[file_extension] + 1  # the count
  #     else:
  #       extension_types = { extension: len( non_dir_list ) }
  #     d_i_d = directory_info_dict =  {
  #       u'count_filtered_filelist': len( non_dir_list ),
  #       u'date_time': unicode( datetime.datetime.now() ),
  #       u'directory_path': directory,
  #       u'extension_filter': extension,
  #       u'extension_types': extension_types,
  #       u'filtered_filelist': non_dir_list, }
  #     rd[u'directory_listing'].append( d_i_d )
  #   ## combined_listing
  #   single_filelist = []
  #   for dir_info in rd[u'directory_listing']:
  #     single_filelist = single_filelist + dir_info[u'filtered_filelist']
  #   rd[u'combined_listing'][u'combined_count'] = len( single_filelist )
  #   rd[u'combined_listing'][u'combined_file_list'] = sorted(single_filelist)
  #   ## file_type_tool
  #   rd[u'combined_filetype_listing'][u'tool_info'] = bh.getFileToolInfo()
  #   ## file_types
  #   for file_path in rd[u'combined_listing'][u'combined_file_list']:
  #     rd[u'combined_filetype_listing'][u'files'][file_path] = bh.getFileType( file_path )
  #   ## file_types_counts
  #   for item in rd[u'combined_filetype_listing'][u'files'].items():
  #     key = item[0]; value = item[1]  # value, ie, u'image/jpg'
  #     if value in rd[u'combined_filetype_listing'][u'file_types']:
  #       rd[u'combined_filetype_listing'][u'file_types'][value] += 1
  #     else:
  #       rd[u'combined_filetype_listing'][u'file_types'][value] = 1
  #   ## output
  #   t = Task.objects.get( name=u'makeImagesListing', project=u'BELL201207' )
  #   t.output = json.dumps( rd, indent=2, sort_keys=True )
  #   t.save()
