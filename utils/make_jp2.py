# -*- coding: utf-8 -*-

""" (Will) Creates jp2 from specified .jpg or .tif file.
    Takes source master filepath (spaces handled) and destination filepath (no spaces).
    Useful for testing.
    ---
    Not yet complete; TODO: accept source and destination paths as arguments. """


master_filename_raw = item_data_dict[u'object_image_scan_filename']
master_filename_utf8 = master_filename_raw.encode( u'utf-8' )
master_filename_encoded = urllib.quote( master_filename_utf8 ).decode( u'utf-8' )
source_filepath = u'%s/%s' % ( MASTER_IMAGES_DIR_PATH, master_filename_raw )
temp_jp2_filename = master_filename_raw.replace( u' ', u'_' )
jp2_filename = temp_jp2_filename[0:-4] + u'.jp2'
destination_filepath = u'%s/%s' % ( JP2_IMAGES_DIR_PATH, jp2_filename )
self.logger.info( u'in fedora_metadata_updater_and_image_builder.update_existing_metadata_and_create_image(); destination_filepath, %s' % destination_filepath )
image_builder.create_jp2( source_filepath, destination_filepath )
print u'- jp2 created.'
