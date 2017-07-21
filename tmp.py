import tempfile
import shutil
import os
import sys

source_filepath = sys.argv[1]

if 'tif' in source_filepath.split( '.' )[-1]:
    if ',' in source_filepath:
        with open(source_filepath, 'rb') as original_tiff:
            with tempfile.NamedTemporaryFile(delete=False) as copy_of_tiff:
                shutil.copyfileobj( original_tiff, copy_of_tiff )
                copy_of_tiff.flush()
                os.fsync(copy_of_tiff.fileno())
                print(copy_of_tiff.name)
