### overview ###

code related to ingesting bell-gallery images into the bdr.


### flow ###

- convert raw filemaker-pro xml to json
    - foundation/acc_num_to_data.py
    - end result: `accession_number_to_data_dict.json`
    - status: done, but will have to redo once the remaining two imagename/metadata-filename discrepances are fixed.

- run script to get list of files in the images-to-ingest directory
    - utils/make_image_list.py
    - end result: `bell_images_listing.json`
    - status: done

- compare imagefile-filenames to metadata-filenames
    - utils/check_filenames_against_metadata.py
    - console output lists files for which there's no metadata
    - work through discrepancies w/J.C.
    - end result: updated `accession_number_to_data_dict.json`
    - status: done, but will have to redo once the remaining two imagename/metadata-filename discrepances are fixed.

- match metadata accession-numbers to pid #1
    - foundation/acc_num_to_pid.py
    - end result: `accession_number_to_pid_dict_PROD.json` file containing a dict of accession-numbers-to-pids.
    - accession-numbers without pids imply creation of a new metadata-only object (which may gain an associated image below)
    - accession-numbers with pids imply checking to see if fedora metadata needs to be updated
    - status: done

- run through metadata #1
    - create new metadata objects if neccessary
    - status: not done

- match metadata accession-numbers to pid #2
    - rerun step and ensure there are no null pids
    - status: not done

- run through metadata #2
    - update existing metadata object if necessary
    - status: not done

- run through images #1
    - update existing image object if necessary
        - update associated metadata object if necessary (unlikely -- i.e. master jpg becomes master tif)
    - status: not done

- run through images #2
    - create image object if necessary
        - update the previous metadata-only object
    - status: not done

- run a final check to make sure the custom-solr-index and the updated fedora data match
    - status: not done

- let Bell-J.C. & CIS-J.O. know when done
    - status: not done

---
