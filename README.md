### overview ###

code related to ingesting bell-gallery images into the bdr.


### normal ingest flow ###

- get metadata
    - metadata should be for _all_ the records
    - from having all the metadata, the code can determine what image-&-metadata additions to make, what metadata-only additions to make, and make corrections to existing bdr metadata
    - status: done 2016-03-28
        - metadata file from November only contained a few records; J.C. emailed for new file
        - git it, good added it to repo
            - `a__all_data_raw.xml`
            - `b__all_data_formatted.xml`

- get access to image-directory
    - check desktop access
        - status: done 2016-03-26
            - instructions to view images from desktop from March 17 worked
    - check dev `mount` access
        - status: done 2016-03-26

- convert raw filemaker-pro xml to json
    - foundation/acc_num_to_data.py
    - end result: `c__accession_number_to_data_dict.json`
    - status: done 2016-03-27

- run script to get list of files in the images-to-ingest directory
    - utils/make_image_list.py
    - end result: `d__bell_images_listing.json`
    - status: done 2016-04-11

- compare imagefile-filenames to metadata-filenames
    - utils/check_filenames_against_metadata.py
    - console output lists files for which there's no metadata
    - work through discrepancies w/J.C.
    - end result: updated `e__accession_number_to_data_dict.json`
    - status:

- match metadata accession-numbers to pid #1
    - foundation/acc_num_to_pid.py
    - end result: `accession_number_to_pid_dict.json` file containing a dict of accession-numbers-to-pids.
    - accession-numbers without pids imply creation of a new metadata-only object (which may gain an associated image below)
    - accession-numbers with pids imply checking to see if fedora metadata needs to be updated
    - status:

- make metadata-only list
    - tasks/metadata.MetadataOnlyLister.list_metadata_only_accession_numbers()
    - end result: `metadata_only_accession_numbers.json`
    - status:

- create new metadata objects
    - after creates, confirm a re-run of `foundation/acc_num_to_pid.py` results in zero non-pid matches.
    - status:

- make list of images to process
    - tasks/images.ImageLister.make_image_lists()
    - produces a file containing both a list of images to add, and a list of images to update
    - end result: `g_images_to_process.json`
    - status:

- add images
    - tasks/images.run_enqueue_add_image_jobs() -- and tasks/images.run_add_image( filename_dct )
    - many iterations; permissions issues; Meyerowitz fixes; apostrophe-in-filename handling.
    - status:

- update the custom-solr-index
    - prep list of pids from custom-index
        - tasks/indexer.run_make_pids_from_custom_index()
        - end result: `h__pids_from_custom_index_list.json`
        - status:
    - prep pids-to-delete list
        - tasks/indexer.run_make_deletes_list()
        - end result: `i__custom_index_delete_pids.json`
        - review that list
        - status:
    - prep pids-to-update list
        - tasks/indexer.run_make_deletes_list()
        - adds pid to accession-number-to-data-dct data.
        - end result: `j__custom_index_update_data_list.json`
        - status:
    - run updates
        - tasks/indexer.run_enqueue_index_jobs()
        - end result: `k__entries_updated_tracker.json`
        - status:
    - run deletes -- maybe, check w/J.C.
        - tasks/indexer.run_delete_pids()
        - end result: `l__pids_deleted_tracker.json`

- let Bell-J.C. & CIS-J.O. know when done
    - status:

---
