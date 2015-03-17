### overview ###

code related to ingesting bell-gallery images into the bdr.


### normal ingest flow ###

- convert raw filemaker-pro xml to json
    - foundation/acc_num_to_data.py
    - end result: `accession_number_to_data_dict.json`
    - status: done; discrepancies sorted out march-3; rerun with stripped accession-number keys march-4.

- run script to get list of files in the images-to-ingest directory
    - utils/make_image_list.py
    - end result: `bell_images_listing.json`
    - status: done; march-3.

- compare imagefile-filenames to metadata-filenames
    - utils/check_filenames_against_metadata.py
    - console output lists files for which there's no metadata
    - work through discrepancies w/J.C.
    - end result: updated `accession_number_to_data_dict.json`
    - status: done; march-3; rerun march-4 w/updated metadata keys; no difference.

- match metadata accession-numbers to pid #1
    - foundation/acc_num_to_pid.py
    - end result: `accession_number_to_pid_dict.json` file containing a dict of accession-numbers-to-pids.
    - accession-numbers without pids imply creation of a new metadata-only object (which may gain an associated image below)
    - accession-numbers with pids imply checking to see if fedora metadata needs to be updated
    - status: done; march-4; rerun march-4 w/updated metadata keys; found 9 extra pids.

- make metadata-only list
    - tasks/metadata.MetadataOnlyLister.list_metadata_only_accession_numbers()
    - end result: `metadata_only_accession_numbers.json`
    - status: done; march-4; rerun march-4 w/updated metadata keys; count down to 105 from 114 extra pids.

- create new metadata objects
    - after creates, confirm a re-run of `foundation/acc_num_to_pid.py` results in zero non-pid matches.
    - status: done; march-6.

- make list of images to process
    - tasks/images.ImageLister.make_image_lists()
    - produces a file containing both a list of images to add, and a list of images to update
    - end result: `g_images_to_process.json`
    - status: done; march-9.

- add images
    - tasks/images.run_enqueue_add_image_jobs() -- and tasks/images.run_add_image( filename_dct )
    - many iterations; permissions issues; Meyerowitz fixes; apostrophe-in-filename handling.
    - status: done; march-16.

- update the custom-solr-index
    - prep list of pids from custom-index
        - tasks/indexer.run_make_pids_from_custom_index()
        - end result: `h__pids_from_custom_index_list.json`
        - status: in-process
    - prep pids-to-update, and pids-to-delete lists
        - tasks/indexer.run_make_update_and_delete_pids_lists()
        - end result: `i__update_and_delete_pids.json`
        - NOTE: This contains a dict of two lists, pids-to-delete-from-the-custom-index, and pids-to-delete-from-the-bdr.
                Review carefully.
    - run updates
        - tasks/indexer.run_update_pids()
        - end result: `j__pids_updated_list.json`
    - run deletes
        - tasks/indexer.run_delete_pids()
        - end result: `k__pids_deleted_lists.json`
    - status: at `prep list of pids from bdr`

- let Bell-J.C. & CIS-J.O. know when done
    - status: not done

---
