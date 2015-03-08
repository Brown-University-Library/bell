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
    - status: in process

- add images
    - after adds, confirm a re-run of `tasks/images.ImagesToAddLister.list_images()` results in zero images-to-add
    - status: not done

- make metadata-update list
    - TODO
    - end result: `metadata_updates.json`
    - status: not done

- update metadata objects
    - status: not done

- make image-replace list
    - TODO
    - end result: `image_replacements.json`
    - status: not done

- replace images

- run a final check to make sure the custom-solr-index and the updated fedora data match
    - status: not done

- let Bell-J.C. & CIS-J.O. know when done
    - status: not done

---
