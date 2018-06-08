### overview ###

code related to ingesting bell-gallery images into the bdr.

---

### code architecture/layout ###

- code for tasks below - goes in tasks/
- one-off scripts go in one_offs/

---

### normal ingest flow ###

- TODOs at the bottom - investigate before processing another batch.

- get metadata
    - metadata should be for _all_ the records
        - NOTE: we need the FileMaker exported format 'FMPXMLRESULT'. (Occasionally we have mistakenly received a different format.)
    - from having all the metadata, the code can determine what image-&-metadata additions to make, what metadata-only additions to make, and make corrections to existing bdr metadata
    - status:
        - DONE; added to git 2018-May-10; re-added new data 2018-May-17; re-added new data 2018-May-21.
            - `a__all_data_raw.xml`

- format metadata
    - why: makes it easy to see the source fields, and if necessary check source xml
    - tasks/format_xml_file.py
    - end result: `b__all_data_formatted.xml`
    - status: DONE 2018-May-10; re-added new data 2018-May-17; re-added new data 2018-May-21.

- get access to image-directory
    - check dev `mount` access
        - notes:
            - the specific mount directory changes each run, so check that our symlink is correct.
            - the share path is in a settings comment.
        - status: DONE 2018-May-11

- convert raw filemaker-pro xml to json
    - tasks/acc_num_to_data.py
    - end result: `c__accession_number_to_data_dict.json`
    - status:
        - DONE 2018-May-12; re-done w/new data 2018-May-17; re-done w/new data 2018-May-21.

- run script to get list of files in the images-to-ingest directory
    - tasks/make_image_list.py
    - end result: `d1__bell_images_listing.json`
    - status: DONE 2018-May-14; re-done since i thought some filenames were changed, but not, no problem - 2018-May-17; re-done w/new metadata 2018-May-21.

- compare imagefile-filenames to metadata-filenames
    - tasks/check_filenames_against_metadata.py
        - NOTE: update the line:

                filenames_subset = self.filter_filenames( filenames_dct, [ '.22', '.tif', '.tiff' ] )  # from manual inspection of d__bell_images_listing.json

            ...with the appropriate suffixes from the d1__bell_images_listing.json - 'extension_types'

        - NOTE: code _will_ count as a 'find' an image-filename ending in, say, '.tif', even if, in c__accession_number....json, the filename does _not_ contain that extension.

    - end result: `data/d2__images_metadata_comparison.json` -- work through any not-matched files with J.C. before proceeding.
    - status:
        DONE 2018-May-21
        - 2018-May-14 - emailed J.C. list of non-matches
        - 122 image-directory filenames _were_ found in the metadata
        - 14 image-directory filenames were not found in the metadata.
        - 2018-May-15 - emailed J.C. explanation of remaining non-matches.
        - 2018-May-16 - emailed J.C. request for correct-metadata format.
        - 2018-May-17 - emailed J.C. explanation of remaining single non-match.
        - 2018-May-21 - all image-directory filenames now found.

- match metadata accession-numbers to pid #1
    - tasks/acc_num_to_pid.py
    - end result: `e1__accession_number_to_pid_dict.json` file containing a dict of accession-numbers-to-pids.
    - accession-numbers without pids imply creation of a new metadata-only object (which may gain an associated image later)
    - accession-numbers with pids imply checking to see if fedora metadata needs to be updated
    - note, after metadata-only objects are created, this will be re-run until the `count_null` is zero.
    - status: IN-PROCESS
        - run 2018-May-21; shows 220 accession-numbers without pids
        - run 2018-May-23 after updating 1 bdr object's accession number; shows 219 accession-numbers without pids

- check for, confirm, and delete old bell items from fedora
    - note: this step is being done here because:
        - we wanted to check with J.C. as early as possible re any images that we may need to check (this is the last interactive step with J.C. so it's nice having it here, early in the process, close to the previous image-checking steps).
        - we need the data created in the previous step: `match metadata accession-numbers to pid`
    - steps:
        - make list of bdr pids to delete.
            - NOTE: check whether an object to delete has an image - just update the metadata?
            - `from tasks import cleanup; cleanup.run_make_bdr_pids_to_delete()`
            - end result: `e2__bdr_pids_to_delete.json`
        - manually verify -- with J.C. -- objects to delete, if they have images.
            - explanation
                - if an accession number changes, then we would see that as a new object
                - then the old object (with the old accession number) would need to be deleted
                - if that object to be deleted has an image, then we might lose the image, because she wouldn't send us the image if it wasn't updated.
                - so, any items to delete with an image - check with her.
            - status: 2018-May-22, DONE
                - emailed J.C. re the 22 found.
                - essential result: we updated one bdr object's accession-number to preserve bdr image history
        - manually run deletion.
            - NOTE: when beginning this process, manually cleanup the `e3__bdr_entries_deleted_tracker.json` file (DON'T DELETE) to `{}`.
                - This is because each individual deletion updates this status-dict.
            - `from tasks import cleanup; cleanup.run_delete_single_pid_from_bdr( pid )`
            - end result: `e3__bdr_entries_deleted_tracker.json`
            - status: 2018-Jun-06: DONE

- make metadata-only list
    - note: this creates metadata-only objects, some of which may be updated in a later step to also have image-datastreams.
    - tasks/metadata.run_metadata_only_lister()
    - to run (from `bell_code` directory)...

            >>> from tasks import metadata
            >>> metadata.run_metadata_only_lister()

    - end result: `f1__metadata_only_accession_numbers.json`
    - status:
        - run 2018-Jun-07

- create new metadata objects
    - tasks/metadata.run_create_metadata_only_objects()

    - to run (from `bell_code` directory)...

            >>> from tasks import metadata
            >>> metadata.run_create_metadata_only_objects()

    - to confirm that object is in fedora:
            https://api_search/?q=mods_id_bell_accession_number_ssim:the%20accession_number

    - tracker result: `f2__metadata_obj_tracker.json`
    - after creates, confirm a re-run of `foundation/acc_num_to_pid.py` results in zero non-pid matches.
        - note that this re-run will update, as it should, `e__accession_number_to_pid_dict.json` -- the dict of accession-numbers-to-pids.
    - status: IN-PROCESS 2018-June-07
        - 2017 note: also reran prep scripts to confirm that all accession-numbers have bdr pids

- make image-filename to data dct
    - produces a dct and then json file like:

            {
            u'Zorn PR_1971.705.tif': {u'accession_number': u'PR 1971.705',
                                      u'pid': u'bdr:301594'},
            u'Zorn PR_1971.709.tif': {u'accession_number': u'PR 1971.709',
                                      u'pid': u'bdr:301595'}
            }

    - script: tasks/images.run_make_image_filename_dct()
    - to run (from `bell_code` directory)...

            >>> from tasks import images
            >>> images.run_make_image_filename_dct()

    - end result: `fg__images_filename_dct.json`
        - note that at the bottom of that file are any files that were excluded from the filename-dct.
    - status: done, 2017-07-05.


- make list of images to process
    - tasks/images.ImageLister.make_image_lists()

    - to run (from `bell_code` directory)...

            >>> from tasks import images
            >>> images.run_make_image_lists()

    - produces a file containing both a list of images to add, and a list of images to update
    - end result: `g_images_to_process.json`
    - status: done: 2017-07-05

- add new images
    - tasks/images.run_enqueue_add_image_jobs()
    - to run (from `bell_code` directory)...

            >>> from tasks import images
            >>> images.run_enqueue_add_image_jobs()

    - for adding individual images, if necessary: tasks/images.run_add_image( filename_dct )
    - status: done 2017-07-06

- TODO: make sure jpg images get an image/jpeg mimetype in Fedora
    - add a code step that verifies the mimetype in Fedora

- add updated images
    - tasks/images.run_enqueue_add_image_jobs()
    - to run (from `bell_code` directory)...

            >>> from tasks import images
            >>> images.run_enqueue_update_image_jobs()

    - for adding individual images, if necessary: tasks/images.run_add_image( filename_dct )
    - status: done 2017-07-21

- purge caches for updated images
    - loris cache (info, source, derivatives) - DONE 8/1/2017
    - thumbnail in fedora & django cache - DONE 8/1/2017


- update the custom-solr-index
    - make solr pids list
        - tasks/indexer.run_make_solr_pids_list()
        - end result: j__solr_pids_list.json
        - note: h__x.json & i__x.json were files that are no longer needed
        - status: done 2017-08-04
    - create solr data file
        - tasks/indexer.run_create_solr_data()
        - end result: k__data_for_solr.json
        - status: done 2017-08-07
    - delete & post records to solr
        - tasks/indexer.update_solr_core()
        - status: done 2017-08-11

- let Bell-J.C. & CIS-J.B. know when done
    - status: done via BJD email, 2017-Aug-11-Friday

---

### Some bdr-api url calls for reference...

_(no indent=true available)_

- [search api for item](https://repository.library.brown.edu/api/search/?q=pid:%22bdr:10876%22)
- [search api for collection](https://repository.library.brown.edu/api/search/?q=rel_is_member_of_ssim:%22bdr:10870%22)
- [search api on name](https://repository.library.brown.edu/api/search/?q=name:%22Rivera,%20Diego%22)
- [search api on pid and name](https://repository.library.brown.edu/api/search/?q=pid:%22bdr:10876%22&fq=name:%22Rivera,%20Diego%22)
- [collection and name (goal)](https://repository.library.brown.edu/api/search/?q=rel_is_member_of_ssim:%22bdr:10870%22&fq=name:%22Rivera,%20Diego%22)
- [collection and name wildcard (goal)](https://repository.library.brown.edu/api/search/?q=rel_is_member_of_ssim:%22bdr:10870%22&fq=name:%22Rivera*%22)

---

### TODOs...

- switch ```'utf-8', 'replace'``` to ```'utf-8'```
- in `acc_num_to_pid.py`, change function names that indicate solr is being hit, to names that indicate that bdr-search-api is being hit.
- review tests and edit or delete those not used.
    - consider incorporating test-run into README flow.
- low-importance interesting idea -- create a script to check file-access dates to determine old-unused code.
- switch paths in data/ to explicit relative paths, instead of settings
- consider validating mods before sending it to api (tho api does validate it )
- look into updating BDR metadata if Bell metadata has changed
- Normalization
    - check all images have MASTER & JP2 datastreams (not TIFF, ...)
    - check MASTER & JP2 headers: should have a filename, content type for JP2s, ... (HEAD request works for this)
- review one_off scripts and get rid of least-necessary scripts
- get rid of any utils/ files that we haven't used
- low-priority: consider special-casing the object-type determination of bell-objects

---

---
