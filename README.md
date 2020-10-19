### overview ###

code related to ingesting bell-gallery images into the bdr.

---

### code architecture/layout ###

- code for tasks below - goes in tasks/
- one-off scripts go in one_offs/

---

### normal ingest flow ###

- TODOs at the bottom - investigate before processing another batch.

- get access to image-directory
    - check dev `mount` access
        - notes:
            - the specific mount directory changes each run, so check that our symlink is correct (need to use the TIFF directory - there could be another JPG directory, but just ignore it)
            - the share path is in a settings comment.
        - status: DONE 2020-10-19

- get metadata
    - metadata should be for _all_ the records
        - NOTE: we need the FileMaker exported format 'FMPXMLRESULT'. (Occasionally we have mistakenly received a different format.)
    - from having all the metadata, the code can determine what image-&-metadata additions to make, what metadata-only additions to make, and make corrections to existing bdr metadata
    - end result: `a__all_data_raw.xml`
    - status:
        - DONE 2020-10-19

- format metadata
    - why: makes it easy to see the source fields, and if necessary check source xml
    - tasks/format_xml_file.py
    - end result: `b__all_data_formatted.xml`
    - status: DONE 2020-10-19

- convert raw filemaker-pro xml to json
    - tasks/acc_num_to_data.py
    - end result: `c__accession_number_to_data_dict.json`
    - status:
        - DONE 2020-10-19

- run script to get list of files in the images-to-ingest directory
    - tasks/make_image_list.py
    - end result: `d1__bell_images_listing.json`
    - status: DONE 2020-10-19

- compare imagefile-filenames to metadata-filenames
    - tasks/check_filenames_against_metadata.py
        - NOTE: update the line:

                filenames_subset = self.filter_filenames( filenames_dct, [ '.22', '.tif', '.tiff' ] )  # from manual inspection of d__bell_images_listing.json

            ...with the appropriate suffixes from the d1__bell_images_listing.json - 'extension_types'

        - NOTE: code _will_ count as a 'find' an image-filename ending in, say, '.tif', even if, in c__accession_number....json, the filename does _not_ contain that extension.

    - end result: `data/d2__images_metadata_comparison.json` -- work through any not-matched files with J.C. before proceeding.
    - status:
        DONE 2020-10-19

- match metadata accession-numbers to pid #2
    - tasks/acc_num_to_pid.py
    - end result: `e1__accession_number_to_pid_dict.json` file containing a dict of accession-numbers-to-pids.
    - accession-numbers without pids imply creation of a new metadata-only object (which may gain an associated image later)
    - accession-numbers with pids imply checking to see if fedora metadata needs to be updated
    - note, after metadata-only objects are created, this will be re-run until the `count_null` is zero.
    - status: 2020-10-19; shows 289 accession-numbers without pids

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
            - status: 2019-08-06 - 1 (metadata) pid found to delete - nothing to verify
            - status: 2019-08-27 - many metadata pids found to delete - nothing to verify
        - manually run deletion.
            - NOTE: when beginning this process, manually cleanup the `e3__bdr_entries_deleted_tracker.json` file (DON'T DELETE) to `{}`.
                - This is because each individual deletion updates this status-dict.
            - `from tasks import cleanup; cleanup.run_delete_single_pid_from_bdr( pid )`
            - end result: `e3__bdr_entries_deleted_tracker.json`
            - status: 2019-08-06 DONE
            - status: 2019-08-27 DONE

- make metadata-only list
    - tasks/metadata.run_metadata_only_lister()
    - to run (from `bell_code` directory)...

            >>> from tasks import metadata
            >>> metadata.run_metadata_only_lister()

    - end result: `f1__metadata_only_accession_numbers.json`
    - status:
        - DONE 2019-08-06, 2019-08-27

- create new metadata objects
    - note: this creates metadata-only objects, some of which may be updated in a later step to also have image-datastreams.
    - note: first manually clean out the file `f2__metadata_obj_tracker.json` by replacing the old data with `{}`
    - tasks/metadata.run_create_metadata_only_objects()
    - to run (from `bell_code` directory)...

            >>> from tasks import metadata
            >>> metadata.run_create_metadata_only_objects()

    - to confirm that object is in fedora:
            https://api_search/?q=mods_id_bell_accession_number_ssim:the%20accession_number

    - tracker result: `f2__metadata_obj_tracker.json`
    - after creates, confirm a re-run of `tasks/acc_num_to_pid.py` results in zero non-pid matches.
        - note that this re-run will update, as it should, `e1__accession_number_to_pid_dict.json` -- the dict of accession-numbers-to-pids.
    - status:
        - new-metadata-objects-created - DONE 2019-08-07, 2019-08-27
        - `acc_num_to_pid.py` - DONE 2019-08-07, 2019-08-27

- update metadata for existing objects in the BDR, if needed
    - explanation: for each accession-number -- this prepares the data for the bell-json-datastream from the source data, and compares it to the item's bell-json-datastream from the bdr-item. If there is a difference, we update the bdr object's bell-json-datastream and the bdr object's mods-datastream
    - tasks/metadata.run_update_metadata_if_needed()
    - status: DONE 2019-08-07, 2019-08-27

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

    - end result: `g1__images_filename_dct.json`
        - note that at the bottom of that file are any files that were excluded from the filename-dct.
    - status: DONE 2019-08-07, 2019-08-27

- make list of images to process
    - tasks/images.ImageLister.make_image_lists()

    - to run (from `bell_code` directory)...

            >>> from tasks import images
            >>> images.run_make_image_lists()

    - produces a file containing both a list of images to add, and a list of images to update
    - end result: `g2__images_to_process.json`
    - status: DONE 2019-08-07, 2019-08-27

- add images
    - tasks/images.add_images()
    - note:
        - this merges the to-add list and the to-update list; they're both processed the same way.
        - TODO for next run: normalize the log statements (get rid of self.logger)
    - to run (from `bell_code` directory)...

            >>> from tasks import images
            >>> images.add_images()

    - updates the `g2__images_to_process.json` file's `lst_images_to_add` and `lst_images_to_update` lists of image-dict-data with a `'status': ingested_(timestamp)` dict-entry.
    - note: verify that JP2 job was run for recreating JP2 when MASTER was updated
    - status: DONE 2019-08-07 (& cleaned up failed JP2 jobs), 2019-08-27 (no images)

- purge caches for updated images
    - loris cache (info, source, derivatives) - DONE 2019-08-07
    - thumbnails in fedora re-created - DONE (happened automatically when MASTER was overwritten)
    - django thumbnail cache - DONE 2019-08-07 
    - N/A for 2019-08-27

- create final json file for CIS
    - make solr pids list
        - tasks/indexer.run_make_solr_pids_list()
        - end result: j__solr_pids_list.json
        - status: DONE 2019-08-07, 2019-08-27
    - create solr data file
        - NOTE: when beginning this process, manually empty the `k__data_for_solr.json` file (DON'T DELETE) to `{}`.
            - explanation: this will allow the following code to be run with a built-in 'tracker' capability.
        - tasks/indexer.run_create_solr_data()
        - end result: k__data_for_solr.json
        - status: DONE 2019-08-07, 2019-08-27

- let Bell-J.C. & CIS know when done
    - status: DONE 2019-08-07, 2019-08-27

- 8/26/2019 - repost some updated images
- 8/27/2019 - new export to remove some records

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

- update BDR API calls: don't have to pass in dsID, and JP2 will automatically be generated for new image objects that don't have it.
- combine steps where possible, eg initial data-file preparation
- at some point early on when making a BDR-API call, confirm that a BDR image exists if J.C.'s metadata field `object_image_scan_filename` is not null.
- switch ```'utf-8', 'replace'``` to ```'utf8'```

(at least do the two above)

- questions/ideas from J.R.: link from BDR site over to Bell site; should Bell metadata-only object go into the BDR, if it's not the system of record? Also, could have a data dictionary for the metadata fields coming from J.C. - a label and short explanation of what it means. Could also make the Bell metadata the primary metadata in the BDR, instead of creating a minimal MODS.
    - *note from J.C. - all the metadata does need to be on the website, which means it needs to be in the k__ file we create. We could still decide not to create all the objects in the BDR if we want.
- add to the metadata-production code a check to ensure there's a title, if not, put in 'No Title' for the mods-datastream (bell json stream is ok).
- in `acc_num_to_pid.py`, change function names that indicate solr is being hit, to names that indicate that bdr-search-api is being hit.
- review tests and edit or delete those not used.
    - consider incorporating test-run into README flow.
- low-importance interesting idea -- create a script to check file-access dates to determine old-unused code.
- Normalization
    - check all images have MASTER & JP2 datastreams (not TIFF, ...)
- review one_off scripts and get rid of least-necessary scripts
- get rid of any utils/ files that we haven't used
- low-priority: consider special-casing the object-type determination of bell-objects
- see if a linter can list all the environmental-variables used by the code -- and remove from settings.sh those not used.
- if next time this project is run (meaning after June 2018), loris and fedora cache-clearning isn't fully incorporated into the BDR-api, consider creating a programmatic api-wrapper around existing code for this step.

---

---
