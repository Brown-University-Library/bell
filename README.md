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
    - from having all the metadata, the code can determine what image-&-metadata additions to make, what metadata-only additions to make, and make corrections to existing bdr metadata
    - status:
        - done; added to git 2018-05-10
            - `a__all_data_raw.xml`

- format metadata
    - why: makes it easy to see the source fields, and if necessary check source xml
    - tasks/format_xml_file.py
    - end result: `b__all_data_formatted.xml`
    - status:
        - in process

- get access to image-directory
    - check desktop access
        - status: done 2017-06-26
    - check dev `mount` access
        - status: done 2017-06-26

- convert raw filemaker-pro xml to json
    - foundation/acc_num_to_data.py
    - end result: `c__accession_number_to_data_dict.json`
    - status:
        - done 2017-06-26
        - redone 2017-06-29 with J.C.'s corrections

- run script to get list of files in the images-to-ingest directory
    - utils/make_image_list.py
    - end result: `d__bell_images_listing.json`
    - status:
        - done 2017-06-27
        - redone 2017-06-29 with J.C.'s corrections

- compare imagefile-filenames to metadata-filenames
    - utils/check_filenames_against_metadata.py
        - NOTE: update the line:

                filenames_subset = self.filter_filenames( filenames_dct, [ '.22', '.tif', '.tiff' ] )  # from manual inspection of d__bell_images_listing.json

            ...with the appropriate suffixes from the d__bell_images_listing - 'extension_types'

        - NOTE: code _will_ count as a 'find' an image-filename ending in, say, '.tif', even if, in c__accession_number....json, the filename does _not_ contain that extension.

    - end result: `data/d2__images_metadata_comparison.json` -- work through any not-matched files with J.C. before proceeding.
    - work:
        - emailed J.C. list of non-matches 2017-06-28
        - 38 image-directory filenames were found in the metadata
        - 67 image-directory filenames were not found in the metadata.
        - got a new file from J.C., reran matching and after one manual imgage filename corection are all set.
    - status: idone 2017-06-29.

- match metadata accession-numbers to pid #1
    - foundation/acc_num_to_pid.py
    - end result: `e__accession_number_to_pid_dict.json` file containing a dict of accession-numbers-to-pids.
    - accession-numbers without pids imply creation of a new metadata-only object (which may gain an associated image later)
    - accession-numbers with pids imply checking to see if fedora metadata needs to be updated
    - status:
        - run 2017-06-28; shows 97 accession-numbers without pids
        - rerun 2017-06-29 after new metadata file from J.C.; shows 97 accession-numbers without pids (one changed pid from previous list)
        - done 2017-07-03 -- no acession numbers without pids.

- make metadata-only list
    - tasks/metadata.run_metadata_only_lister()
    - to run (from `bell_code` directory)...

            >>> from tasks import metadata
            >>> metadata.run_metadata_only_lister()

    - end result: `f__metadata_only_accession_numbers.json`
    - status:
        - run 2017-06-29
        - rerun 2017-06-29 after new metadata file from J.C.
        - done: rerun 2017-07-03 -- now shows, appropriately, none

- create new metadata objects
    - TODO: update to use new-style BDR API params (eg. 'additional_rights' has to be in the 'rights' parameter, not at the top level)
    - tasks/metadata.run_enqueue_create_metadata_only_jobs()

    - to run (from `bell_code` directory)...

            >>> from tasks import metadata
            >>> metadata.run_enqueue_create_metadata_only_jobs()

    - to confirm that object is in fedora:
            https://api_search/?q=mods_id_bell_accession_number_ssim:the%20accession_number

    - after creates, confirm a re-run of `foundation/acc_num_to_pid.py` results in zero non-pid matches.
        - note that this re-run will update, as it should, `e__accession_number_to_pid_dict.json` -- the dict of accession-numbers-to-pids.
    - status: done 2017-07-03
        - also reran prep scripts to confirm that all accession-numbers have bdr pids

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

- delete old bell items from fedora
    - steps:
        - make list of bdr pids to delete.
            - NOTE: do the deletion accession number check as early as possible
            - NOTE: check whether an object to delete has an image - just update the metadata?
            - `from tasks import cleanup; cleanup.run_make_bdr_pids_to_delete()`
            - end result: `m__bdr_delete_pids.json`
        - manually verify -- with J.C. -- objects to delete, if they have images.
            - explanation
                - if an accession number changes, then we would see that as a new object
                - then the old object (with the old accession number) would need to be deleted
                - if that object to be deleted has an image, then we might lose the image, because she wouldn't send us the image if it wasn't updated.
                - so, any items to delete with an image - check with her.
            - status: 2017-Aug-10, J.C. said OK to delete them all.
        - manually run deletion.
            - `from tasks import cleanup; cleanup.run_delete_single_pid_from_bdr( pid )`
            - end result: `n__bdr_entries_deleted_tracker.json`
            - status: 2017-Aug-11: done
            - TODO: script a check against the API to make sure we get a 404 (after cache expires).

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

### Some solr-pub calls for reference...

- [get list of pids](https://library.brown.edu/search/solr_pub/bell/?q=*:*&fl=pid&wt=json&indent=2&start=0&rows=10000)
- [get list of accession-numbers](https://library.brown.edu/search/solr_pub/bell/?q=*:*&fl=accession_number_original&wt=json&indent=2&start=0&rows=10000)
- [search accession-number](https://library.brown.edu/search/solr_pub/bell/?q=PH%201998.24&wt=json&indent=2)
- [search pid](https://library.brown.edu/search/solr_pub/bell/?q=%22bdr:299010%22&wt=json&indent=2)

---

### TODOs...

- switch paths in data/ to explicit relative paths, instead of settings
- consider validating mods before sending it to api (tho api does validate it )
- investigate indexing of metadata-only objects; specifically, object_type is 'undetermined' instead of 'CommonMetadata'
- look into updating BDR metadata if Bell metadata has changed
- Normalization
    - check all images have MASTER & JP2 datastreams (not TIFF, ...)
    - check MASTER & JP2 headers: should have a filename, content type for JP2s, ... (HEAD request works for this)
- review one_off scripts and get rid of least-necessary scripts
- get rid of any utils/ files that we haven't used

---

---
