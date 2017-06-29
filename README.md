### overview ###

code related to ingesting bell-gallery images into the bdr.


### normal ingest flow ###

- get metadata
    - metadata should be for _all_ the records
    - from having all the metadata, the code can determine what image-&-metadata additions to make, what metadata-only additions to make, and make corrections to existing bdr metadata
    - status:
        - done; added to git 2017-06-26
            - `a__all_data_raw.xml`
            - `b__all_data_formatted.xml`

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
        - 67 image-directory filenames were not found in the metadata:

                [start]
                "Bing PH_2015.11.1.tif",
                "Bing PH_2015.11.2.tif",
                "Bing PH_2015.11.4.tif",
                "Bing PH_2015.11.5.tif",
                "Bosworth PH_2007.46.A,B.tif",
                "Freed PH_2015.11.10.tif",
                "Freed PH_2015.11.11.tif",
                "Freed PH_2015.11.12.tif",
                "Freed PH_2015.11.13.tif",
                "Freed PH_2015.11.14.tif",
                "Freed PH_2015.11.15.tif",
                "Freed PH_2015.11.16.tif",
                "Freed PH_2015.11.17.tif",
                "Freed PH_2015.11.18.tif",
                "Freed PH_2015.11.19.tif",
                "Freed PH_2015.11.20.tif",
                "Freed PH_2015.11.21.tif",
                "Freed PH_2015.11.22.tif",
                "Freed PH_2015.11.23.tif",
                "Freed PH_2015.11.24.tif",
                "Freed PH_2015.11.25.tif",
                "Freed PH_2015.11.26.tif",
                "Freed PH_2015.11.6.tif",
                "Freed PH_2015.11.7.tif",
                "Freed PH_2015.11.8.tif",
                "Freed PH_2015.11.9.tif",
                "Leibling PH_2001.1.10.tif",
                "Leibling PH_2001.1.2.tif",
                "Leibling PH_2001.1.3.tif",
                "Leibling PH_2001.1.4.tif",
                "Leibling PH_2001.1.5.tif",
                "Leibling PH_2001.1.6.tif",
                "Leibling PH_2001.1.7.tif",
                "Leibling PH_2001.1.8.tif",
                "Leibling PH_2001.1.9.tif",
                "Leilbing PH_2001.1.1.tif",
                "Meiselas PH_2006.12.1.tif",
                "Meiselas PH_2006.12.10.tif",
                "Meiselas PH_2006.12.11.tif",
                "Meiselas PH_2006.12.12.tif",
                "Meiselas PH_2006.12.13.tif",
                "Meiselas PH_2006.12.14.tif",
                "Meiselas PH_2006.12.15.tif",
                "Meiselas PH_2006.12.16.tif",
                "Meiselas PH_2006.12.17.tif",
                "Meiselas PH_2006.12.18.tif",
                "Meiselas PH_2006.12.19.tif",
                "Meiselas PH_2006.12.2.tif",
                "Meiselas PH_2006.12.20.tif",
                "Meiselas PH_2006.12.3.tif",
                "Meiselas PH_2006.12.4.tif",
                "Meiselas PH_2006.12.5.tif",
                "Meiselas PH_2006.12.6.tif",
                "Meiselas PH_2006.12.7.tif",
                "Meiselas PH_2006.12.8.tif",
                "Meiselas PH_2006.12.9.tif",
                "Meyerowitz PH_2015.11.49.tif",
                "Meyerowitz PH_2015.11.50.tif",
                "Meyerowitz PH_2015.11.51.tif",
                "Meyerowitz PH_2015.11.52.tif",
                "Meyerowitz PH_2015.11.53.tif",
                "Meyerowitz PH_2015.11.54.tif",
                "Meyerowitz PH_2015.11.55.tif",
                "Meyerowitz PH_2015.11.56.tif",
                "Meyerowitz PH_2015.11.57.tif",
                "Newman PH_2013.7.3.tif",
                "Newman PH_2013.7.4.tif"
                [end]

    - status: in-process.

- match metadata accession-numbers to pid #1
    - foundation/acc_num_to_pid.py
    - end result: `e__accession_number_to_pid_dict.json` file containing a dict of accession-numbers-to-pids.
    - accession-numbers without pids imply creation of a new metadata-only object (which may gain an associated image later)
    - accession-numbers with pids imply checking to see if fedora metadata needs to be updated
    - status: done 2017-06-28; shows 97 accession-numbers without pids

- make metadata-only list
    - tasks/metadata.run_metadata_only_lister()
    - to run (from `bell_code` directory)...

            >>> from tasks import metadata
            >>> metadata.run_metadata_only_lister()

    - end result: `f__metadata_only_accession_numbers.json`
    - status: done 2017-06-29

- create new metadata objects
    - tasks/metadata.run_enqueue_create_metadata_only_jobs()

    - to run (from `bell_code` directory)...

            >>> from tasks import metadata
            >>> metadata.run_enqueue_create_metadata_only_jobs()

    - after creates, confirm a re-run of `foundation/acc_num_to_pid.py` results in zero non-pid matches.
        - note that this re-run will update, as it should, `e__accession_number_to_pid_dict.json` -- the dict of accession-numbers-to-pids.
    - status: done 2016-05-06
    - reran status of `foundation/acc_num_to_pid.py`: done 2016-05-09 -- now, appropriately, shows no accession-numbers without bdr pids.
    - done again 2016-05-19 after newly-found additions
    - reran status of `foundation/acc_num_to_pid.py`: done 2016-05-19 -- now, appropriately, shows no accession-numbers without bdr pids.

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
    - status: done, 2016-05-19.


- make list of images to process
    - tasks/images.ImageLister.make_image_lists()

    - to run (from `bell_code` directory)...

            >>> from tasks import images
            >>> images.run_make_image_lists()

    - produces a file containing both a list of images to add, and a list of images to update
    - end result: `g_images_to_process.json`
    - status: done: 2016-05-09
    - status: redone 2016-05-19

---

temp unusual step...

- cull the lists of images-to-add and images-to-update by removing the images that were processed (those in `g__images_to_process_temp_archive.json`).
    - end result: `data/g_images_to_process_CULLED.json`
    - status: cull-list completed 2016-05-19

---

- add new images
    - tasks/images.run_enqueue_add_image_jobs()
    - to run (from `bell_code` directory)...

            >>> from tasks import images
            >>> images.run_enqueue_add_image_jobs()

    - for adding individual images, if necessary: tasks/images.run_add_image( filename_dct )
    - status:
        - done: 2016-05-12
        - extra tifs redone: 2016-05-23
        - jpg code fixed and redone: 2016-05-25


- add updated images
    - tasks/images.run_enqueue_add_image_jobs()
    - to run (from `bell_code` directory)...

            >>> from tasks import images
            >>> images.run_enqueue_update_image_jobs()

    - for adding individual images, if necessary: tasks/images.run_add_image( filename_dct )
    - status: done
        - images updated: 2016-05-16
        - old TIFF datastreams deleted: 2016-05-17

- update the custom-solr-index
    - prep list of pids from custom-index
        - tasks/indexer.run_make_pids_for_custom_index()
        - end result: `h__pids_from_custom_index_list.json`
        - status: done: 2016-05-26
    - prep pids-to-delete list
        - tasks/indexer.run_make_delete_pids_list()
        - end result: `i__custom_index_delete_pids.json`
        - review that list
        - status: done 2016-05-26
    - prep pids-to-update list
        - tasks/indexer.run_make_update_pids_list()
        - adds pid to accession-number-to-data-dct data.
        - end result: `j__custom_index_update_data_list.json`
        - status: done, 2016-05-26
    - run updates
        - tasks/indexer.run_enqueue_index_jobs()
        - end result: `k__entries_updated_tracker.json`
        - status: done! 2016-05-26
    - run deletes
        - tasks/indexer.run_solr_deletions()
        - end result: `l__solr_pids_deleted_tracker.json`
        - 2016-05-26 note: not sure what this step was; there is no function 'indexer.run_delete_pids()'
            - I think this is a TODO. I spot-checked some of the entries in i__custom_index_delete_pids.json, and they're not in the [custom bell solr index](http://library.brown.edu/search/solr_pub/bell/?q=*:*&wt=json). So, this may have referred to planned code to delete old pids from **fedora**. For example, one of the entries in that delete_pids list is 'bdr:299542'. That [still appears in the bdr](https://repository.library.brown.edu/studio/item/bdr:299542/), but not the custom bell solr index. A search on that title shows that the new record, with [pid 'bdr:650784' is in the bdr](https://repository.library.brown.edu/studio/search/?utf8=✓&q=Untitled+%5BSaint+and+Child+and+Crucifixion+%5D), and that pid _is_, appropriately in the updated custom bell gallery solr index.
            - Note: usually when an image is added to a metadata-only record, a new pid is not created. New pids can be created when the bell 'accession-number' changes -- and a handful do regularly change with each ingestion, because of bell staff corrections.
        - 2016-Oct-20: in response to J.C. recent emails, realized indexer deletes not done.
        - status: done, 2016-Oct-25

- delete old bell items from fedora
    - 2016-Oct-20: in response to J.C. recent emails, should write code to delete non-source-data entries from fedora. TODO.
    - steps:
        - validate that the original-data count matches the custom-solr doc-count.
            - `from tasks import cleanup; cleanup.run_validate_solr_counts()`
            - status: done, 2016-Nov-01
        - validate that all original-data accession-numbers are in the bell custom solr-index.
            - `from tasks import cleanup; cleanup.run_validate_solr_accession_numbers()`
            - status: done, 2016-Nov-02
        - validate that all original-data pids are in the bell custom solr-index.
            - `from tasks import cleanup; cleanup.run_validate_solr_pids()`
            - status: done, 2016-Nov-02
        - make list of bdr pids to delete.
            - `from tasks import cleanup; cleanup.run_make_bdr_pids_to_delete()`
            - end result: `m__bdr_delete_pids.json`
            - status: done, 2016-Nov-03
        - run deletion.
            - `from tasks import cleanup; cleanup.run_delete_single_pid_from_bdr( pid )`
            - end result: `n__bdr_entries_deleted_tracker.json`
            - status: most done weeks ago, did last four from emailing J.C. 2016-Nov-29

- run bdr validation
    - validate that all original-data pids are in the bdr.
    - validate that all original-data accession-numbers are in the bdr.

- run solr validation
    - validate that all original-data pids are in the the custom solr index.
    - validate that all original-data accession-numbers are in the custom solr index.

- let Bell-J.C. & CIS-J.O. know when done
    - old status: done, 2016-05-26

- TODO update future flow...
    - run initial bdr validation
    - run bdr cleanup
    - run rest of bdr validation
    - update solr index
    - run solr validation

---

### feb 2017 J.C. followup ###

- J.C. question originally from jan 12, 2017

- reported issue...

    - [CIS webiste Schrager search](https://www.brown.edu/campus-life/arts/bell-gallery/collection?quick=schrager) contains 10 items with 4 duplicates

        - The 4 duplicate CIS images appear to have accession numbers that are not currently valid (perhaps they were in the past).
        - I'm guessing that the internal index that the CIS website uses did not remove the old entries when it added the new ones.

    - [BDR Schrager search](https://repository.library.brown.edu/studio/search/?search_field=&q=schrager+%22bell+gallery%22) contains 8 works, 6 by Schrager and 2 containing metadata for duplicate Schrager's and images from another artist Bill Jacobson

        - odd, internally I have the correct Jacobson metadata for the two Jacobson images -- I cannot see how incorrect metadata was applied
        - emailing J.C. about this and CIS issue 24 feb 2017.
        - I did pass on the correct info to the bell-custom-solr index!

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

- modernize logging
- re-architecting settings -- perhaps `prep_env_settings.sh`, `dev_env_settings.sh`, `prod_env_settings.sh`

---

---
