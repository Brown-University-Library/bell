### overview ###

code related to ingesting bell-gallery images into the bdr.


### normal ingest flow ###

- get metadata
    - metadata should be for _all_ the records
    - from having all the metadata, the code can determine what image-&-metadata additions to make, what metadata-only additions to make, and make corrections to existing bdr metadata
    - status:
        - done 2016-03-28
        - metadata file from November only contained a few records; J.C. emailed for new file
        - git it, good added it to repo
            - `a__all_data_raw.xml`
            - `b__all_data_formatted.xml`
        - redone 2016-04-18 after J.C. metadata-imagefile matching
        - redone 2016-04-26 after J.C. metadata-imagefile matching

- get access to image-directory
    - check desktop access
        - status: done 2016-03-26
            - instructions to view images from desktop from March 17 worked
    - check dev `mount` access
        - status: done 2016-03-26

- convert raw filemaker-pro xml to json
    - foundation/acc_num_to_data.py
    - end result: `c__accession_number_to_data_dict.json`
    - status:
        - done 2016-03-27
        - redone 2016-04-18 after J.C. metadata-imagefile matching
        - redone 2016-04-26 after J.C. metadata-imagefile matching

- run script to get list of files in the images-to-ingest directory
    - utils/make_image_list.py
    - end result: `d__bell_images_listing.json`
    - status:
        - done 2016-04-11
        - redone 2016-04-18 after J.C. updated metadata and images
        - redone 2016-04-26 after J.C. updated metadata and images

- compare imagefile-filenames to metadata-filenames
    - utils/check_filenames_against_metadata.py
    - log output will list any image-files for which there's no metadata; work through any discrepancies w/J.C.
    - end result: updated `e__accession_number_to_data_dict.json`
    - work:
        - emailed J.C. list of non-matches 2016-04-11
        - 558 image-directory filenames were found in the metadata
        - 69 image-directory filenames were not found in the metadata:

                [start]
                'Abbott PH_1980.1533.tif',
                'Abbott PH_1982.1689.10.tif',
                'Abbott PH_1982.1697.1.tif',
                'Abbott PH_1982.1697.2.tif',
                'Abbott PH_1982.1697.7.tif',
                'Abbott PH_1982.1697.9.tif',
                'Abbott PH_1983.1822.tif',
                'Agam PR_1986.51.tif',
                'Albers DC_2011.3.a.tif',
                'Albers PR_0.160.tif',
                'Alechinsky P_1996.127.tif',
                'Andre PR_2012.3.43.tif',
                'Anonymous PH_1994.13.tif',
                'Anonymous PR_1981.1654.tif',
                'Anonymous PR_2001.8.11.tif',
                'Bergman PR_1986.59.tif',
                'Bosworth PH_2007.46.1.A,B.tif',
                'Bosworth PH_2007.46.2.A,B.tif',
                'Bosworth PH_2007.46.3.A-C.tif',
                'Bosworth PH_2007.46.4.A-C.tif',
                'Bosworth PH_2007.46.5.A-C.tif',
                'Bosworth PH_2007.46.6.A-C.tif',
                'Bosworth PH_2007.46.7.A-C.tif',
                'Bosworth PH_2007.46.8.A-C.tif',
                'Cohen PR_1980.1681.B.tif',
                'Coughlin PR_1986.56.tif',
                'Delaney PR_1986.47.tif',
                'Delaune PR_2001.8.8.tif',
                'Evans PH_2014.10.tif',
                'Evans PH_2014.11.tif',
                'Gibson PH_1987.7.7.tif',
                'Gibson PH_2011.4.2 copy.tif',
                'Gibson PH_2011.4.3 copy.tif',
                'Gibson PH_2012.1.1 copy.tif',
                'Gibson PH_2012.1.2 copy.tif',
                'Goodyear PR_2010.1.12 copy.tif',
                'Goodyear PR_2010.1.12.tif',
                'Goya PR_00.387 copy.tif',
                'Goya PR_00.389 copy.tif',
                'Goya PR_00.390 copy.tif',
                'Hepworth PR_1981.1679.I.tif',
                'Hepworth PR_1981.1679.J.tif',
                'Hockey PR_1980.1323.tif',
                'Jones PR_1981.1675.B.tif',
                'Kaneda PR_2012.3.34.tif',
                'Lemieux PR_2007.25.tif',
                'Luster PH_2005.3.tif',
                'Luster PH_2005.4.tif',
                'Luster PH_2005.5.tif',
                'Luster PH_2005.6.tif',
                'Manet PR_0.606 copy.tif',
                'Mense PR_0.742 copy.tif',
                'Miro PR_0.426.tif',
                'Miro PR_1981.9.A.tif',
                'Newman PR_2011.5.tif',
                'Olitski SC_1990.39.A-E.tif',
                'Paolozzi PR_1987.21.tif',
                'Picasso PR_0.381.tif',
                'Picasso PR_0.382.tif',
                'Rugendas D_2001.8.3.tif',
                'Starn PH_2014.14.A-D.tif',
                'Starn PH_2014.15.A,B.tif',
                'Sultan PH_2000.5.11.tif',
                'Ulreich PR_1986.25.tif',
                'Ulreich PR_1986.26.tif',
                'Ulreich PR_1986.27.tif',
                'Ulreich PR_1986.28.tif',
                'Van Ostade PR_1946.117.tif',
                'Zorn PR_1942.506.tif'
                [end]
        - per J.C.'s instructions, removed 2 unneeded files from image-folder:
            - Delaune PR_2001.8.8.tif
            - Goodyear PR_2010.1.12 copy.tif
        - ...and added 3 new files to the image-folder:
            - Abbott PH_1982.1698.10.tif
            - Cohen PR_1981.1681.B.tif
            - Goya PR_1942.506.tif
        - 2016-04-18, emailed J.C. new list of non-matches

                [start]
                'Abbott PH_1982.1689.10.tif',
                'Abbott PH_1982.1697.7.tif',
                'Andre PR_2012.3.43.tif',
                'Anonymous PH_1994.13.tif',
                'Baskin PR_2005.2 copy.tif',
                'Cohen PR_1980.1681.B.tif',
                'Gibson PH_1987.7.7.tif',
                'Gibson PH_2011.4.2.tif',
                'Gibson PH_2011.4.3.tif',
                'Gibson PH_2012.1.1.tif',
                'Gibson PH_2012.1.2.tif',
                'Goya PR_00.387.tif',
                'Goya PR_00.389 copy.tif',
                'Goya PR_00.390.tif',
                'Jones PR_1981.1675.B.tif',
                'Manet PR_0.606.tif',
                'Mense PR_0.742.tif',
                'Newman PR_2011.5.tif',
                'Olitski SC_1990.39.A-E.tif',
                'Starn PH_2014.14.A-D.tif',
                'Starn PH_2014.15.A,B.tif',
                'Ulreich PR_1986.25.tif',
                'Ulreich PR_1986.26.tif',
                'Ulreich PR_1986.27.tif',
                'Ulreich PR_1986.28.tif',
                'Zorn PR_1942.506.tif'
                [end]

        - 2016-04-26, emailed J.C. new list of non-matches, and in email listed the specific differences

                [list start]
                'Gibson PH_1987.7.7.tif'
                'Starn PH_2014.15.A,B.tif'
                'Ulreich PR_1986.25.tif'
                'Ulreich PR_1986.26.tif'
                'Ulreich PR_1986.27.tif'
                'Ulreich PR_1986.28.tif'
                [list end]

    - status: in-process

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
