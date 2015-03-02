### overview ###

code related to ingesting bell-gallery images into the bdr.


### flow ###

- convert raw filemaker-pro xml to json

- run script to get list of files in the images-to-ingest directory

- compare imagefile-filenames to metadata-filenames and work through discrepancies w/J.C.

- run through metadata:
    - update existing metadata object if necessary
    - create new metadata objects if neccessary

- run through images:
    - update existing image object if necessary
        - update associated metadata object if necessary (i.e. master jpg becomes master tif)
    - create image object if necessary
        - update the previous metadata-only object

- run a final check to make sure the custom-solr-index and the updated fedora data match

- let Bell-J.C. & CIS-J.O. know when done

---
