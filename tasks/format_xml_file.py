import os
import sys
from lxml import etree


RAW_DATA_PATH = os.path.join('data', 'a__all_data_raw.xml')
FORMATTED_DATA_PATH = os.path.join('data', 'b__all_data_formatted.xml')


def create_formatted_xml_file():
    with open(RAW_DATA_PATH, 'rb') as f:
        data = f.read()
    xml_doc = etree.fromstring(data)
    formatted_data = etree.tostring(xml_doc, pretty_print=True)
    with open(FORMATTED_DATA_PATH, 'wb') as f:
        f.write(formatted_data)


if __name__ == '__main__':
    create_formatted_xml_file()

