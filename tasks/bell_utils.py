import os
import requests


TASKS_DIR = os.path.dirname(os.path.abspath(__file__))
BELL_CODE_DIR = os.path.dirname(TASKS_DIR)
DATA_DIR = os.path.join(BELL_CODE_DIR, 'data')
PROD_ITEM_API_URL = os.environ['PROD_ITEM_API_URL']


def get_item_api_data(pid):
    item_api_url = '%s/%s/' % (PROD_ITEM_API_URL, pid)
    r = requests.get(item_api_url)
    if r.ok:
        return r.json()
    else:
        msg = '%s - %s' % (r.status_code, r.text)
        raise Exception(msg)

