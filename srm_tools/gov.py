import time

import requests

from srm_tools.logger import logger


def get_gov_api(url, skip):
    # Note: The gov API is buggy or just weird. It looks like you can set a high limit of items,
    # but the most that get returned in any payload is 50.
    # If you pass, for example, limit 1000, the stats on the response object will say:
    # {... "total":90,"start_index":0,"page_size":1000,"current_page":1,"pages":1...}
    # but this is wrong - you only have 50 items, and it turns out you need to iterate by using
    # skip. And then, the interaction between limit and skip is weird to me.
    # you need to set a limit higher than the results we will retreive, but whatever you put in limit
    # is used, minus start_index, to declare page_size, which is wrong ......
    # we are going to batch in 50s which seems to be the upper limit for a result set.
    # also, we get blocked sometimes, but it is not consistent - the retry logic is for that.
    timeout = 5
    wait_when_blocked = 180
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"
    }
    retries = 5
    while retries:
        try:
            # when we get blocked, we still get a success status code
            # we only know we have a bad payload by parsing it.
            # so we catch the exception on parsing as json.
            response = requests.get(
                url,
                params={'limit': 1000, 'skip': skip},
                timeout=timeout,
                headers=headers,
            ).json()
            total, results = response['total'], response['results']
            retries = 0
        except:
            total, results = 0, tuple()
            retries = retries - 1
            time.sleep(wait_when_blocked)

    if total == 0:
        msg = 'Gov API access blocked. Cannot complete data extraction.'
        logger.info(msg)
        raise Exception(msg)
    return total, results
