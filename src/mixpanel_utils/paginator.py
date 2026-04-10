"""
MIT License

Copyright (c) 2016 Sean Coonce

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

Author: Sean Coonce - https://github.com/cooncesean
Github repo: https://github.com/cooncesean/mixpanel-query-py
"""

import math
import itertools
from multiprocessing.pool import ThreadPool

try:
    import ujson as json
except ImportError:
    import json


class ConcurrentPaginator(object):
    """
    Concurrently fetches all pages in a paginated collection.

    Currently, only the people API (`/api/2.0/engage`) supports pagination.
    This class is designed to support the people API's implementation of
    pagination.
    """

    def __init__(self, get_func, concurrency=20):
        """
        Initialize with a function that fetches a page of results.
        `concurrency` controls the number of threads used to fetch pages.

        Example:
            client = MixpanelQueryClient(...)
            ConcurrentPaginator(client.get_engage, concurrency=10)
        """
        self.get_func = get_func
        self.concurrency = concurrency

    def fetch_all(self, params=None):
        """
        Fetch all results from all pages, and return as a list.

        If params need to be sent with each request (in addition to the
        pagination) params, they may be passed in via the `params` kwarg.
        """
        params = params and params.copy() or {}

        first_page = self.get_func(params)
        results = first_page["results"]
        params["session_id"] = first_page["session_id"]

        start, end = self._remaining_page_range(first_page)
        fetcher = self._results_fetcher(params)
        return results + self._concurrent_flatmap(fetcher, list(range(start, end)))

    def fetch_all_to_file(self, file_handle, params=None, on_block=None, block_size=100000):
        """
        Fetch all results from all pages and write them as JSONL to a file handle.

        Optionally calls `on_block(block_number)` every `block_size` profiles written,
        and `on_block(None)` as a sentinel when all pages are done.

        Returns the total number of profiles written.
        """
        params = params and params.copy() or {}
        count = 0

        first_page = self.get_func(params)
        for result in first_page["results"]:
            file_handle.write(json.dumps(result) + "\n")
            count += 1
            if on_block and count % block_size == 0:
                file_handle.flush()
                on_block(count // block_size - 1)

        params["session_id"] = first_page["session_id"]
        start, end = self._remaining_page_range(first_page)

        fetcher = self._results_fetcher(params)
        pool = ThreadPool(processes=self.concurrency)
        for page_results in pool.imap_unordered(fetcher, range(start, end)):
            for result in page_results:
                file_handle.write(json.dumps(result) + "\n")
                count += 1
                if on_block and count % block_size == 0:
                    file_handle.flush()
                    on_block(count // block_size - 1)
        pool.close()
        pool.join()

        file_handle.flush()
        # Signal final partial block if any remaining profiles since last block signal
        if on_block:
            if count % block_size != 0:
                on_block(count // block_size)
            on_block(None)

        return count

    def _results_fetcher(self, params):
        def _fetcher_func(page):
            req_params = dict(list(params.items()) + [("page", page)])
            return self.get_func(req_params)["results"]

        return _fetcher_func

    def _concurrent_flatmap(self, func, iterable):
        pool = ThreadPool(processes=self.concurrency)
        res = list(itertools.chain(*pool.map(func, iterable)))
        pool.close()
        pool.join()
        return res

    def _remaining_page_range(self, response):
        num_pages = math.ceil(response["total"] / float(response["page_size"]))
        return response["page"] + 1, int(num_pages)
