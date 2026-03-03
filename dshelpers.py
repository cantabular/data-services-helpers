#!/usr/bin/env python


import datetime
import inspect
import logging
import time
from collections import OrderedDict
from contextlib import contextmanager
from io import BytesIO
from urllib.parse import urlparse

import requests
import requests_cache
import scraperwiki
from requests.structures import CaseInsensitiveDict

L = logging.getLogger("sw.ds.helpers")

_MAX_RETRIES = 5
_TIMEOUT = 60
_HIT_PERIOD = 2  # seconds between requests to the same domain

_RATE_LIMIT_ENABLED = True  # Used inside rate_limit_disabled() context manager
_LAST_TOUCH = {}  # domain name => datetime

_USER_AGENT = "ScraperWiki Limited (bot@scraperwiki.com)"

__all__ = [
    "install_cache",
    "download_url",
    "request_url",
    "rate_limit_disabled",
    "batch_processor",
]


@contextmanager
def batch_processor(callback, batch_size=2000):
    processor = BatchProcessor(callback, batch_size)
    try:
        yield processor
    finally:
        processor.flush()


class BatchProcessor:
    """
    You can push items here and they'll be stored in a queue. When batch_size
    items have been pushed, the given callback is called with the list of
    items and the queue is cleared.

    Note: You must call flush() to process the final items: this is not done
    automatically (yet)
    """

    def __init__(self, callback, batch_size):
        self.queue = []
        self.callback = callback
        self.batch_size = batch_size

    def push(self, row):
        self.queue.append(row)
        if len(self.queue) >= self.batch_size:
            self.flush()

    def flush(self):
        self.callback(self.queue)
        self.queue = []


def _get_most_recent_record(table_name, column):
    result = scraperwiki.sql.select(
        f"MAX({column}) AS most_recent FROM {table_name} LIMIT 1"
    )
    return result[0]["most_recent"]


def install_cache(expire_after=12 * 3600, cache_post=False):
    """
    Patches the requests library with requests_cache.
    """
    allowable_methods = ["GET"]
    if cache_post:
        allowable_methods.append("POST")
    requests_cache.install_cache(
        expire_after=expire_after, allowable_methods=allowable_methods
    )


def download_url(url, back_off=True, **kwargs):
    """
    Get the content of a URL and return a file-like object.
    back_off=True provides retry
    """
    if back_off:
        return _download_with_backoff(url, as_file=True, **kwargs)
    else:
        return _download_without_backoff(url, as_file=True, **kwargs)


def request_url(url, back_off=True, **kwargs):
    if back_off:
        return _download_with_backoff(url, as_file=False, **kwargs)
    else:
        return _download_without_backoff(url, as_file=False, **kwargs)


@contextmanager
def rate_limit_disabled():
    global _RATE_LIMIT_ENABLED
    _RATE_LIMIT_ENABLED = False
    try:
        yield
    finally:
        _RATE_LIMIT_ENABLED = True


def _download_without_backoff(url, as_file=True, method="GET", **kwargs):
    """
    Get the content of a URL and return a file-like object.
    """
    # Make requests consistently hashable for caching.
    # 'headers' is handled by requests itself.
    # 'cookies' and 'proxies' contributes to headers.
    # 'files' and 'json' contribute to data.
    for k in ["data", "params"]:
        if k in kwargs and isinstance(kwargs[k], dict):
            kwargs[k] = OrderedDict(sorted(kwargs[k].items()))

    kwargs_copy = dict(kwargs)
    if not _is_url_in_cache(method, url, **kwargs):
        now = datetime.datetime.now()
        _rate_limit_for_url(url, now)
        _rate_limit_touch_url(url, now)

    L.info(f"Download {url}")
    if "timeout" not in kwargs_copy:
        kwargs_copy["timeout"] = _TIMEOUT
    if "headers" in kwargs_copy:
        head_dict = CaseInsensitiveDict(kwargs_copy["headers"])
        if "user-agent" not in head_dict:
            head_dict["user-agent"] = _USER_AGENT
        kwargs_copy["headers"] = head_dict
    else:
        kwargs_copy["headers"] = CaseInsensitiveDict({"user-agent": _USER_AGENT})

    response = requests.request(method, url, **kwargs_copy)

    if logging.getLogger().isEnabledFor(logging.DEBUG):
        # This can be slow on large responses, due to chardet.
        L.debug(f'"{response.text}"')

    response.raise_for_status()

    if as_file:
        return BytesIO(response.content)
    else:
        return response


def _download_with_backoff(url, **kwargs):
    next_delay = 10

    for n in range(0, _MAX_RETRIES):
        try:
            return _download_without_backoff(url, **kwargs)
        except (requests.exceptions.RequestException, TimeoutError) as e:
            L.exception(e)
            L.info(f"Retrying in {next_delay} seconds: {url}")
            time.sleep(next_delay)
            next_delay *= 2

    raise RuntimeError(f"Max retries exceeded for {url}")


def _is_url_in_cache(*args, **kwargs):
    """Return True if request has been cached or False otherwise."""
    # Only include allowed arguments for a PreparedRequest.
    allowed_args = list(
        inspect.signature(requests.models.PreparedRequest.prepare).parameters
    )
    # self is in there as .prepare() is a method.
    allowed_args.remove("self")

    kwargs_cleaned = {}
    for key, value in dict(kwargs).items():
        if key in allowed_args:
            kwargs_cleaned[key] = value

    prepared_request = _prepare(*args, **kwargs_cleaned)
    request_hash = _get_hash(prepared_request)
    try:
        return requests_cache.get_cache().contains(key=request_hash)
    except AttributeError as e:  # requests_cache not enabled
        if str(e) == "'NoneType' object has no attribute 'contains'":
            return False
        raise


def _get_hash(prepared_request):
    """Create requests_cache key from a prepared Request."""
    # TODO: This should use whatever requests_cache is monkeypatching into
    # requests. Not sure how to discover this. In practice, all backends
    # use BaseCache's implementation.
    return requests_cache.backends.base.BaseCache().create_key(prepared_request)


def _prepare(*args, **kwargs):
    """Return a prepared Request."""
    return requests.Request(*args, **kwargs).prepare()


def _rate_limit_for_url(url, now=datetime.datetime.now()):
    """ """
    if not _RATE_LIMIT_ENABLED:
        return
    domain = _get_domain(url)
    last_touch = _LAST_TOUCH.get(domain)

    if last_touch:
        delta = now - last_touch
        if delta < datetime.timedelta(seconds=_HIT_PERIOD):
            wait = _HIT_PERIOD - delta.total_seconds()
            L.debug(f"Rate limiter: sleeping {wait}s")
            time.sleep(wait)


def _rate_limit_touch_url(url, now=None):
    if now is None:
        now = datetime.datetime.now()
    domain = _get_domain(url)
    L.debug(f"Recording hit for domain {domain} at {now}")
    _LAST_TOUCH[domain] = now


def _get_domain(url):
    """
    _get_domain('http://foo.bar/baz/')
    u'foo.bar'
    """
    return urlparse(url).netloc
