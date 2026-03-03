data-services-helpers
=====================

A module containing classes and functions previously used by [The Sensible Code Company's](https://sensiblecode.io) Data Services team.

## Installation

For the current release:

```
pip install dshelpers
```

## Usage

### batch_processor

    with batch_processor(callback_function(), batch_size=2000) as b:
        # loop to make rows here
        b.push(row)

Here, `push` on the `batch_processor` queues items in a list. When the
context manager is exited, calls the `callback_function` with the list of
items.

Often used to bundle multiple calls to `scraperwiki.sqlite.save` when saving
data to a database.

### install_cache

`install_cache(expire_after=12 * 3600, cache_post=False)`

For installing a `requests_cache`; requires the
[`requests-cache`](https://requests-cache.readthedocs.org/) package.

`expire_after` is the cache expiry time in seconds.

`cache_post` defines if HTTP POST requests should be cached as well.

### download_url

`download_url(url, back_off=True, **kwargs)`

Retrieve the content of `url`, by default using `requests.request('GET', url)`,
and return a file-like object. If `back_off=True`, then this will retry (with
backoff) on failure; otherwise, only one attempt is made. Returns the
`response.content` as a StringIO object.

The `**kwargs` can be arguments that
[`requests`](http://docs.python-requests.org/en/latest/) recognises, e.g.
`method` or `headers`.

### request_url

`request_url(url, back_off=True, **kwargs)`

As `download_url`, but returns the `response` object.

## Tests

Run with `pytest`.
