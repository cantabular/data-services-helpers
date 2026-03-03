import datetime
import socket
from unittest.mock import call, patch

import pytest
import requests
from requests.structures import CaseInsensitiveDict

from dshelpers import (
    _HIT_PERIOD,
    _LAST_TOUCH,
    _TIMEOUT,
    _USER_AGENT,
    _download_with_backoff,
    _download_without_backoff,
    _rate_limit_for_url,
    _rate_limit_touch_url,
    install_cache,
    rate_limit_disabled,
    request_url,
)


def test_rate_limit_touch_url_works():
    time = datetime.datetime(2010, 11, 1, 10, 15, 30)

    with patch.dict(_LAST_TOUCH, {}, clear=True):
        assert {} == _LAST_TOUCH
        _rate_limit_touch_url("http://foo.com/bar", now=time)
        assert {"foo.com": time} == _LAST_TOUCH


@patch("time.sleep")
def test_rate_limit_no_sleep_if_outside_period(mock_sleep):
    previous_time = datetime.datetime(2013, 10, 1, 10, 15, 30)

    with patch.dict(_LAST_TOUCH, {}, clear=True):
        _rate_limit_touch_url("http://foo.com/bar", now=previous_time)
        _rate_limit_for_url(
            "http://foo.com/bar",
            now=previous_time + datetime.timedelta(seconds=_HIT_PERIOD),
        )

    mock_sleep.assert_not_called()


@patch("time.sleep")
def test_rate_limit_sleeps_up_to_correct_period(mock_sleep):
    previous_time = datetime.datetime(2013, 10, 1, 10, 15, 30)

    with patch.dict(_LAST_TOUCH, {}, clear=True):
        _rate_limit_for_url("http://foo.com/bar", now=previous_time)

        mock_sleep.assert_not_called()

        _rate_limit_touch_url("http://foo.com/bar", now=previous_time)

        _rate_limit_for_url(
            "http://foo.com/bar",
            now=previous_time + datetime.timedelta(seconds=1, microseconds=500000),
        )

    mock_sleep.assert_called_once_with(_HIT_PERIOD - 1.5)


@patch("dshelpers.requests_cache.install_cache")
def test_set_cache_methods(mock_install_cache):
    install_cache(cache_post=True)
    mock_install_cache.assert_called_with(
        expire_after=43200, allowable_methods=["GET", "POST"]
    )


@patch("time.sleep")
@patch("dshelpers.requests.request")
def test_passes_headers_through(mock_request, mock_time_sleep):
    fake_response = requests.Response()
    fake_response.status_code = 200
    fake_response._content = b"Hello"
    mock_request.return_value = fake_response
    _download_with_backoff("http://fake_url.com", headers={"this": "included"})
    mock_request.assert_called_with(
        "GET",
        "http://fake_url.com",
        headers=CaseInsensitiveDict(
            {
                "this": "included",
                "user-agent": "ScraperWiki Limited (bot@scraperwiki.com)",
            }
        ),
        timeout=60,
    )


@patch("time.sleep")
@patch("dshelpers.requests.request")
def test_passes_method_through(mock_request, mock_time_sleep):
    fake_response = requests.Response()
    fake_response.status_code = 200
    fake_response._content = b"Hello"
    mock_request.return_value = fake_response
    _download_with_backoff("http://fake_url.com", method="POST")
    mock_request.assert_called_with(
        "POST",
        "http://fake_url.com",
        headers=CaseInsensitiveDict(
            {"user-agent": "ScraperWiki Limited (bot@scraperwiki.com)"}
        ),
        timeout=60,
    )


@patch("time.sleep")
@patch("dshelpers.requests.request")
def test_override_timeout(mock_request, mock_time_sleep):
    fake_response = requests.Response()
    fake_response.status_code = 200
    fake_response._content = b"Hello"
    mock_request.return_value = fake_response
    _download_with_backoff("http://fake_url.com", timeout=10)
    mock_request.assert_called_with(
        "GET",
        "http://fake_url.com",
        headers=CaseInsensitiveDict(
            {"user-agent": "ScraperWiki Limited (bot@scraperwiki.com)"}
        ),
        timeout=10,
    )


@patch("time.sleep")
@patch("dshelpers.requests.request")
def test_get_response_object_on_good_site(mock_request, mock_sleep):
    fake_response = requests.Response()
    fake_response.status_code = 200
    fake_response._content = b"Hello"
    mock_request.return_value = fake_response
    assert b"Hello" == request_url("http://fake_url.com").content


@patch("time.sleep")
@patch("dshelpers.requests.request")
def test_backoff_function_works_on_a_good_site(mock_request, mock_sleep):
    fake_response = requests.Response()
    fake_response.status_code = 200
    fake_response._content = b"Hello"
    mock_request.return_value = fake_response
    assert b"Hello" == _download_with_backoff("http://fake_url.com").read()


@patch("time.sleep")
@patch("dshelpers.requests.request")
def test_backoff_function_works_after_one_failure(mock_request, mock_sleep):
    def response_generator():
        bad_response = requests.Response()
        bad_response.status_code = 500

        good_response = requests.Response()
        good_response.status_code = 200
        good_response._content = b"Hello"

        yield bad_response
        yield bad_response
        yield good_response

    mock_request.side_effect = response_generator()

    with rate_limit_disabled():
        assert b"Hello" == _download_with_backoff("http://fake_url.com").read()

    assert [call(10), call(20)] == mock_sleep.call_args_list
    expected_call = call(
        "GET",
        "http://fake_url.com",
        timeout=_TIMEOUT,
        headers=CaseInsensitiveDict({"user-agent": _USER_AGENT}),
    )
    assert [expected_call, expected_call, expected_call] == mock_request.call_args_list


@patch("time.sleep")
@patch("dshelpers.requests.request")
def test_backoff_raises_on_five_failures(mock_request, mock_sleep):
    fake_response = requests.Response()
    fake_response.status_code = 500

    mock_request.return_value = fake_response

    with rate_limit_disabled():
        with pytest.raises(RuntimeError):
            _download_with_backoff("http://fake_url.com")

    assert [
        call(10),
        call(20),
        call(40),
        call(80),
        call(160),
    ] == mock_sleep.call_args_list


@patch("time.sleep")
@patch("dshelpers.requests.request")
def test_handle_socket_timeout(mock_request, mock_sleep):
    fake_response = requests.Response()
    fake_response.status_code = 200
    fake_response._content = b"Hello"

    mock_request.side_effect = [socket.timeout, fake_response]
    # socket.timeout used to cause an exception.
    _download_with_backoff("http://fake_url.com")


@patch("dshelpers.requests.request")
def test_download_url_sets_user_agent(mock_request):
    fake_response = requests.Response()
    fake_response.status_code = 200
    fake_response._content = b"Hello"
    mock_request.return_value = fake_response

    _download_without_backoff("http://invalid")

    expected_user_agent = "ScraperWiki Limited (bot@scraperwiki.com)"
    expected_call = call(
        "GET",
        "http://invalid",
        timeout=60,
        headers=CaseInsensitiveDict({"user-agent": expected_user_agent}),
    )

    assert [expected_call] == mock_request.call_args_list
