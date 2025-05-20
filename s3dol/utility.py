"""
This module provides utility functions and custom exceptions for the `s3dol` package.

Classes:
- S3DolException: Base exception class for `s3dol` package.
- S3KeyError: Exception raised when a key error occurs.
- NoSuchKeyError: Exception raised when a key is not found.
- KeyNotValidError: Exception raised when a key is not valid.
- GetItemForKeyError: Exception raised when an error occurs while getting an item for a key.
- S3HttpError: Exception raised when an HTTP error occurs.
- S3Not200StatusCodeError: Exception raised when an HTTP status code other than 200 is returned.

Functions:
- raise_on_error: Function that raises an error.
- return_none_on_error: Function that returns None.
- return_empty_tuple_on_error: Function that returns an empty tuple.
- Resp.status_code: Function that returns the HTTP status code from a dictionary.
- Resp.contents: Function that returns the contents from a dictionary.
- Resp.key: Function that returns the key from a dictionary.
- Resp.common_prefixes: Function that returns the common prefixes from a dictionary.
- Resp.prefix: Function that returns the prefix from a dictionary.
- Resp.buckets: Function that returns the buckets from a dictionary.
- Resp.body: Function that returns the body from a dictionary.
- Resp.ascertain_status_code: Function that raises an error if the HTTP status code is not 200.
- Resp.ascertain_200_status_code: Function that raises an error if the HTTP status code is not 200 and the response is not a dictionary with a 'ResponseMetadata' key.
"""

""""""
from dol.paths import path_get, OnErrorType


class S3DolException(Exception): ...


class S3KeyError(KeyError): ...


class NoSuchKeyError(S3KeyError): ...


class KeyNotValidError(S3KeyError): ...


class GetItemForKeyError(S3KeyError): ...


class S3HttpError(RuntimeError): ...


class S3Not200StatusCodeError(S3HttpError): ...


raise_on_error: OnErrorType
return_none_on_error: OnErrorType
return_empty_tuple_on_error: OnErrorType


def raise_on_error(d: dict):
    raise


def return_none_on_error(d: dict):
    return None


def return_empty_tuple_on_error(d: dict):
    return ()


class Resp:
    @staticmethod
    def status_code(d, on_error: OnErrorType = raise_on_error):
        return path_get(d, ["ResponseMetadata", "HTTPStatusCode"], on_error)

    @staticmethod
    def contents(d, on_error: OnErrorType = return_empty_tuple_on_error):
        return path_get(d, ["Contents"], on_error)

    @staticmethod
    def key(d, on_error: OnErrorType = raise_on_error):
        return path_get(d, ["Key"], on_error)

    @staticmethod
    def common_prefixes(d, on_error: OnErrorType = return_empty_tuple_on_error):
        return path_get(d, ["CommonPrefixes"], on_error)

    @staticmethod
    def prefix(d, on_error: OnErrorType = raise_on_error):
        return path_get(d, ["Prefix"], on_error)

    @staticmethod
    def buckets(d, on_error: OnErrorType = return_empty_tuple_on_error):
        return path_get(d, ["Buckets"], on_error)

    @staticmethod
    def body(d, on_error: OnErrorType = return_none_on_error):
        return path_get(d, ["Body"], on_error)

    @staticmethod
    def ascertain_status_code(
        d,
        status_code=200,
        raise_error=S3HttpError,
        *error_args,
        **error_kwargs,
    ):
        if Resp.status_code(d) != status_code:
            raise raise_error(*error_args, **error_kwargs)

    @staticmethod
    def ascertain_200_status_code(d):
        status_code = Resp.status_code(d, return_none_on_error)
        if status_code != 200:
            if not isinstance(d, dict):
                raise S3Not200StatusCodeError(
                    "Yeah, that's not even a dict, so doubt it's even a response."
                    f"I'm expecting a response over here. Instead I got a {type(d)}"
                )
            elif "ResponseMetadata" in d:
                raise S3Not200StatusCodeError(
                    f"Status code was not 200. Was {d['ResponseMetadata']}. "
                    f"ResponseMetadata is {d['ResponseMetadata']}"
                )
            else:
                raise S3Not200StatusCodeError(
                    f"Status code was not 200. In fact, the response dict didn't even have a ResponseMetadata key"
                )
