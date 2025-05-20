"""Test utils"""

import os

_kinds = {
    "BUCKET": LookupError,
    "ACCESS": LookupError,
    "SECRET": LookupError,
    "ENDPOINT": None,
}


def extract_s3_access_info(access_dict):
    return {
        "bucket_name": access_dict["bucket"],
        "aws_access_key_id": access_dict["access"],
        "aws_secret_access_key": access_dict["secret"],
        "endpoint_url": access_dict["endpoint"],
    }


def _s3_env_var_name(kind, perm="RO"):
    kind = kind.upper()
    perm = perm.upper()
    assert kind in _kinds, f"kind should be in {list(_kinds)}"
    assert perm in {"RW", "RO"}, "perm should be in {'RW', 'RO'}"
    return f"S3_TEST_{kind}_{perm}"


def get_s3_test_access_info_from_env_vars(perm=None):
    if perm is None:
        try:
            return get_s3_test_access_info_from_env_vars(perm="RO")
        except LookupError:
            return get_s3_test_access_info_from_env_vars(perm="RW")
    else:
        access_keys = dict()
        for kind in _kinds:
            k = _s3_env_var_name(kind, perm)
            if (v := os.environ.get(k, _kinds[kind])) is LookupError:
                raise LookupError(f"Couldn't find the environment variable: {k}")
            else:
                access_keys[kind.lower()] = v
        return extract_s3_access_info(access_keys)
