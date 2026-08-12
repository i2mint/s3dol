"""Tier-1 tests for the value refs and write domain (ADR-0005)."""

import io
import pickle

import pytest

from s3dol.values import (
    Chunks,
    Filepath,
    Streamable,
    as_fileobj,
    is_bytes_like,
    reject_str_value,
)


def test_str_is_rejected_with_the_two_option_message():
    with pytest.raises(TypeError) as info:
        reject_str_value("/tmp/big.mp4")
    message = str(info.value)
    assert "encode()" in message and "Filepath" in message


def test_bare_pathlike_is_rejected_pointing_at_filepath():
    from pathlib import Path

    with pytest.raises(TypeError, match="Filepath"):
        as_fileobj(Path("/tmp/x"))


def test_alien_type_names_the_extension_point():
    with pytest.raises(TypeError, match="register"):
        as_fileobj(12345)


@pytest.mark.parametrize(
    "value", [b"bytes", bytearray(b"ba"), memoryview(b"mv")]
)
def test_bytes_like_members(value):
    assert is_bytes_like(value)
    assert as_fileobj(value).read() == bytes(value)


def test_open_binary_files_dispatch_via_io_iobase():
    # The trap: @register(typing.BinaryIO) is accepted and never fires
    # (io.BytesIO is not in its MRO — ADR-0005 §1). io.IOBase is the base
    # that actually covers real file objects.
    assert as_fileobj(io.BytesIO(b"data")).read() == b"data"


def test_filepath_normalises_pathlike_and_reads(tmp_path):
    target = tmp_path / "payload.bin"
    target.write_bytes(b"CONTENT")
    ref = Filepath(target)  # a Path is fine; normalised to str
    assert isinstance(ref.path, str)
    with as_fileobj(ref) as fileobj:
        assert fileobj.read() == b"CONTENT"
    # re-readable (N1-total): a second consumption works
    with as_fileobj(ref) as fileobj:
        assert fileobj.read() == b"CONTENT"
    assert pickle.loads(pickle.dumps(ref)) == ref


def test_chunks_concatenates_and_is_one_shot():
    ref = Chunks(iter([b"ab", b"cd", b"e"]))
    assert as_fileobj(ref).read() == b"abcde"
    with pytest.raises(ValueError, match="one-shot"):
        as_fileobj(ref)  # the documented error, not a silent b'' write


def test_streamable_reopens_per_write():
    ref = Streamable(lambda: iter([b"xy", b"z"]))
    assert as_fileobj(ref).read() == b"xyz"
    assert as_fileobj(ref).read() == b"xyz"  # fresh stream each time


def test_iterable_fileobj_partial_reads():
    fileobj = as_fileobj(Chunks(iter([b"abc", b"defg"])))
    assert fileobj.read(2) == b"ab"
    assert fileobj.read(3) == b"cde"
    assert fileobj.read() == b"fg"


def test_write_strategies_are_picklable_values():
    from s3dol.reads import BytesReads, StreamReads
    from s3dol.writes import SimpleWrites, TransferWrites

    for strategy in (TransferWrites(), SimpleWrites(), BytesReads(), StreamReads()):
        assert pickle.loads(pickle.dumps(strategy)) == strategy
