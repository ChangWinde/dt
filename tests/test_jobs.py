import re

from dt.jobs import new_job_id, sanitize_name
from dt.render import compress_indices


def test_sanitize():
    assert sanitize_name("exp 42/lr=3e-4") == "exp-42-lr-3e-4"
    assert sanitize_name("///") == "job"
    assert sanitize_name("ok_name-1") == "ok_name-1"


def test_job_id_shape():
    jid = new_job_id("exp 42")
    assert re.fullmatch(r"\d{8}-\d{4}_exp-42_[0-9a-f]{4}", jid)


def test_compress_indices():
    assert compress_indices([]) == "-"
    assert compress_indices([0, 1, 2, 3, 5, 7]) == "0-3 5 7"
    assert compress_indices([4]) == "4"
    assert compress_indices([1, 2]) == "1-2"
