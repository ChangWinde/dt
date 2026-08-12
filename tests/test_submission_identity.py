"""Regression tests for laptop submission stdout parsing (C1 audit fix)."""

from dt.cli import _captured_submission_identity


def test_accepts_current_16_hex_job_id_line():
    job_id = "20260812-0117_exp42_a1b2c3d4e5f60718"
    parsed, payload = _captured_submission_identity(
        f"launcher noise\n{job_id}\n", json_=False
    )
    assert parsed == job_id
    assert payload is None


def test_accepts_legacy_4_hex_job_id_line():
    job_id = "20260720-0900_smoke_ab12"
    parsed, _payload = _captured_submission_identity(job_id + "\n", json_=False)
    assert parsed == job_id


def test_rejects_non_job_id_tail_line():
    parsed, payload = _captured_submission_identity("done\nnot a job id\n", json_=False)
    assert parsed is None
    assert payload is None
