import pytest


@pytest.mark.skipif(
    pytest.importorskip("pyspark", reason="pyspark is not installed") is None,
    reason="pyspark is not installed",
)
def test_load_pyspark():
    import pyspark  # noqa: F401

    assert True
