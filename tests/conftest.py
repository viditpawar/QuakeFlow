import pytest


@pytest.fixture
def topics():
    """Throwaway raw/validated/dlq topics for tests that run the real Flink job."""
    from tests import flink_support as support

    if not support.stack_is_up():
        pytest.skip("Kafka and Flink are not running")
    names = support.create_topics()
    yield names
    support.delete_topics(names)
