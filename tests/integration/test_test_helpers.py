"""The test helpers must clean up after themselves, or leaked topics pile up unnoticed.

Needs Kafka only: docker compose --profile core up -d
"""

import pytest
from confluent_kafka.admin import AdminClient

from tests import flink_support as support

pytestmark = pytest.mark.integration


def test_topics_created_for_a_test_are_really_deleted_by_the_helper():
    if not support.stack_is_up_kafka():
        pytest.skip("Kafka is not running")
    names = support.create_topics()
    admin = AdminClient({"bootstrap.servers": support.BOOTSTRAP})
    mine = {names["raw"], names["validated"], names["dlq"]}
    assert mine <= set(admin.list_topics(timeout=15).topics)

    support.delete_topics(names)

    assert not mine & set(admin.list_topics(timeout=15).topics), "the helper left its topics behind"
