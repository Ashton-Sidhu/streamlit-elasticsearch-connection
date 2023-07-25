import pytest

from elasticsearch_connection.connection import ElasticsearchConnection


def test_init():
    """Test if we can connect to ES cluster."""

    conn = ElasticsearchConnection("elasticsearch")

    # Throws an error if connection fails.
    conn._raw_instance.info()
