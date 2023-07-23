import pytest

from elasticsearch_connection.connection import ElasticsearchConnection


def test_init():

    conn = ElasticsearchConnection("elasticsearch")

    # Throws an error if an issue exists.
    conn._raw_instance.info()
