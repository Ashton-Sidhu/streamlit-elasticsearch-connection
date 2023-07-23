import pytest

from elasticsearch_connection.connection import ElasticsearchConnection


def test_init():

    conn = ElasticsearchConnection("elasticsearch")
    conn._raw_instance.info()
