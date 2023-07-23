from typing import Any, Dict

import elasticsearch
from streamlit.connections import ExperimentalBaseConnection
from streamlit.runtime.caching import cache_data


class ElasticsearchConnection(ExperimentalBaseConnection[elasticsearch.Elasticsearch]):
    """st.experimental_connection implementation for Elasticsearch"""

    @property
    def client(self) -> elasticsearch.Elasticsearch:
        return self._raw_instance

    def _connect(self) -> elasticsearch.Elasticsearch:

        secrets_dict = self._secrets.to_dict()

        cloud_id = secrets_dict.get("cloud_id", None)
        hosts = secrets_dict.get("hosts", None)
        user = secrets_dict.get("user", None)
        password = secrets_dict.get("password", None)
        api_id = secrets_dict.get("api_id", None)
        api_key = secrets_dict.get("api_key", None)

        if not (cloud_id or hosts):
            raise RuntimeError("You must either provide a cloud_id or hosts url to connect to.")

        if cloud_id:
            es_client = elasticsearch.Elasticsearch(cloud_id=cloud_id)
        else:
            es_client = elasticsearch.Elasticsearch(hosts=hosts)

        # Use api authentication first if provided.
        if api_id:
            return es_client.options(api_key=(api_id, api_key))

        if user:
            return es_client.options(basic_auth=(user, password))

        # For clusters with no auth, i.e local deployments
        return es_client

    def query(self, *, index: str, query: Dict[str, Any], ttl: int = 3600, **kwargs) -> Dict[str, Any]:
        @cache_data(ttl=ttl)
        def _query(index, query, **kwargs) -> Dict[str, Any]:
            return self.client.search(index=index, query=query, **kwargs).body

        return _query(index=index, query=query, **kwargs)

    def index(self, *, index: str, doc: Dict[str, Any], **kwargs) -> None:
        self.client.index(index=index, document=doc, **kwargs)
