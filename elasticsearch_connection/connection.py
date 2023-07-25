from typing import Any, Dict, List, Optional

import eland as ed
import elastic_transport
import elasticsearch
import pandas as pd
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

    def query(
        self,
        *,
        index: str,
        columns: Optional[List[str]] = None,
        query: Optional[Dict[str, Any]] = None,
        ttl: int = 3600,
    ) -> pd.DataFrame:
        """
        Queries an Elasticsearch index and returns the results as a Pandas DataFrame.

        Parameters
        ----------
        index: str
            Elasticsearch index to query.

        columns: List[str]
            Fields in the Elasticsearch index to select. Defaults to all fields.

        query: Dict[Any]
            Elasticsearch query to filter data in the index. Defaults to None, all data from the index gets fetched.

        ttl: int
            How long to keep data cached in seconds. Defaults to 3600s (1h)

        Returns
        -------
        df: pandas.DataFrame
        """

        @cache_data(ttl=ttl)
        def _query(index, query) -> pd.DataFrame:
            es_df = ed.DataFrame(self.client, es_index_pattern=index, columns=columns)

            if query:
                es_df = es_df.es_query(query=query)

            return es_df.to_pandas()

        return _query(index=index, query=query)

    def insert(self, *, index: str, doc: Dict[str, Any], **kwargs) -> elastic_transport.ObjectApiResponse:
        """
        Inserts a document into an Elasticsearch index.

        Parameters
        ----------
        index: str
            Elasticsearch index.

        doc: Dict[Any]
            Document to insert into Elasticsearch index.

        kwargs:
            Keyword arguments to pass into `elasticsearch.Elasticsearch.index` function https://elasticsearch-py.readthedocs.io/en/v8.8.2/api.html#elasticsearch.Elasticsearch.index.

        Returns
        -------
        elastic_transport.ObjectApiResponse: Response object with response details.
        """
        return self.client.index(index=index, document=doc, **kwargs)
