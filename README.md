# Streamlit Elasticsearch Connection

Connect to a Elasticsearch cluster from your Streamlit app. Powered by `st.experimental_connection()` and [Elasticsearch](https://github.com/elastic/elasticsearch-py).

## Install

```
pip install streamlit git+https://github.com/sidhulabs/streamlit-elasticsearch-connection  
```


## Example

Setup your `.streamlit/secrets.toml`. You must specify one of the following:
- hosts
- cloud_id

For auth methods, you must either use:
- a username or password 
- api id and api key.

```toml
# .streamlit/secrets.toml

[connections.elasticsearch]
cloud_id = ...
# hosts = ...

user = ...
password = ...

# api_id = ...
# api_key = ...
```

Then in your streamlit script:

```python
# main.py

import streamlit as st
from elasticsearch_connection import ElasticsearchConnection

conn = st.experimental_connection("elasticsearch", type=ElasticsearchConnection)

df = conn.query("kibana_sample_data_flights")
st.dataframe(data)
```

## Q&A

> Does this work for hosted or cloud clusters?

It works for both.