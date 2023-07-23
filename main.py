import pandas as pd
import streamlit as st

from elasticsearch_connection import ElasticsearchConnection

st.set_page_config(page_title="Experimental ES Connection")

st.title("Elasticsearch Experimental Connection")

with st.echo():
    conn = st.experimental_connection("elasticsearch", type=ElasticsearchConnection)
    conn


with st.echo():
    data = conn.query(index="kibana_sample_data_flights", query={"match_all": {}}, size=10_000)
    data

with st.echo():
    df = pd.json_normalize(data=data["hits"]["hits"])
    st.dataframe(df)
