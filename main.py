import pandas as pd
import streamlit as st

from elasticsearch_connection import ElasticsearchConnection

st.set_page_config(page_title="Experimental ES Connection")

st.title("Elasticsearch Experimental Connection")

with st.echo():
    conn = st.experimental_connection("elasticsearch", type=ElasticsearchConnection)
    conn


index = "kibana_sample_data_flights"

with st.echo():
    df = conn.query(index=index)
    df

with st.echo():
    df = conn.query(
        index=index,
        columns=["AvgTicketPrice", "Carrier"],
        query={"bool": {"filter": [{"term": {"Carrier": "JetBeats"}}]}},
    )
    df
