from datetime import datetime

import pandas as pd
import streamlit as st

from elasticsearch_connection import ElasticsearchConnection

st.set_page_config(page_title="Experimental ES Connection")

st.title("Elasticsearch Experimental Connection")

with st.echo():
    conn = st.experimental_connection("elasticsearch", type=ElasticsearchConnection)
    conn


index = "kibana_sample_data_flights"


with st.expander(label="Reading Data", expanded=True):
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

with st.expander("Writing Data"):
    with st.echo():
        doc = {
            "AvgTicketPrice": 12345.00,
            "Cancelled": True,
            "Carrier": "Streamlit Airways",
            "Dest": "Streamlit Town",
            "DestAirportID": "STT",
            "DestCityName": "Toronto",
            "DestCountry": "CA",
            "DestLocation": {"lat": "-33.94609833", "lon": "151.177002"},
            "DestRegion": "TO-06",
            "DestWeather": "Sunny",
            "DistanceKilometers": 12_123_123,
            "DistanceMiles": 12_123,
            "FlightDelay": False,
            "FlightDelayMin": 0,
            "FlightDelayType": "No Delay",
            "FlightNum": "6T06TOR",
            "FlightTimeHour": 1.2,
            "FlightTimeMin": 75,
            "Origin": "Streamlit",
            "OriginAirportID": "STR",
            "OriginCityName": "Streamlit City",
            "OriginCountry": "ST",
            "OriginLocation": {"lat": "-33.94609833", "lon": "151.177002"},
            "OriginRegion": "ST-TO",
            "OriginWeather": "Sunny",
            "dayOfWeek": 6,
            "timestamp": datetime.now(),
        }

    with st.echo():
        resp = conn.insert(index=index, doc=doc)
        resp
