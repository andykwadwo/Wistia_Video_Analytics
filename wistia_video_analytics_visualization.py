from turtle import color

import streamlit as st
import pandas as pd
from PIL import Image
import awswrangler as wr
import numpy as np
import plotly.express as px


# copy data from S3 bucket to a pandas dataframe using awswrangler
# Define your S3 bucket and file key
bucket_name = "wistia-analytics-gold-data"
key1 = "fact-media-engagement"

# Construct the S3 URI
s3_uri_key1 = f"s3://{bucket_name}/{key1}"

df_fact = wr.s3.read_csv(s3_uri_key1)

visitor_count = df_fact.groupby('media_id')['visitor_id'].nunique().reset_index(name='unique_visitor_count')

st.title("Wistia Video Analytics Dashboard")
st.write("Unique Visitor Count by Media:")

st.bar_chart(visitor_count.set_index('media_id')['unique_visitor_count'], use_container_width=True)

