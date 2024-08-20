# ---
# jupyter:
#   jupytext:
#     formats: py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.16.4
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # ETL to pull data from bitcoin monitor

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## Import required libraries

# %%
from datetime import datetime
import logging
import sys
from typing import Any, Dict, List, Optional

# %%
import psycopg2.extras as p
import requests


# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## Extract data

# %%
def get_exchange_data(url):
    try:
        r = requests.get(url)
    except requests.ConnectionError as ce:
        logging.error(f"There was an error with the request, {ce}")
        sys.exit(1)
    return r.json().get('data', [])


# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## Transform data

# %%
def flatten_exchange_data(data):
    return data.get("data", [])


# %%
def get_utc_from_unix_time(unix_ts, second = 1000):
    return datetime.fromtimestamp(int(unix_ts) / second) if unix_ts else None



# %%
def clean_exchange_data(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # use Decimal for percentTotalVolume, volumeUsd
    # convert updated to datetime
    # if null volumeUsd drop rows
    processed_data = []
    for entry in data:
        if entry["volumeUsd"] is not None:
            entry["percentTotalVolume"] = entry["percentTotalVolume"]
            entry["updated"] = get_utc_from_unix_time(entry["updated"])
            entry["volumeUsd"] = entry["volumeUsd"]
            entry["tradingPairs"] = int(entry["tradingPairs"])
            processed_data.append(entry)
    return processed_data


# %% [markdown] jp-MarkdownHeadingCollapsed=true
# ## Load data

# %%
# No load, since we are presenting this as an UI

# %% [markdown]
# ## ETL Run

# %%
url = "https://api.coincap.io/v2/exchanges" # url to get data from

# %%
# Data for the exchanges (your Bitcoin exchange data)
exchange_data = clean_exchange_data(get_exchange_data(url))

# %% [markdown]
# ## Web app

# %% [markdown]
# Start a simple FastHTML serve to display a plotly chart of the pulled bitcoin exchange data

# %% editable=true slideshow={"slide_type": ""}
import json
from fasthtml.common import * 

# Initialize the FastHTML app with Plotly JS
app, rt = fast_app(hdrs=(Script(src="https://cdn.plot.ly/plotly-2.32.0.min.js"),))

# Prepare data for Plotly
exchange_names = [exchange['name'] for exchange in exchange_data]
volume_usd = [exchange['volumeUsd'] for exchange in exchange_data]

plotly_data = json.dumps({
    "data": [{
        "x": exchange_names,
        "y": volume_usd,
        "type": "bar",
        "name": "Volume in USD"
    }],
    "layout": {
        "title": "Bitcoin Exchange Trading Volume",
        "xaxis": {"title": "Exchange"},
        "yaxis": {"title": "Volume in USD"}
    }
})

@rt("/")
def get():
    return Titled("Chart Demo", Div(id="myDiv"),
                  Script(f"var data = {plotly_data}; Plotly.newPlot('myDiv', data.data, data.layout);"))

serve()


# %%
# ! python main.py

# %%
