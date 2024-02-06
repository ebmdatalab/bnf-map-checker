# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: all
#     notebook_metadata_filter: all,-language_info
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.3.3
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# # Checking accuracy of BNF code change maps

# An issue was raised by one of the clinical informatician team members:
#
# >_An interesting case from an ICB colleague who is trying to check whether there’s any prescribing of Reslizumab (in response to a Medicines Supply Notification)…
# OpenPrescribing says it has been prescribed, ePACT says there is no prescribing.
# When looking at the EPD and PCA data, I can’t see any entries for Reslizumab at all - even nationally. In both cases I’m comparing November data, although I’ve checked other time periods with the same result. So, how are we detecting prescribing of Reslizumab in OpenPrescribing? Worried we could be ‘hallucinating’ prescribing!
# p.s. I have re-read the spot the difference blog, but I don’t think it can explain this particular case._

# Reslizumab is a monoclonal antibody which is normally only prescribed by specialists, and therefore we wouldn't expect to see it in primary care prescribing.  However [OpenPrescribing shows over 15,000 items per year](https://openprescribing.net/analyse/#org=CCG&numIds=0304020Z0&denom=nothing&selectedTab=summary).  As the EPD data doesn't have any prescribing, the most likely candidate is the BNF maps supplied by the NHSBSA.  These maps describe changes to the BNF codes made in January every year, and the Bennett Institute uses these maps to normalise the BNF code to the most current version across all time periods.
#
#

# It would be worth checking the maps supplied by the NHSBSA (which are stored in the OpenPrescribing [GitHub repo](https://github.com/ebmdatalab/openprescribing/tree/main/openprescribing/frontend/management/commands/presentation_replacements) against both the "normalised" and "raw" data, to see if there are any abnormalities.

#import required libraries
import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
from ebmdatalab import bq
import os
import requests
from io import StringIO

# ### Import map data from Github

res = requests.get('https://api.github.com/repos/ebmdatalab/openprescribing/contents/openprescribing/frontend/management/commands/presentation_replacements') #uses GitHub API to get list of all files listed in measure definitions - defaults to main branch
data = res.text #creates text from API result
github_df = pd.read_json(data) #turns JSON from API result into dataframe
display(github_df) # displays all available files

codes_df = pd.DataFrame() #creates blank dataframe
for row in github_df[github_df['name'].str.contains('.txt')].itertuples(index=True): #iterates through rows, and continues if file is .txt 
        url = (getattr(row, "download_url")) #gets URL from API request  
        year_df=pd.read_csv(url,sep='\t',header=None, names=['old_bnf_code', 'new_bnf_code']) # creates 2 columns from tab-separated txt file, and names the column header
        year_df['change_date'] = pd.to_datetime(row.name[:4], format='%Y').strftime('%Y-%m-%d') # adds the year of change from the file name in a yyyy-mm-dd format
        codes_df = pd.concat([codes_df,year_df], axis=0, ignore_index=True) # concatentates into single dataframe

display(codes_df)

# ### Create codelist to download data from BigQuery

bnf_code_list = codes_df['old_bnf_code'].astype(str).tolist() + codes_df['new_bnf_code'].astype(str).tolist() # create two lists (one for old BNF codes, one for new), and concatenate into one list
where_clause = "(BNF_CODE LIKE '" +"%' OR BNF_CODE LIKE '".join(bnf_code_list) + "')" # create "WHERE bnf_code LIKE 'x%' or bnf_code LIKE 'y%'"" format for use with BigQuery

# ### Get data from BigQuery

#this query downloads all data from the raw data with either old or new BNF codes
sql = f"""
SELECT
  month,
  bnf_code,
  bnf_name,
  items
FROM
  richard.all_prescribing_items
WHERE {in_clause}
  """
exportfile = os.path.join("..","data","items_df.csv")
items_df = bq.cached_read(sql, csv_path=exportfile, use_cache=True)
display(items_df)

# +
#pd.options.mode.chained_assignment = None # suppress warning
#filtered_df['code_length'] = filtered_df['old_bnf_code'].astype(str).apply(len)
# -

# ### Merge with BNF code change maps

key = items_df['bnf_code'].str.extract('^(' + '|'.join(codes_df['old_bnf_code']) + ')') # create key to allow partial match of codes to join
code_check_df = codes_df.merge(items_df.assign(key=key), left_on='old_bnf_code', right_on='key').drop('key', 1) # merge two dfs using the key above
display(code_check_df)

filtered_df = df3[df3['old_bnf_code'].apply(lambda x: len(str(x)) != 15)]

filtered_df.head()

df3['month'] = pd.to_datetime(df3['month'])

df3['change_date'] = pd.to_datetime(df3['change_date'])

filtered_df = df3[df3['month'] > df3['change_date']]

filtered_df.head(500)

result_df = filtered_df.groupby(['old_bnf_code', 'new_bnf_code', 'bnf_name', 'change_date']).agg({'items': 'sum', 'month': 'max'}).reset_index().sort_values(by='items', ascending=False).rename(columns={'month': 'latest_month'})

result_df.head(200)

merged_df = pd.merge(result_df, items_df, left_on='new_bnf_code', right_on='bnf_code', how='inner')

merged_df.head()

wrong_code_df =  merged_df.groupby(['old_bnf_code', 'new_bnf_code', 'bnf_name_x', 'bnf_name_y','change_date']).agg({'items_x':'sum', 'items_y': 'sum', 'month': 'max'})

wrong_code_df.head(200)


