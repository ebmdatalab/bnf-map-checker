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

import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
from ebmdatalab import bq
import os
import requests
from io import StringIO

# +
# Specify the GitHub repository information
repo_owner = 'ebmdatalab'
repo_name = 'openprescribing/openprescribing/frontend/management/commands/presentation_replacements'
base_url = f'https://api.github.com/repos/{repo_owner}/{repo_name}/contents/'

# Get the list of all files in the repository
response = requests.get(base_url)
if response.status_code == 200:
    repo_contents = response.json()

    # Filter for only .txt files
    txt_files = [file['name'] for file in repo_contents if file['name'].endswith('.txt')]

    # Loop through each .txt file and import as a DataFrame
    for txt_file in txt_files:
        file_url = base_url + txt_file
        # Fetch the content of the file
        response = requests.get(file_url)
        if response.status_code == 200:
            content = response.text
            # Assuming your .txt files are delimited (e.g., CSV), adjust the separator accordingly
            df = pd.read_csv(StringIO(content), delimiter='\t')  # Replace '\t' with the appropriate separator
            # Display or process the DataFrame as needed
            print(f"DataFrame for {txt_file}:\n{df}\n")
        else:
            print(f"Failed to fetch {txt_file}. Status code: {response.status_code}")
else:
    print(f"Failed to fetch repository contents. Status code: {response.status_code}")
# -

res = requests.get('https://api.github.com/repos/ebmdatalab/openprescribing/contents/openprescribing/frontend/management/commands/presentation_replacements') #uses GitHub API to get list of all files listed in measure definitions - defaults to main branch
data = res.text #creates text from API result
df = pd.read_json(data) #turns JSON from API result into dataframe


df.head()

codes_df = pd.DataFrame() #creates blank dataframe
for row in df[df['name'].str.contains('.txt')].itertuples(index=True): #iterates through rows, and continues if file is .json
        url = (getattr(row, "download_url")) #gets URL from API request  
        year_df=pd.read_csv(url,sep='\t',header=None, names=['old_bnf_code', 'new_bnf_code'])
        year_df['change_date'] = pd.to_datetime(row.name[:4], format='%Y').strftime('%Y-%m-%d') # adds the year of change from the file name
        codes_df = pd.concat([codes_df,year_df], axis=0, ignore_index=True) # concatentates into single dataframe

year_df.head()

print(codes_df)

# +
#bnf_code_list = (codes_df['old_bnf_code'] + codes_df['new_bnf_code']).tolist()

old_bnf_code_list = codes_df['old_bnf_code'].astype(str).tolist()
new_bnf_code_list = codes_df['new_bnf_code'].astype(str).tolist()
bnf_code_list = old_bnf_code_list + new_bnf_code_list

# -

in_clause = "(BNF_CODE LIKE '" +"%' OR BNF_CODE LIKE '".join(bnf_code_list) + "')"
where = "WHERE " +in_clause
print(where)

#pull data from BigQuery for antibiotic stewardship: co-amoxiclav, cephalosporins & quinolones (KTT9) measure
in_clause = "(BNF_CODE LIKE '" +"%' OR BNF_CODE LIKE '".join(bnf_code_list) + "')"
#where = "WHERE " +in_clause
#print(where)
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

items_df.head()

filtered_df = codes_df[codes_df['old_bnf_code'].apply(lambda x: len(str(x)) != 15)]

print(filtered_df)

#pull data from BigQuery for antibiotic stewardship: co-amoxiclav, cephalosporins & quinolones (KTT9) measure
sql = """
SELECT *
FROM hscic.bnf
  """
exportfile = os.path.join("..","data","bnf_df.csv")
bnf_df = bq.cached_read(sql, csv_path=exportfile, use_cache=False)

pd.options.mode.chained_assignment = None # suppress warning
filtered_df['code_length'] = filtered_df['old_bnf_code'].astype(str).apply(len)









# +
df1 = pd.DataFrame({'A': ['a', 'b', 'cc']})

df2 = pd.DataFrame({'B': ['ar', 'd', 'ar'],
                    'C': ['x1', 'x1', 'x2']})
# -

df1.head()

df2.head()

key = df2['B'].str.extract('^(' + '|'.join(df1['A']) + ')')


key.head()

df3 = df1.merge(df2.assign(key=key), left_on='A', right_on='key').drop('key', 1)


df3.head()

items_df.head()

codes_df.head()

key = items_df['bnf_code'].str.extract('^(' + '|'.join(codes_df['old_bnf_code']) + ')')

key.head()

df3 = codes_df.merge(items_df.assign(key=key), left_on='old_bnf_code', right_on='key').drop('key', 1)

df3.head()

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


