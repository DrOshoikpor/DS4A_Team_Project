#!/usr/bin/env python
# coding: utf-8



import requests
import pandas as pd
import json
import csv
import shutil
import os


# Function that pulls metadata for datasets
def get_find_datasets(url, key_word):
    payments_datasets = [['dataset', 'dataset_id','download_url']]
    r = requests.get(url).json()

    for i in r:
        if (dict(i)['title'][:4] >= '2020') & (key_word in dict(i)['title']):
            payments_datasets.append([dict(i)['title'], 
                                      dict(i)['identifier'], 
                                     i['distribution'][0]['data']['downloadURL']])
    return payments_datasets


# Function to download files
def download_files(url, filename):
    # NOTE the stream=True parameter below
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=None):
                if chunk: 
                    f.write(chunk)
    return filename


# URL for dataset metadata. Need to pull the metadata first to get the download url for each dataset
url = 'https://openpaymentsdata.cms.gov/api/1/metastore/schemas/dataset/items?show-reference-ids'
keyword = 'Payment Data'

dataset_info = get_find_datasets(url, keyword)


file_save_location = 'data/'

for i in dataset_info[1:]:
    download_url = i[2]
    filename = file_save_location+f'{i[0]}.csv'.replace(' ','_').lower()
    dataset_id = i[1]
    
    print(download_url)
    print(f'Downloading {filename}.')
    #download_files(download_url, filename)
    print(f'Download DONE.\n')







