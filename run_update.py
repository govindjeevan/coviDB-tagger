#!/usr/bin/env python
# coding: utf-8


#─────────────────────────────────────────────────────────────────────────────────────────
#─██████████████─██████████████─██████──██████─██████████─████████████───██████████████───
#─██░░░░░░░░░░██─██░░░░░░░░░░██─██░░██──██░░██─██░░░░░░██─██░░░░░░░░████─██░░░░░░░░░░██───
#─██░░██████████─██░░██████░░██─██░░██──██░░██─████░░████─██░░████░░░░██─██░░██████░░██───
#─██░░██─────────██░░██──██░░██─██░░██──██░░██───██░░██───██░░██──██░░██─██░░██──██░░██───
#─██░░██─────────██░░██──██░░██─██░░██──██░░██───██░░██───██░░██──██░░██─██░░██████░░████─
#─██░░██─────────██░░██──██░░██─██░░██──██░░██───██░░██───██░░██──██░░██─██░░░░░░░░░░░░██─
#─██░░██─────────██░░██──██░░██─██░░██──██░░██───██░░██───██░░██──██░░██─██░░████████░░██─
#─██░░██─────────██░░██──██░░██─██░░░░██░░░░██───██░░██───██░░██──██░░██─██░░██────██░░██─
#─██░░██████████─██░░██████░░██─████░░░░░░████─████░░████─██░░████░░░░██─██░░████████░░██─
#─██░░░░░░░░░░██─██░░░░░░░░░░██───████░░████───██░░░░░░██─██░░░░░░░░████─██░░░░░░░░░░░░██─
#─██████████████─██████████████─────██████─────██████████─████████████───████████████████─
#─────────────────────────────────────────────────────────────────────────────────────────

#    𝓐𝓾𝓽𝓱𝓸𝓻:
#                    _         __    _                      
#     ___ ____ _  __(_)__  ___/ /   (_)__ ___ _  _____ ____ 
#    / _ `/ _ \ |/ / / _ \/ _  /   / / -_) -_) |/ / _ `/ _ \
#    \_, /\___/___/_/_//_/\_,_/ __/ /\__/\__/|___/\_,_/_//_/
#   /___/                      |___/                        
# 
#  Automation Script to update CoviDB Database (GSheet) with Tech/Non-Tech Tags 
#  Made for https://covidb.org/ by govindjeevan7@gmail.com
#  Last Modified: 07 Jul 2021


RESET_ALL=False
G_SERVICE_KEY_PATH="covid-key.json"
COVIDB_SHEET_KEY = "1JDHxCcE07oL5Mj3aqGRcm8a3rQkBufC5ndpdkSI9my4"
UPDATE_BATCH_SIZE = 50


import gspread
import numpy as np
import pandas as pd
import utils
import tqdm

def get_coverage():
    gc = gspread.service_account(filename=G_SERVICE_KEY_PATH)
    sh = gc.open_by_key(COVIDB_SHEET_KEY)
    worksheet = sh.sheet1
    data =  worksheet.get_all_values()
    data = np.array(data)
    articles = pd.DataFrame(data=data[1:,:],columns=data[0,:])
    return int((len(articles[( (articles["Technical (Y/N)"]=="Y")) | (articles["Technical (Y/N)"]=="N")])/len(articles))*100)

coverage = get_coverage()
print("Tagging Coverage: "+ str(coverage))

gc = gspread.service_account(filename=G_SERVICE_KEY_PATH)
sh = gc.open_by_key(COVIDB_SHEET_KEY)
worksheet = sh.sheet1
data =  worksheet.get_all_values()
data = np.array(data)
articles = pd.DataFrame(data=data[1:,:],columns=data[0,:])
tech_tag = worksheet.findall("Technical (Y/N)", in_row=1)[0]
mappings = utils.load_mappings("mapping.yaml")

TECHNICAL_TAG_COL = tech_tag.col
TECHNICAL_TAG_ADDR = tech_tag.address[0]

if RESET_ALL is False:
    articles = articles[(articles["Technical (Y/N)"]=="") | (articles["Technical (Y/N)"]=="Manual")][::-1]

print(f'Total Articles to Tag: {len(articles)}')

num_batches = int(len(articles)/UPDATE_BATCH_SIZE)

cnt = 0
for article_batch in np.array_split(articles, num_batches):
    cnt+=1
    print(f'Processing Batch {cnt}..')
    categories = utils.get_category_parallel(article_batch["Resource URL"].to_numpy())
    print(categories)
    update_range = []
    for i, idx in enumerate(article_batch.index):
        article = article_batch.loc[idx]
        tag = utils.find_tag(mappings, utils.get_host(article["Resource URL"]), categories[i])
        if tag =="technical":
            tag = "Y"
        elif tag == "non-technical":
            tag = "N"
        else:
            tag = "Manual"
        article_dict = {
            "values": [[tag]] , 
            "range": TECHNICAL_TAG_ADDR+str(idx+2)
        }
        update_range.append(article_dict)
    if (len(update_range)>0):
        print(f'Updating Batch {cnt} to Google Sheet..')
        try:
            worksheet.batch_update(update_range)
        except:
            print("Count not update")

coverage = get_coverage()
print("Tagging Coverage: "+ str(coverage))
utils.update_coverage_readme(coverage)