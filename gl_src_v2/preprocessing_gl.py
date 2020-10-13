## -------------------------------------------------- ##
## -- Preprocessing documents (clean and tokenize) -- ##
## -------------------------------------------------- ##

import os
import re
import csv
import json
import sqlite3
import argparse
import numpy as np
import pandas as pd
import requests
from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords


# ---- FILEPATH ----

ROOT_PATH = "/gpfs/accounts/azjacobs_root/azjacobs1/yijingch/reddit-topic-models"
WD_PATH = os.getcwd()

DBNAME = "reddit-db-new.db"
DBPATH = ROOT_PATH + "/data/" + DBNAME

DBNAME_CLEAN = "reddit-db-preprocessed.db"
DBPATH_CLEAN = ROOT_PATH + "/data/" + DBNAME_CLEAN

CACHE_FILE = "processed_table.csv"
CACHE_FPATH = WD_PATH + "/" + CACHE_FILE

# ----


def fetch_table(tablename): # ok!
    t = tablename[:tablename.index("-")]  # table type
    if t  == "submission":
        qr = f"SELECT id, author, subreddit, title, selftext, created_utc FROM '{tablename}' "
        qr += "WHERE author != '[deleted]'"
        df = pd.read_sql_query(qr, conn1)
        df["body"] = df.apply(lambda x: x.title + " " + x.selftext, axis=1)
    elif t == "comment":
        qr = f"SELECT id, author, subreddit, link_id, body, created_utc FROM '{tablename}' "
        qr += "WHERE author != '[deleted]'"
        df = pd.read_sql_query(qr, conn1)
    else:
        print("invalid table type")
        df = None
    return df


def clean_string(string_piece):
    """
    INPUT: a single string
    OUTPUT: a cleaned string
    """
    string_piece = re.sub(r"/?[r|u|U]/[A-Za-z0-9_-]+", "", string_piece)  # clean user and subreddit name
    string_piece = re.sub(r"[^\x00-\x7f]","", string_piece) # clean non-english
    string_piece = re.sub(r"\(?https?:\/\/.*[\r\n]*\)?", "", string_piece) # clean url
    return string_piece


# def score_string(comment_string):
#     api_key = "AIzaSyAcch7IEkIQMELQOGQEWqZ3SLtzP_wteek"
#     url = ("https://commentanalyzer.googleapis.com/v1alpha1/comments:analyze" + "?key=" + api_key)
#     data_dict = {
#         "comment": {"text": comment_string},
#         "languages": ["en"],
#         'requestedAttributes': {'TOXICITY': {}, 'SEVERE_TOXICITY': {}}
#     }
#     response = requests.post(url=url, data=json.dumps(data_dict))
#     response_dict = json.loads(response.content)
#     try:
#         toxic = response_dict["attributeScores"]['TOXICITY']["spanScores"][0]["score"]["value"]
# #         severe = response_dict['attributeScores']['SEVERE_TOXICITY']["spanScores"][0]["score"]["value"]
# #         output = (toxic, severe)
#         output = toxic
#     except:
#         print(f"can't score {comment_string}")
#         output = ""
#     return output


def preprocessing(tablename, min_len=5):
    print(f"fetching {tablename}...")
    # df = fetch_table(tablename).sample(50) # for debugging
    df = fetch_table(tablename)

    print(f"cleaning {tablename}")
    df["body"] = df["body"].map(lambda x: clean_string(x)).astype("str")
    df["body_len"] = df["body"].map(lambda x: len(x)).astype("int")
    df = df[df["body_len"] > min_len]

    # print(f"scoring {tablename}")
    # df["score"] = df["body"].map(lambda x: score_string(x))

    df = df[["id", "author", "subreddit", "body", "created_utc"]]

    df.to_sql(tablename, conn2, if_exists="replace")
    print(f"new {tablename} saved!")
    return df






if __name__ == "__main__":

    argparser = argparse.ArgumentParser()
    argparser.add_argument("--start_year", help="Start parsing from this year", type=int, default=2012, required=False)
    argparser.add_argument("--end_year", help="Start parsing from this year", type=int, default=2021, required=False)

    args = argparser.parse_args()
    start_year = args.start_year
    end_year = args.end_year

    # set up db
    conn1 = sqlite3.connect(DBPATH)
    cur1 = conn1.cursor()
    conn2 = sqlite3.connect(DBPATH_CLEAN)
    cur2 = conn2.cursor()

    # get all tables
    cur1.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    all_tablenames = [x[0] for x in cur1.fetchall()]
    all_tablenames = [x for x in all_tablenames if int(x[-4:]) in list(np.arange(start_year, end_year))]

    # check if cache file exists:
    if os.path.isfile(CACHE_FPATH):
        with open(CACHE_FPATH, "r") as csvf:
            reader = csv.reader(csvf)
            processed_table = list(reader)[0]
    else:
        processed_table = []

    # processed unfinished tables
    table_to_process = list(set(all_tablenames) - set(processed_table))
    print(f"{len(processed_table)} processed tables: {processed_table}")
    print(f"{len(table_to_process)} unprocessed tables: {table_to_process}")

    for table in table_to_process:
        print(f"processing {table}")
        preprocessing(table)
        processed_table.append(table)
        # update cache file
        with open(CACHE_FPATH, "w",) as csvf:
            writer = csv.writer(csvf)
            writer.writerow(processed_table )
