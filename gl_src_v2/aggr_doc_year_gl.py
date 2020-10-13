## ------------------------------------ ##
## ---- aggregate document by year ---- ##
## ------------------------------------ ##

from datetime import datetime
import pandas as pd
import numpy as np
import argparse
import sqlite3
import ijson
import json
import csv
import os

from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords

# ---- FILEPATH ----

# ROOT_PATH = os.getcwd()
ROOT_PATH = "/gpfs/accounts/azjacobs_root/azjacobs1/yijingch/reddit-topic-models"
WD_PATH = os.getcwd()

DBNAME = "reddit-db-preprocessed.db"
DBPATH = ROOT_PATH + "/data/" + DBNAME

CACHE_FILE = "processed_year.csv"
CACHE_FPATH = WD_PATH + "/" + CACHE_FILE

# ----

STOPWORDS = {'out', 'own', 'mightn', 've', 'are', 'just', 'won', 'any', 'up', 'them', 'mustn', 'but', 'doesn', 'or', 'd', 'of', 'too', 'can', 'yourself', 'an', 'hadn', 'from', 'she', 'through', 'over', "mightn't", 'y', 'as', "hadn't", 'yours', 'needn', 'its', 'above', 'aren', 'which', 'into', 'very', 'nor', 'it', 'ma', 'once', "it's", 'being', 'than', 't', "shan't", 'your', 'how', 'am', 'should', 'he', 'his', 'haven', 'down', 'on', 'each', "don't", 'in', 'couldn', 'few', 'between', "shouldn't", 'm', 'myself', 'be', "you've", "that'll", 'under', 'then', 'some', 'and', "should've", 'you', 'whom', "aren't", 'with', 'have', 'until', 'been', 'ain', "wasn't", 're', 'having', 'will', 'wouldn', 'i', 'before', 'a', 'ourselves', 'more', "won't", 'is', 'him', 'about', 'those', 'other', 'only', 'after', 'during', "hasn't", "needn't", 'ours', 'does', "you'll", 'most', 'no', 'herself', 'themselves', "didn't", 'now', 'were', 'there', 'not', 'both', 'do', "haven't", 'her', 'their', 'by', 'was', 'these', 'isn', 'when', 'did', 'the', 'again', 'so', "weren't", 'll', 'that', 'has', "wouldn't", 'who', "you're", 'same', 'shouldn', 'my', 'hers', 'because', 'hasn', 'below', 'while', 'yourselves', 'off', 'here', 'they', 'theirs', 'shan', 'didn', 'doing', 'to', 'had', 'me', 'what', 'if', "she's", 'for', 'weren', 'o', 's', "isn't", "couldn't", "mustn't", 'we', 'against', 'wasn', 'at', 'our', 'such', 'why', 'this', 'further', 'don', 'all', 'itself', "doesn't", "you'd", 'himself', 'where'}
TOKENIZER = RegexpTokenizer(r'\w+')

# ----


def fetch_table(year):
    subm_df = pd.read_sql_query(f"SELECT * FROM 'submission-{year}'", conn)
    comm_df = pd.read_sql_query(f"SELECT * FROM 'comment-{year}'", conn)
    return subm_df, comm_df


def tokenize(string_piece):
    wordbag = TOKENIZER.tokenize(string_piece)
    return wordbag


def clean_wordbag(wordbag):
    wordbag = [w.lower() for w in wordbag if (w not in STOPWORDS) and (len(w) > 1)]
    return wordbag


def aggregate_by_subm(year): # ok!
    subm_df, comm_df = fetch_table(year)

    # aggregate comments
    comm_df["subm_id"] = comm_df["link_id"].map(lambda x: x[3:])
    aggr_func = {"body": lambda x: list(x)}
    comm_aggr = comm_df.groupby("subm_id").aggregate(aggr_func).reset_index()
    comm_aggr.columns = ["id", "comments"]

    # merge tables
    these_cols = ["id", "comments", "body", "created_utc", "subreddit"]
    aggr_df = comm_aggr.merge(subm_df, on="id")[these_cols]

    return aggr_df


def clean_tokenize_df(aggr_df): # ok!
    aggr_df = aggr_df[:10000].copy() # subset for debugging

    aggr_df["comments"] = aggr_df["comments"].map(lambda x: " ".join(x))
    aggr_df["tokens"] = aggr_df.apply(lambda x: tokenize(x.comments)+tokenize(x.body), axis=1)
    aggr_df["tokens"] = aggr_df["tokens"].map(lambda x: clean_wordbag(x))

    these_cols = ["id", "created_utc", "subreddit", "tokens"]

    return aggr_df[these_cols]


def dump_df_to_json(aggr_df, year, filepath):
    aggr_df = aggr_df[:100] # subset for debugging
    aggr_df.to_json(filepath, orient = "records")



if __name__ == "__main__":

    argparser = argparse.ArgumentParser()
    argparser.add_argument("--start_year", help="Start parsing from this year", type=int, default=2012, required=False)
    argparser.add_argument("--end_year", help="Start parsing from this year", type=int, default=2021, required=False)

    args = argparser.parse_args()
    start_year = args.start_year
    end_year = args.end_year


    # set up db
    conn = sqlite3.connect(DBPATH)
    cur = conn.cursor()


    token_folder = "topic-token"
    try:
        os.mkdir(ROOT_PATH + f"/data/{token_folder}")
        print(f"created folder: /data/{token_folder}")
    except:
        print("folder already exist!")
        pass


    # we need to process
    all_years = [*range(start_year, end_year)]


    # what we already have: check if cache file exists
    if os.path.isfile(CACHE_FPATH):
        with open(CACHE_FPATH, "r") as csvf:
            reader = csv.reader(csvf)
            processed_year = list(reader)[0]
            processed_year = [int(x) for x in processed_year]
    else:
        processed_year = []

    # year to process
    year_to_process = list(set(all_years) - set(processed_year))
    print(f"already processed: {processed_year}")
    print(f"will process: {year_to_process}")

    for year in year_to_process:
        try:
            os.mkdir(ROOT_PATH + f"/data/{token_folder}/{year}")
            print(f"created folder /data/{token_folder}/{year}")
        except:
            print("folder already exists!")

        print(f"""
        ********** ********** ********** ********** **********
                           PROCESSING {year}
        ********** ********** ********** ********** **********
        """)

        print(" - aggregating by submission...")
        aggr_df = aggregate_by_subm(year)
        print(" - cleaning and tokenizing...")
        aggr_df_clean = clean_tokenize_df(aggr_df)
        print(" - exporting results to json...")
        fpath = ROOT_PATH + f"/data/{token_folder}/{year}/tokens-{year}.json"
        dump_df_to_json(aggr_df_clean, year, filepath=fpath)
        processed_year.append(year)

        with open(CACHE_FPATH, "w",) as csvf:
            writer = csv.writer(csvf)
            writer.writerow(processed_year)

# test python3 aggr_doc_year.py --start_year=2012 --end_year=2014








##
