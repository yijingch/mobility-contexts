## --------------------------------------------- ##
## ---- Aggregate document (per submission) ---- ##
## --------------------------------------------- ##

# endpoint documentation: https://pushshift.io/api-parameters/

from datetime import datetime
# from reset_db import DBNAME, DBPATH
import pandas as pd
import numpy as np
import argparse
import sqlite3
import ijson
import json
import os

## todo: get large filelist
## compose a large subreddit name list


ROOT_PATH = "/gpfs/accounts/azjacobs_root/azjacobs1/yijingch/reddit-topic-models"
DESC_FILE = ROOT_PATH + "/data/index/descriptives-from-2012.csv"
# LARGE_SUBR = ["politics", "the_donald", "chapotraphouse", "neoliberal"]

INVALID_TEXT = ["[deleted]", "[removed]", "\n"]


def load_subreddit(filepath = ROOT_PATH + "/data/index/left-right-labels.csv"):
    data = pd.read_csv(filepath)
    subreddit_ls = data.subreddit.tolist()
    return subreddit_ls


def fetch_data(colname, jsondata):
    try: output = jsondata[colname]
    except: output = ""
    return output


def fetch_json(folder_name, start_year, end_year):  # ok!
    """
    -------------------------------------------------
    input folder structure:
      data
        |--- raw
        |     |--- comment-from-2012
        |     |      |--- 2012
        |     |      |     |--- comm_subreddit1_2012.json
        |     |      |     |--- comm_subreddit2_2012.json
        |     |      |     |--- ...
        |     |      |     |--- comm_subreddiN_2012.json
        |     |      |--- 2013
        |     |      |--- ...
        |     |      |--- 2020
        |     |--- submission-from-2012
        |     |      |--- 2012
        |     |      |     |--- subm_subreddit1_2012.json
        |     |      |     |--- subm_subreddit2_2012.json
        |     |      |     |--- ...
        |     |      |     |--- subm_subredditN_2012.json
        |     |      |--- 2013
        |     |      |--- ...
        |     |      |--- 2020
    -------------------------------------------------
    ACTION: fetch a list of json filepath (not only filename)
    """
    json_files_all = []
    json_files_year = []
    for year in [*range(start_year, end_year)]:
        json_files_year = os.listdir(f"{ROOT_PATH}/data/raw/{folder_name}/{year}")
        json_files_all += json_files_year

    return json_files_all


def aggr_subm_to_txt(js_fname, js_fpath, total_num):
    """
    -------------------------------------------------
    output folder structure:
      data
        |--- topic-doc
        |     |--- 2012
        |     |      |--- subr1
        |     |      |     |--- subr1_subm1_2012.txt
        |     |      |     |--- subr1_subm2_2012.txt
        |     |      |     |--- ...
        |     |      |     |--- subr1_submN_2012.txt
        |     |      |--- subr2
        |     |      |--- ...
        |     |      |--- subrN
        |     |--- 2013
        |     |--- ...
        |     |--- 2020
    -------------------------------------------------
    ACTION:
    1. parse json
    2. create and write txt file
    """

    subrname = js_fname[5:-10]
    year = int(js_fname[-9:-5])

    with open(js_fpath) as jsonf:
        data = ijson.items(jsonf, "item")
        index = 1
        for d in data:
            id = fetch_data("id", d)
            selftext = fetch_data("selftext", d).strip()
            if selftext not in INVALID_TEXT:
                title = fetch_data("title", d)
                with open(ROOT_PATH + f"/data/{doc_folder}/{year}/{subrname}/{subrname}_{id}_{year}.txt", "w") as txtf:
                    txtf.write(title + " " + selftext + "\n")
            index += 1
            if index%1000 == 0:
                print(f"    {js_fname} -- progress:", index/total_num)



def aggr_comm_to_txt(js_fname, js_fpath, total_num):
    """
    ACTION:
    1. parse json
    2. if file exists: append content in the existing txt file;
       if not, create and write a new txt file.
    """

    subrname = js_fname[5:-10]
    year = int(js_fname[-9:-5])

    with open(js_fpath) as jsonf:
        data = ijson.items(jsonf, "item")
        index = 1
        for d in data:
            id = fetch_data("id", d)
            link_id = fetch_data("link_id", d)
            subm_id = link_id[3:]
            body = fetch_data("body", d).strip()
            if body not in INVALID_TEXT:
                # if os.path.isfile(f"data/topic-doc/{year}/{subrname}/{subrname}_{subm_id}_{year}.txt"):
                #     print("appending!")
                with open(f"data/{doc_folder}/{year}/{subrname}/{subrname}_{subm_id}_{year}.txt", "a") as txtf:
                    txtf.write(body + "\n")
            index += 1
            if index%1000 == 0:
                print(f"    {js_fname} -- progress:", index/total_num)


def lookup_item_num(subrname, year, obj_type):
    df = pd.read_csv(DESC_FILE)
    obj_num = int(df[(df["subr"]==subrname)&(df["year"]==year)&(df["obj_type"]==obj_type)].num_of_obj)
    return obj_num




if __name__ == "__main__":

    argparser = argparse.ArgumentParser()

    argparser.add_argument("--submission_folder", help="Name of the folder that stores raw submission data", type=str, default="submission-from-2012", required=False)
    argparser.add_argument("--comment_folder", help="Name of the folder that stores raw comment data", type=str, default="comment-from-2012", required=False)
    argparser.add_argument("--start_year", help="Start parsing from this year", type=int, default=2012, required=False)
    argparser.add_argument("--end_year", help="Start parsing from this year", type=int, default=2021, required=False)

    args = argparser.parse_args()
    submission_folder = args.submission_folder
    comment_folder = args.comment_folder

    doc_folder = "topic-doc-all"

    # os.mkdir(ROOT_PATH + f"/data/{doc_folder}")
    # print(f"created folder: /data/{doc_folder}")

    try:
        os.mkdir(ROOT_PATH + f"/data/{doc_folder}")
        print(f"created folder: /data/{doc_folder}")
    except:
        print("folder already exists!")
        pass

    for year in [*range(args.start_year, args.end_year)]:
        try:
            os.mkdir(ROOT_PATH + f"/data/{doc_folder}/{year}")
            print(f"created folder: /data/{doc_folder}/{year}")
        except:
            print("folder already exists!")
            pass

    print("""
    ********** ********** ********** ********** **********
                   AGGREGATING SUBMISSIONS
    ********** ********** ********** ********** **********
    """)

    print("1. fetching submission json files...")
    submission_jsons = fetch_json(folder_name=submission_folder, start_year=args.start_year, end_year=args.end_year)
    print("  number of files:", len(submission_jsons), "\n")

    print("2. parsing json iterable objects...")
    index = 1
    for js_fname in submission_jsons:

        subrname = js_fname[5:-10]
        year = int(js_fname[-9:-5])

        try: os.mkdir(ROOT_PATH + f"/data/{doc_folder}/{year}/{subrname}")
        except: pass

        # if subrname not in LARGE_SUBR:  # comment this in the final run
        print("  processing file:", f"{index}/{len(submission_jsons)}", "-", js_fname)
        json_fpath = f"{ROOT_PATH}/data/raw/{submission_folder}/{year}/{js_fname}"
        total_subm_num = lookup_item_num(subrname, year, "subm")
        print("  total submissions:", total_subm_num)
        aggr_subm_to_txt(js_fname, json_fpath, total_subm_num)
        index += 1


    print("""
    ********** ********** ********** ********** **********
                     AGGREGATING COMMNETS
    ********** ********** ********** ********** **********
    """)

    print("3. fetching comment json files...")
    comment_jsons = fetch_json(folder_name=comment_folder, start_year=args.start_year, end_year=args.end_year)
    print("  number of files:", len(comment_jsons), "\n")

    print("4. parsing json iterable objects...")
    index = 1
    for js_fname in comment_jsons:

        subrname = js_fname[5:-10]
        year = int(js_fname[-9:-5])

        # if subrname not in LARGE_SUBR:  # comment this in the final run
        print("  processing file:", f"{index}/{len(comment_jsons)}", "-", js_fname)
        json_fpath = f"{ROOT_PATH}/data/raw/{comment_folder}/{year}/{js_fname}"
        total_comm_num = lookup_item_num(subrname, year, "comm")
        print("  total comments:", total_comm_num)
        aggr_comm_to_txt(js_fname, json_fpath, total_comm_num)
        index += 1



# test
# python3 aggr_per_subm_year.py --start_year=2012 --end_year=2021
