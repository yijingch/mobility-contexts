## -------------------------------------------- ##
## ---- Parse JSON and save into SQLite DB ---- ##
## -------------------------------------------- ##

# endpoint documentation: https://pushshift.io/api-parameters/

from datetime import datetime
from reset_db import DBNAME, DBPATH
import pandas as pd
import numpy as np
import argparse
import sqlite3
import ijson
import json
import os


ROOT_PATH = os.getcwd()
DESC_FILE = "notebooks/descriptives-from-2012.csv"



def load_subreddit(filepath = "data/left-right-labels.csv"):
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
        # json_files_year = [f"{ROOT_PATH}/data/raw/{folder_name}/{year}" + "/" + x for x in json_files_year]
        json_files_all += json_files_year
    # print(json_files_all)

    return json_files_all



# def stream_json(filepath):
#     with open(filepath) as jsonf:
#         data = ijson.items(jsonf, "item")
#     return data


def parse_submission_to_db(filepath, total_num):
    with open(filepath) as jsonf:
        data = ijson.items(jsonf, "item")

        index = 1
        for d in data:
            id = fetch_data("id", d)
            author = fetch_data("author", d)
            author_fullname = fetch_data("author_fullname", d)
            subreddit = fetch_data("subreddit", d)
            title = fetch_data("title", d)
            selftext = fetch_data("selftext", d).strip()
            score = fetch_data("score", d)
            created_utc = fetch_data("created_utc", d)
            try:
                insertion = (id, author, author_fullname, subreddit, title, selftext, score, created_utc)
                statement = "INSERT into submission "
                statement += "VALUES (?,?,?,?,?,?,?,?)"
                cur.execute(statement, insertion)
                conn.commit()
            except Exception as e:
                print("Error in INSERTING LINES at TABLE submission:", e)
            index += 1
            if index%1000 == 0:
                print("    parsing progress:", index/total_num)


def parse_comment_to_db(filepath, total_num):
    with open(filepath) as jsonf:
        data = ijson.items(jsonf, "item")

        index = 1
        for d in data:
            id = fetch_data("id", d)
            author = fetch_data("author", d)
            author_fullname = fetch_data("author_fullname", d)
            subreddit = fetch_data("subreddit", d)
            parent_id = fetch_data("parent_id", d)
            link_id = fetch_data("link_id", d)
            body = fetch_data("body", d).strip()
            score = fetch_data("score", d)
            created_utc = fetch_data("created_utc", d)
            # print(id)
            try:
                insertion = (id, author, author_fullname, subreddit, parent_id, link_id, body, score, created_utc)
                statement = "INSERT into comment "
                statement += "VALUES (?,?,?,?,?,?,?,?,?)"
                cur.execute(statement, insertion)
                conn.commit()
            except Exception as e:
                print("Error in INSERTING LINES at TABLE comment", e)
            index += 1
            if index%1000 == 0:
                print("    parsing progress", index/total_num)




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

    # connect to db
    conn = sqlite3.connect(DBPATH + DBNAME)
    cur = conn.cursor()

    print("""
    ********** ********** ********** ********** **********
                    PARSING SUBMISSIONS
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
        # print("subr", subrname)
        # print("year", year)

        print("  processing file:", f"{index}/{len(submission_jsons)}", "-", js_fname)
        json_fpath = f"{ROOT_PATH}/data/raw/{submission_folder}/{year}/{js_fname}"
        total_subm_num = lookup_item_num(subrname, year, "subm")
        print("  total submissions:", total_subm_num)
        parse_submission_to_db(json_fpath, total_subm_num)
        index += 1
        # if index > 20: break


    # # test
    # js_fname = submission_jsons[0]
    # subrname = js_fname[5:-10]
    # year = int(js_fname[-9:-5])
    # print("subr", subrname)
    # print("year", year)
    # json_fpath = f"{ROOT_PATH}/data/raw/{submission_folder}/{year}/{js_fname}"
    # total_subm_num = lookup_item_num(subrname, year, "subm")
    # print(total_subm_num)
    # parse_submission_to_db(json_fpath, total_subm_num)

    print("""
    ********** ********** ********** ********** **********
                       PARSING COMMENTS
    ********** ********** ********** ********** **********
    """)

    print("3. fetching comment json files...")
    comment_jsons = fetch_json(folder_name=comment_folder, start_year=args.start_year, end_year=args.end_year)
    print(comment_jsons)
    print("  number of files:", len(comment_jsons))


    print("4. parsing json iterable objects...")
    index = 1
    for js_fname in comment_jsons:
        subrname = js_fname[5:-10]
        year = int(js_fname[-9:-5])
        # print("subr", subrname)
        # print("year", year)

        print("  processing file:", f"{index}/{len(comment_jsons)},"  "-", js_fname)
        json_fpath = f"{ROOT_PATH}/data/raw/{comment_folder}/{year}/{js_fname}"
        total_comm_num = lookup_item_num(subrname, year, "comm")
        print("  total comments:", total_comm_num)
        parse_comment_to_db(json_fpath, total_comm_num)
        index += 1
        # if index > 20: break

    # # test
    # js_fname = comment_jsons[3]
    # subrname = js_fname[5:-10]
    # year = int(js_fname[-9:-5])
    # print("subr", subrname)
    # print("year", year)
    # json_fpath = f"{ROOT_PATH}/data/raw/{comment_folder}/{year}/{js_fname}"
    # total_comm_num = lookup_item_num(subrname, year, "comm")
    # print("total", total_comm_num)
    # parse_comment_to_db(json_fpath, total_comm_num)








# test
# fetch_json("submission-from-2012", 2012, 2013)
# python3 parse_json_to_db.py --submission_folder="submission-from-2012" --start_year=2012 --end_year=2013
# python3 parse_json_to_db.py --comment_folder="comment-from-2012" --start_year=2012 --end_year=2013

# final-run
# python3 reset_db.py
# python3 parse_json_to_db.py --submission_folder="submission-from-2012" --comment_folder="comment-from-2012" --start_year=2012 --end_year=2021

# test-set (smaller data sample for debugging)
# python3 reset_db.py
# python3 parse_json_to_db.py --submission_folder="submission-from-2012" --comment_folder="comment-from-2012" --start_year=2012 --end_year=2015
