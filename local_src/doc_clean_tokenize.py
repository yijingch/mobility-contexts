## -------------------------------------- ##
## ---- Clean and Tokenize Documents ---- ##
## -------------------------------------- ##


# code reference:
# filter non-english: https://stackoverflow.com/questions/20078816/replace-non-ascii-characters-with-a-single-space/20079244#20079244


import os
import re
import csv
import nltk
import argparse
import numpy as np
import pandas as pd
from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords
from collections import Counter
import matplotlib.pyplot as plt

ROOT_PATH = os.getcwd()
STOPWORDS = set(stopwords.words("english"))
TOKENIZER = RegexpTokenizer(r'\w+')

DESC_FILE = "index/descriptives-from-2012.csv"


def fetch_txt_filepath(doc_folder, start_year, end_year):  # ok!
    """
    INPUT: name of the document folder, start - end (default: 2012 - 2021)
    OUTPUT: a dictionary of directory structure
    {2012:
        {subr1: [txt1, txt2, ...],
         subr2: [...],
         ..., }
     2013: {...},
     ...,
     2020: {...}}
    """
    year_dict = {}
    for year in [*range(start_year, end_year)]:
        print("  year:", year)
        subr_dict = {}
        subr_year = os.listdir(f"{ROOT_PATH}/data/{doc_folder}/{year}")

        try: subr_year.remove(".DS_Store")
        except: pass

        for subr in subr_year:
            subr_dict[subr] = os.listdir(f"{ROOT_PATH}/data/{doc_folder}/{year}/{subr}")
        year_dict[year] = subr_dict
    return year_dict


def read_txt(txt_fpath):  # ok!
    """
    INPUT: file path (not filename)
    OUTPUT: a long string
    """
    with open(txt_fpath, "r") as txtf:
        string_chunk = [line.strip() for line in txtf if line.strip()] # a list of strings
    string_piece = " ".join(string_chunk)
    return string_piece


def clean_string(string_piece):
    """
    INPUT: a single string
    OUTPUT: a cleaned string
    """
    string_piece = re.sub(r"/?[r|u|U]/[A-Za-z0-9_-]+", "", string_piece)  # clean user and subreddit name
    string_piece = re.sub(r"[^\x00-\x7f]","", string_piece) # clean non-english
    string_piece = re.sub(r"\(?https?:\/\/.*[\r\n]*\)?", "", string_piece) # clean url
    return string_piece


def tokenize(string_piece):
    """
    INPUT: a single string
    OUTPUT: a list of words (wordbag)
    """
    wordbag = TOKENIZER.tokenize(string_piece)
    return wordbag


def clean_wordbag(wordbag):
    """
    INPUT: a list of words (wordbag)
    OUTPUT: a cleaned wordbag
    """
    # clean stopwords and single-character words
    wordbag = [w.lower() for w in wordbag if (w not in STOPWORDS) and (len(w) > 1)]
    return wordbag


def build_subr_word_dict(subrname, year, txtf_ls, subm_total):
    """
    INPUT: subreddit name, a year number, a list of txt files, minimum doc length
    OUTPUT: a dictionary of tokens for the subreddit during the year
    {txt1: [word1, word2, ..., wordN]
     txt2: [...]
     ...
     txtN: [...]}
    """
    subr_word_dict = {}
    subr_doc_len_ls = []

    index = 0
    for txt_fname in txtf_ls:
        # print("    processing", txt_fname)
        txt_fpath = f"{ROOT_PATH}/data/{doc_folder}/{year}/{subr}/{txt_fname}"
        string_piece = read_txt(txt_fpath)
        string_piece = clean_string(string_piece)
        tokens = tokenize(string_piece)
        tokens = clean_wordbag(tokens)
        subr_doc_len_ls.append(len(tokens))
        subr_word_dict[txt_fname] = tokens
        index += 1
        if index%1000 == 0:
            print("    progress", index/subm_total)

    # min_doc_len = np.percentile(subr_doc_len_ls, 1-retain_pct)
    # we will filter out documents in the topic modeling script (at year level)
    # plt.hist(subr_doc_len_ls, bins=50)
    # plt.show()

    return subr_word_dict


def limit_vocab_size(subr_word_dict, retain_pct):
    """
    INPUT: a vocab dictionary for one subreddit in one time bin (one year)
    OUTPUT: a new vocab dictionary with limited words (with top frequency)
    """
    subr_wordbag = []  # a wordbag for the entire subreddit in one time bin
    for txt_fname, tokens in subr_word_dict.items():
        subr_wordbag += tokens
    count_words = Counter(subr_wordbag)
    orig_vsize = len(count_words)
    max_vsize = int(retain_pct * orig_vsize)

    print("    old vocab size:", orig_vsize)
    print("    new vocab size:", max_vsize)

    sorted_count_words = sorted(count_words.items(), key = lambda x: x[1], reverse=True)
    vocab = sorted_count_words[:max_vsize]
    vocab = [x[0] for x in vocab]  # a list of words we'll keep in the output dict

    new_subr_word_dict = {}
    print("    filtering infrequent words...")
    for txt_fname, tokens in subr_word_dict.items():
        new_subr_word_dict[txt_fname] = [w for w in tokens if w in vocab]
    return new_subr_word_dict


def write_token_csv(subrname, year, subr_word_dict, subm_total):
    """
    INPUT: subreddit name, one specified year, subreddit vocab dictionary
    OUTPUT: csv files
    -------------------------------------------------
    output folder structure:
      data
        |--- topic-tok
        |     |--- 2012
        |     |      |--- subr1
        |     |      |     |--- subr1_subm1_2012.csv
        |     |      |     |--- subr1_subm2_2012.csv
        |     |      |     |--- ...
        |     |      |     |--- subr1_submN_2012.csv
        |     |      |--- subr2
        |     |      |--- ...
        |     |      |--- subrN
        |     |--- 2013
        |     |--- ...
        |     |--- 2020
    -------------------------------------------------
    """
    index = 0
    for txt_fname, tokens in subr_word_dict.items():
        # print(f"    writing {txt_fname[:-9]}.csv")
        with open(f"data/{token_folder_name}/{year}/{subrname}/{txt_fname[:-9]}_{year}.csv", "w") as f:
            writer = csv.writer(f)
            writer.writerow(tokens)
        index += 1
        if index%1000 == 0:
            print("    progress", index/subm_total)


def lookup_item_num(subrname, year, obj_type):
    df = pd.read_csv(DESC_FILE)
    obj_num = int(df[(df["subr"]==subrname)&(df["year"]==year)&(df["obj_type"]==obj_type)].num_of_obj)
    return obj_num



if __name__ == "__main__":
    argparser = argparse.ArgumentParser()

    argparser.add_argument("--doc_folder", help="Name of the document folder", type=str, default="topic-doc", required=False)
    argparser.add_argument("--start_year", help="Start parsing from this year", type=int, default=2012, required=False)
    argparser.add_argument("--end_year", help="Start parsing from this year", type=int, default=2021, required=False)
    argparser.add_argument("--retain_vocab_pct", help="Percentage of vocabs (with top frequency) that we will keep in each subreddit's wordbag", type=float, default=0.8, required=True)
    # argparser.add_argument("--min_doc_len", help="Minimum document length required (to filter out documents that only contain a few words)", type=int, default=800, required=True)

    args = argparser.parse_args()
    doc_folder = args.doc_folder
    start_year = args.start_year
    end_year = args.end_year
    retain_vp = args.retain_vocab_pct
    # min_doc_len = args.min_doc_len

    token_folder_name = "subm-tok-small"

    # create a token folder
    try: os.mkdir(ROOT_PATH + f"/data/{token_folder_name}")
    except: pass

    print("fetching directory tree...")
    filepath_dict = fetch_txt_filepath(doc_folder, start_year, end_year)

    for year, subr_txtf_dict in filepath_dict.items():  # for each year
        print(f"""
        ********** ********** **********
                     {year}
        ********** ********** **********
        """)

        # create a folder for that year
        try: os.mkdir(ROOT_PATH + f"/data/{token_folder_name}/{year}")
        except: pass

        for subr, txtf_ls in subr_txtf_dict.items():  # for each subreddit
            print("for subreddit", subr)

            # create a folder for that subreddit
            try: os.mkdir(ROOT_PATH + f"/data/{token_folder_name}/{year}/{subr}")
            except: pass

            # look up submission number
            subm_total = lookup_item_num(subr, year, "subm")

            # build a word dict
            print("  building a subreddit word dict...")
            subr_wd = build_subr_word_dict(subr, year, txtf_ls, subm_total)

            # processing the word dict
            print("  limiting vocab size...")
            subr_wd = limit_vocab_size(subr_wd, retain_vp)

            # writing output csvs
            print("  writing output files...")
            write_token_csv(subr, year, subr_wd, subm_total)





# test
# python doc_clean_tokenize.py --doc_folder="topic-doc-small" --start_year=2012 --end_year=2014 --retain_vocab_pct=0.8


# func test
# doc_folder = "topic-doc-small"
# year = 2014
# subr = "askaconservative"
# test_ls = os.listdir(f"{ROOT_PATH}/data/{doc_folder}/{year}/{subr}")
# build_subr_word_dict("askaconservative", 2014, test_ls)


# test run2
# python doc_clean_tokenize.py --doc_folder="topic-doc-small2" --start_year=2012 --end_year=2021 --retain_vocab_pct=0.8




# def limit_doc_len(wordbag, min_len):
#     """
#     INPUT: a wordbag retrieved from a single txt file; the minimum document length
#     OUTPUT: True or False
#     """
#     if len(wordbag) >= min_len:
#         output = True
#     else:
#         output = False
#     return output


##
