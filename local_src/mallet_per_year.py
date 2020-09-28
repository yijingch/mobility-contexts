## ------------------------------------------- ##
## ---- Build a MALLET model for each year---- ##
## ------------------------------------------- ##


import gensim
from gensim.test.utils import common_texts
from gensim.corpora.dictionary import Dictionary
from gensim.test.utils import datapath
from gensim.models.wrappers import ldamallet
from gensim.parsing.preprocessing import remove_stopwords
from gensim.models.coherencemodel import CoherenceModel
import pandas as pd
import argparse
# import pickle
import json
import os
import os.path
import csv


"""
    -------------------------------------------------
    token folder structure:
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


ROOT_PATH = os.getcwd()
os.environ.update({"MALLET_HOME":r"/Users/yijingch/mallet-2.0.8/"})
PATH_TO_MALLET = "/Users/yijingch/mallet-2.0.8/bin/mallet"


def read_csv(csv_fpath):
    """
    INPUT: a single csv filepath
    OUTPUT: a single list of tokens
    """
    with open(csv_fpath, "r") as csvf:
        reader = csv.reader(csvf, delimiter=",")
        tokens = list(reader)[0]
    return tokens

def write_json(index_dict, year):
    with open("index/directory-tree-" + str(year) + ".json", "w") as jsonf:
        json.dump(index_dict, jsonf)

def read_json(json_fpath):
    with open(json_fpath) as jsonf:
        data = json.load(jsonf)
    return data


def import_tokens(token_folder, year, min_len):
    """
    INPUT: name of the token_folder, one specific year, minimum number of tokens required in a document (will filter out short documents)
    OUTPUT/ACTION:
    1) return a list of token list (what I'll call a token chunk) for one specific year
    [[tokens from subm1 in subr1],
     [tokens from subm2 in subr1],
     [...],
     [tokens from submN in subr1],
     [tokens from subm1 in subr2],
     ...
     [tokens from submN in subrN]]
    2) write an index dictionary for looking up documents
    {subr1: [subm1, subm2, ..., submN],
     subr2: [subm1, subm2, ..., submN],
     ...
     subrN: [subm1, subm2, ..., submN]}
    """
    year_token_chunk = []

    if os.path.isfile(ROOT_PATH + "/index/directory-tree-" + str(year) + ".json"):
        print("fetching directory trees...")
        year_index_dict = read_json(ROOT_PATH + "/index/directory-tree-" + str(year) + ".json")
        for subr, csv_fls in year_index_dict.items():
            print("  from subreddit", subr)
            for csvf in csv_fls:
                csv_fpath = ROOT_PATH + "/data/" + token_folder + "/" + str(year) + "/" + subr + "/" + csvf
                tokens = read_csv(csv_fpath)
                if len(tokens) >= min_len:
                    year_token_chunk.append(tokens)

    else:
        print("building directory trees...")
        subr_ls = os.listdir(ROOT_PATH + "/data/" + token_folder + "/" + str(year))

        try: subr_ls.remove(".DS_Store")
        except: pass

        year_index_dict = {}

        for subr in subr_ls:
            print("  from subreddit", subr)
            csv_fls = os.listdir(ROOT_PATH + "/data/" + token_folder + "/" + str(year) + "/" + subr)
            year_index_dict[subr] = csv_fls

            for csvf in csv_fls:
                csv_fpath = ROOT_PATH + "/data/" + token_folder + "/" + str(year) + "/" + subr + "/" + csvf
                tokens = read_csv(csv_fpath)
                if len(tokens) >= min_len:
                    year_token_chunk.append(tokens)

        print("saving directory tree...")
        write_json(year_index_dict, year)

    return year_token_chunk


def build_model(year, topwords_folder, model_folder, year_token_chunk, num_topics, num_iterations, write_output=False, num_words=50):

    print("building model...")
    print("- number of documents:", len(year_token_chunk))
    print("- number of topics:", num_topics)

    build_dict = Dictionary(year_token_chunk)
    build_corpus = [build_dict.doc2bow(text) for text in year_token_chunk]
    malletmodel = ldamallet.LdaMallet(PATH_TO_MALLET, build_corpus, num_topics=num_topics, id2word=build_dict, iterations=num_iterations)

    malletmodel.save(model_folder + "/MALLET-k-" + str(num_topics) + "-" + str(year) + ".model")

    # evaluate the model
    coherence = CoherenceModel(model=malletmodel, texts=year_token_chunk, corpus=build_corpus, dictionary=build_dict, coherence="c_v")
    coh_score = coherence.get_coherence()
    print("- coherence score:", coh_score)

    ## write output
    if write_output:

        print("writing output...")
        x = malletmodel.show_topics(num_topics=num_topics, num_words=num_words, formatted=False)

        topicwords = [(tp[0], [(wd[0], wd[1]) for wd in tp[1]]) for tp in x]

        # topicsfile = open("output/{root_output_dir}/topic-words-" + str(year) + "k-" + str(num_topics) + ".csv", "w")

        topic_df = pd.DataFrame()

        for t in range(num_topics):

            topic = topicwords[t][0]
            words = topicwords[t][1]
            sorted_words = sorted(words, key=lambda x: x[1], reverse=True)
            topic_df["topic_" + str(topic)] = sorted_words

        topic_df.to_csv(topwords_folder + "/topic-words-" + str(year) + "k-" + str(num_topics) + ".csv", index=False)

    return coh_score



if __name__ == "__main__":

    argparser = argparse.ArgumentParser()

    argparser.add_argument("--token_folder", help="Name of the token folder", type=str, default="topic-tok", required=False)
    argparser.add_argument("--start_year", help="Start parsing from this year", type=int, default=2012, required=False)
    argparser.add_argument("--end_year", help="Start parsing from this year", type=int, default=2021, required=False)
    argparser.add_argument("--doc_min_len", help="Minimum of doc length required", type=int, default=10, required=False)

    args = argparser.parse_args()

    token_folder = args.token_folder
    start_year = args.start_year
    end_year = args.end_year
    min_doc_len = args.doc_min_len

    root_topwords_dir = "MALLET-topic-words-small"
    root_model_dir = "MALLET-model-cache-small"

    try: os.mkdir(ROOT_PATH + "/output")
    except: pass

    try: os.mkdir(ROOT_PATH + "/index")
    except: pass

    try: os.mkdir(ROOT_PATH + "/output/" + root_topwords_dir)
    except: pass

    try: os.mkdir(ROOT_PATH + "/output/" + root_model_dir)
    except: pass

    coherence_dict = {}

    for year in [*range(start_year, end_year)]:

        # collect a token chunk
        print("importing tokens...")
        year_token_chunk = import_tokens(token_folder, year, min_len=min_doc_len)

        coh_year = {}
        for i in range(30, 85, 5):
        # for i in range(5, 10, 1):
            print("""
        ******* ******* ******* ******* *******
          Fitting MALLET topic model sets
          year: {} | # topics: {}
        ******* ******* ******* ******* *******
            """.format(year, i))

            # create a separate output folder
            topwords_folder = ROOT_PATH + "/output/" + root_topwords_dir + "/model-" + str(year)
            model_folder = ROOT_PATH + "/output/" + root_model_dir + "/model-" + str(year)
            try: os.mkdir(topwords_folder)
            except: pass
            try: os.mkdir(model_folder)
            except: pass

            # # collect a token chunk
            # print("importing tokens...")
            # year_token_chunk = import_tokens(token_folder, year, min_len=min_doc_len)

            # build the model
            coh_score = build_model(year, topwords_folder, model_folder, year_token_chunk, i, num_iterations=200, write_output=True, num_words=200)

            coh_year["topic_"+str(i)] = coh_score
        coherence_dict[year] = coh_year
        print("coherence:", coherence_dict)

    print("coherence scores:", coherence_dict)
    with open("output/" + root_topwords_dir + "/coherence-summary.json", "w") as jsonf:
        json.dump(coherence_dict, jsonf)

    print("saved in output/" + root_topwords_dir + "/coherence-summary.json")






# test run
# python3 mallet_per_year.py --token_folder="subm-tok-small" --start_year=2012 --end_year=2021
# python3 mallet_per_year.py --token_folder="subm-tok-small" --start_year=2012 --end_year=2014


# save model: https://stackoverflow.com/questions/17354417/gensim-how-to-save-lda-models-produced-topics-to-a-readable-format-csv-txt-et

# python3 mallet_per_year.py --token_folder="subm-tok-small" --start_year=2019 --end_year=2020
