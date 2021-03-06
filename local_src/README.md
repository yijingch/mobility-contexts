# mobility-contexts - local codes documentation

## Pre-processing / Analysis Documentations


##### Data files:

For privacy conerns we don't publish data files in this public repository. Feel free to reach out to yijingch at umich dot edu if you have any inquiries.

##### Code files:

1. `crawl_reddit_fixstart_by_year.py`
- crawl submissions and comments from subreddits listed in `index/left-right-labels.csv` within a specified time interval into
- e.g., `python3 crawl_reddit_fixstart_by_year.py --start_year=2012 --end_year=2021`
- JSON data sample: `data-snippets/subm_neoliberal_2012.json`

-------------------------------------------------
    output folder structure:
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


2. `reset_db.py` + `parse_json_to_db.py`
- parse relevant data points into a SQLite database
-------------------------------------------------
    CREATE TABLE "submission" (
          "id" TEXT PRIMARY KEY,
          "author" TEXT,
          "author_fullname" TEXT,
          "subreddit" TEXT,
          "title" TEXT,
          "selftext" TEXT,
          "score" INTEGER,
          "created_utc" TEXT
          )

    CREATE TABLE "comment" (
            "id" TEXT PRIMARY KEY,
            "author" TEXT,
            "author_fullname" TEXT,
            "subreddit" TEXT,
            "parent_id" TEXT,
            "link_id" TEXT,
            "body" TEXT,
            "score" INTEGER,
            "created_utc" TEXT
            )
-------------------------------------------------

3. `aggr_per_subm_year.py`
- aggregate documents by submission by year
- input folder: `/data/raw/comment-from-2012`, `/data/raw/submission-from-2012`
- this version skipped four large subreddits `["politics", "the_donald", "chapotraphouse", "neoliberal"]`
- txt data sample: `data-snippets/anarchism_zzx32_2012.txt`

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

4. `doc_clean_tokenize.py`
- clean and tokenize documents
  1. cleaned: URL, non-english characters, user and subreddit name, nltk stopwords
  2. limited vocab size, i.e., only kept top frequency words for each subreddit in each year (default retaining percentage: 80%)
  3. break strings into pieces and stored in .csv files
- csv data sample: `data-snippets/anarchocommunism_adtetf_2019.csv`

-------------------------------------------------
    output folder structure:
      data
        |--- topic-token
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


5. `mallet_per_year.py`
- train MALLET topic model on a number of documents (min length specified)


##### Index/descriptive files:

1. `index/left-right-labels.csv`: a list of subreddits we crawl contents from
2. `/index/descriptives-from-2012.csv`: number of submissions/comments crawled from each subreddit in each year
