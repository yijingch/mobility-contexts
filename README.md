# mobility-contexts

## Pre-processing / Analysis Documentations

##### Code files:

1. `crawl_reddit_fixstart_by_year.py`
- crawl submissions and comments from subreddits listed in `index/left-right-labels.csv` within a specified time interval into
- e.g., `python3 aggr_per_subm_year.py --start_year=2012 --end_year=2021`

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


##### Index/descriptive files:

1. `index/left-right-labels.csv`: a list of subreddits we crawl contents from
2. `/index/descriptives-from-2012.csv`: number of submissions/comments crawled from each subreddit in each year
