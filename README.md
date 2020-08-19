# mobility-contexts

## Pre-processing / Analysis Documentations

CODE FILES:

1. `crawl_reddit_fixstart_by_year.py`
- Action: crawl submissions and comments from subreddits listed in `index/left-right-labels.csv` within a specified time interval into
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

2. 
