## --------------------------- ##
## ---- Prepare SQLite DB ---- ##
## --------------------------- ##

import sqlite3

DBNAME = "reddit-db.db"
ROOT_PATH = "/gpfs/accounts/azjacobs_root/azjacobs1/yijingch/reddit-topic-models"
DBPATH = ROOT_PATH + "/data/"

def reset_db():

    # 1. try to create DB
    try:
        conn = sqlite3.connect(DBPATH + DBNAME)
        cur = conn.cursor()
    except Exception as e:
        print("Error creating DB:", e)
        conn.close()


    # 2. drop tables if exists
    try:
        statement = """
        DROP TABLE IF EXISTS "subreddit";
        """
        cur.execute(statement)
        conn.commit()
    except Exception as e:
        print("Error dropping TABLE subreddit:", e)

    try:
        statement = """
        DROP TABLE IF EXISTS "submission";
        """
        cur.execute(statement)
        conn.commit()
    except Exception as e:
        print("Error dropping TABLE submission:", e)

    try:
        statement = """
        DROP TABLE IF EXISTS "comment";
        """
        cur.execute(statement)
        conn.commit()
    except Exception as e:
        print("Error dropping TABLE comment:", e)


    # 3. create new tables
    try:
        statement = """
        CREATE TABLE "subreddit" (
        "id" TEXT PRIMARY KEY,
        "name" TEXT,
        "label" INTEGER
        );
        """
        cur.execute(statement)
        conn.commit()
    except Exception as e:
        print("Error creating TABLE subreddit:", e)

    try:
        statement = """
        CREATE TABLE "submission" (
        "id" TEXT PRIMARY KEY,
        "author" TEXT,
        "author_fullname" TEXT,
        "subreddit" TEXT,
        "title" TEXT,
        "selftext" TEXT,
        "score" INTEGER,
        "created_utc" TEXT
        );
        """
        cur.execute(statement)
        conn.commit()
    except Exception as e:
        print("Error creating TABLE submission:", e)

    try:
        statement = """
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
        );
        """
        cur.execute(statement)
        conn.commit()
    except Exception as e:
        print("Error creating TABLE comment:", e)

if __name__ == "__main__":
    reset_db()
