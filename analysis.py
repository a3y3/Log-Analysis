#!/usr/bin/env python3
import time
import psycopg2


def execute(query):
    """
    Executes a given query. Note that the function will re-use the cursor
    defined in main.
    :param query: a query that needs to be executed by the cursor.
    :return: the result of the query using cursor.fetchall(), and the time
    taken to execute the query.
    """
    db = psycopg2.connect(database="news")
    c = db.cursor()

    t_start = time.time()
    c.execute(query)
    values = c.fetchall()
    t_finish = time.time()

    t_first = t_finish - t_start
    db.close()
    return values, t_first


def get_top_articles():
    """
    Groups the view on the basis of title and calculates the count.
    :return: the executed result (table of title and count)
    """
    print("What are the most popular three articles of all time?")
    query = """
            select title, count(path) as count
            from article_counts
            group by title
            order by count desc
            limit 3;
            """

    return execute(query)


def get_top_authors():
    """
    Joins the view (article_counts) and authors, groups on the basis of names,
    and counts the
    number of titles by each author.
    :return: result of executed query (name and count)
    """
    print("Who are the most popular article authors of all time?")
    query = """
            select name, count(*)
            from authors join article_counts
            on authors.id = article_counts.author
            group by name
            order by count(*) DESC;
            """
    return execute(query)


def get_days_with_1_percent_errors():
    """
    Joins two predefined views and calculates percentage errors as
    total_errors/total_requests
    (where total_errors and total_requests are columns in the views)
    :return: result in the form of day and percent errors.
    """
    print("On which days did more than 1% of requests lead to errors?")
    query = """
            select total_req.day, (total_errors::decimal/total_requests*100)
            as percent
            from total_req join total_req_err
            on total_req.day = total_req_err.day
            where total_errors::decimal/total_requests*100 >= 2;
            """
    return execute(query)


if __name__ == '__main__':

    answer1, time1 = get_top_articles()
    for a in answer1:
        print("\t\"{}\" - {} views".format(a[0], a[1]))
    print("Time taken:", round(time1, 2), "seconds.\n")

    answer2, time2 = get_top_authors()
    for a in answer2:
        print("\t{} - {} views".format(a[0], a[1]))
    print("Time taken:", round(time2, 2), "seconds.\n")

    answer3, time3 = get_days_with_1_percent_errors()
    for a in answer3:
        print("\t{} - {}% errors".format(a[0], round(a[1], 2)))
    print("Time taken:", round(time3, 2), "seconds.\n")
