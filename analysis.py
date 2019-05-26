#!/usr/bin/env python3
import time
import psycopg2


def execute(query):
    """
    Executes a given query. Note that the function will re-use the cursor defined in main.
    :param query: a query that needs to be executed by the cursor.
    :return: the result of the query using cursor.fetchall(), and the time taken to execute the query.
    """
    t_start = time.time()
    c.execute(query)
    values = c.fetchall()
    t_finish = time.time()

    t_first = t_finish - t_start
    return values, t_first


def solve_question1():
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


def solve_question2():
    """
    Joins the view (article_counts) and authors, groups on the basis of names, and counts the
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


def solve_question3():
    """
    Joins two predefined
    :return:
    """
    print("On which days did more than 1% of requests lead to errors?")
    query = """
            select total_req.day, (total_errors::decimal/total_requests*100) as percent
            from total_req join total_req_err 
            on total_req.day = total_req_err.day
            where total_errors::decimal/total_requests*100 >= 2;
            """
    return execute(query)


if __name__ == '__main__':
    db = psycopg2.connect(database="news")
    c = db.cursor()

    answer1, time1 = solve_question1()
    for a in answer1:
        print("\t\"{}\" - {} views".format(a[0], a[1]))
    print("Time taken:", time1, "seconds.\n")

    answer2, time2 = solve_question2()
    for a in answer2:
        print("\t{} - {} views".format(a[0], a[1]))
    print("Time taken:", time2, "seconds.\n")

    answer3, time3 = solve_question3()
    for a in answer3:
        print("\t{} - {}% errors".format(a[0], a[1]))
    print("Time taken:", time3, "seconds.\n")

    db.close()
