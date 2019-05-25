import time

import psycopg2

view = "create view article_counts as \
select author, title, slug, path \
from articles left join log \
on log.path like '%'||articles.slug||'%';"


def execute(query):
    t_start = time.time()
    c.execute(query)
    values = c.fetchall()
    t_finish = time.time()

    t_first = t_finish - t_start
    return values, t_first


def print_answer(answer, time):
    for a in answer:
        print("\t\"{}\" - {}".format(a[0], a[1]))
    print("Time taken:", time, "\n")


def solve_question1():
    """
    Groups the view on the basis of title and calculates the count.
    :return: the executed result (table of title and count)
    """
    print("What are the most popular three articles of all time?")
    query = """select title, count(path) as count 
                from article_counts 
                group by title 
                order by count desc 
                limit 3;"""

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
    pass


if __name__ == '__main__':
    db = psycopg2.connect(database="news")
    c = db.cursor()

    answer1, time1 = solve_question1()
    print_answer(answer1, time1)

    answer2, time2 = solve_question2()
    print_answer(answer2, time2)
