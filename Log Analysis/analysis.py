import time

import psycopg2

view = "create view article_counts as \
select author, title, slug, path \
from articles left join log \
on log.path like '%'||articles.slug||'%';"


def solve_question1():
    """
    Groups the view on the basis of title and calculates the count.
    :return: the executed result (table of title and count)
    """
    print("What are the most popular three articles of all time?")
    query = "select title, count(path) as count \
                from article_counts \
                group by title \
                order by count desc \
                limit 3;"

    t_start = time.time()
    c.execute(query)
    values = c.fetchall()
    t_finish = time.time()

    t_first = t_finish - t_start
    return values, t_first


def solve_question2():
    """

    :return:
    """
    return None


def solve_question3():
    pass


if __name__ == '__main__':
    db = psycopg2.connect(database="news")
    c = db.cursor()

    answer1, time1 = solve_question1()

    for answer in answer1:
        print("\t{} - {}".format(answer[0], answer[1]))
    print("\nDone. Time taken for first query:", time1)
