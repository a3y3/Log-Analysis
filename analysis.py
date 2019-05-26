import time
import psycopg2

article_counts = """
        create view article_counts as 
        select author, title, slug, path 
        from articles left join log 
        on log.path like '%'||articles.slug||'%';
        """
total_req = """
            create view total_req as
            select time::date as day, count(*) as total_requests
            from log
            group by day;
            """
total_req_err = """
                create view total_req_err as
                select time::date as day, count(*) as total_errors
                from log
                where log.status like '%404%'
                group by day;
                """


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


def print_answer(answer, time):
    """
    Prints the answer and time required in an easy to read format
    :param answer: the database answer to be printed
    :param time: the time it took for the answer to be generated
    :return: None
    """
    for a in answer:
        print("\t\"{}\" - {}".format(a[0], a[1]))
    print("Time taken:", time, "seconds.\n")


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
    print_answer(answer1, time1)

    answer2, time2 = solve_question2()
    print_answer(answer2, time2)

    answer3, time3 = solve_question3()
    print_answer(answer3, time3)

    db.close()
