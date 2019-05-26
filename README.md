# Log Analysis

### Overview
The project served as a basis to understand querying and reporting from a real world server databse containing logs spanning more than a million rows. 3 questions, in particular, were answered from 3 tables:
- What are the most popular three articles of all time?
- Who are the most popular article authors of all time?
- On which days did more than 1% of requests lead to errors?

## How to build
### Pre-requisites
You will need:
- PostgreSQL
- Python3
- The database located [here](https://d17h27t6h515a5.cloudfront.net/topher/2016/August/57b5f748_newsdata/newsdata.zip)

### Steps to build
- Extract the zip to find `newsdata.sql`
- Create the database using `CREATE DATABASE news;`
- Fill the database using `psql -d news -f newsdata.sql`
- Create views by typing these directly into your psql console. You'll need to create 3 views:

  1.  ```
      create view article_counts as 
      select author, title, slug, path 
      from articles left join log 
      on log.path = '/article/'||slug;
      ```
      
  2. ```
     create view total_req as
     select time::date as day, count(*) as total_requests
     from log
     group by day;
     ```
  3. ```
     create view total_req_err as
     select time::date as day, count(*) as total_errors
     from log
     where log.status like '%404%'
     group by day;
     ```
 - Run the analysis using `python3 analysis.py` to see the output on terminal.
