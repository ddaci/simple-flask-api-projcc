from flask import Flask, request, jsonify
import sqlite3
import os
from google.cloud import bigquery

# Init app
app = Flask(__name__)


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


# Flask maps HTTP requests to Python functions.
# The process of mapping URLs to functions is called routing.
@app.route('/', methods=['GET'])
def home():
    return "<h1>Distant Reading Archive</h1><p>This is a prototype API</p>"

# Endpoint pentru obținerea tuturor cărților din BigQuery
@app.route('/api/v2/resources/bigquery-data', methods=['GET'])
def get_bigquery_data():
    client = bigquery.Client()
    query = """
        SELECT * FROM `proiectcc-419616.datasetcarti.carti`
        LIMIT 10
    """
    query_job = client.query(query)  # Execută interogarea
    results = query_job.result()  # Așteaptă finalizarea interogării

    # Convertirea rezultatelor într-o listă de dicționare pentru a le serializa ca JSON
    rows = [dict(row) for row in results]

    return jsonify(rows)


# Endpoint pentru obținerea cărților din BigQuery grupate dupa autor
@app.route('/api/v2/resources/books/by-author', methods=['GET'])
def get_books_by_author():
    query_parameters = request.args
    author = query_parameters.get('author')

    if not author:
        return "<p>Parametrul 'author' trebuie specificat în URL. Exemplu: /api/v2/resources/books/by-author?author=David Brin</p>", 400

    client = bigquery.Client()
    query = f"""
        SELECT * FROM `projectcloudmasterid.books.books`
        WHERE author = '{author}'
    """
    query_job = client.query(query)
    results = query_job.result()

    rows = [dict(row) for row in results]

    if not rows:
        return jsonify({'message': f'Nu s-au găsit cărți scrise de autorul {author}.'}), 404

    return jsonify(rows)


# Endpoint pentru obținerea cărților din BigQuery grupate dupa an
@app.route('/api/v2/resources/books/by-year', methods=['GET'])
def get_books_by_year():
    query_parameters = request.args
    published_year = query_parameters.get('published_year')

    if not published_year:
        return "<p>Parametrul 'published_year' trebuie specificat în URL. Exemplu: /api/v2/resources/books/by-year?published_year=2005</p>", 400

    client = bigquery.Client()
    query = """
        SELECT * FROM `proiectcc-419616.datasetcarti.carti`
        WHERE CAST(published AS STRING) LIKE @published_year
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("published_year", "STRING", published_year)
        ]
    )
    query_job = client.query(query, job_config=job_config)
    results = query_job.result()

    rows = [dict(row) for row in results]

    if not rows:
        return jsonify({'message': f'Nu s-au găsit cărți publicate în anul {published_year}.'}), 404

    return jsonify(rows)


@app.errorhandler(404)
def page_not_found(e):
    return "<h1>404</h1><p>The resource could not be found</p>", 404





# Endpoint pentru adaugarea unei carti
@app.route('/api/v2/resources/books', methods=['POST'])
def add_book():
    if not request.is_json:
        return "<p>The content isn't of type JSON</p>"

    content = request.get_json()
    title = content.get('title')
    author = content.get('author')
    published = content.get('published')
    first_sentence = content.get('first_sentence')

    client = bigquery.Client()
    table_id = "proiectcc-419616.datasetcarti.carti"

    rows_to_insert = [
        {
            "title": title,
            "author": author,
            "published": published,
            "first_sentence": first_sentence
        }
    ]

    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors == []:
        return jsonify(request.get_json())
    else:
        return jsonify({"errors": errors}), 400



# A method that runs the application server.
if __name__ == "__main__":
    # Threaded option to enable multiple instances for multiple user access support
    app.run(debug=False, threaded=True, port=5000)