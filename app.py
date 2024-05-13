from flask import Flask, request, jsonify
import sqlite3
import os
from google.cloud import bigquery
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
# Init app
app = Flask(__name__)

# Flask maps HTTP requests to Python functions.
# The process of mapping URLs to functions is called routing.
# Ruta pentru datele BigQuery face acelasi lucru ca si  A route to return all of available entries in our catalog sqlite
@app.route('/api/v2/resources/bigquery-data', methods=['GET'])
def get_bigquery_data():
    client = bigquery.Client()

    query = """
        SELECT * FROM `proiectcc-419616.datasetcarti.carti`
        LIMIT 10
    """
    query_job = client.query(query)  # Make an API request.

    results = query_job.result()  # Waits for the query to finish

    # Convert the results to a list of dictionaries to jsonify it later
    rows = [dict(row) for row in results]

    return jsonify(rows)


@app.route('/', methods=['GET'])
def home():
    return "<h1>:)</h1><p>This is a prototype API</p>"



@app.errorhandler(404)
def page_not_found(e):
    return "<h1>404</h1><p>The resource could not be found</p>", 404

@app.route('/api/v2/resources/books', methods=['GET'])
def api_filter():
    client = bigquery.Client()
    # Assuming 'published' year is now handled as an integer
    published = 1988
    logger.info(f"Executing query: {query}")
    # Correct query with parameter for type safety
    query = """
        SELECT * FROM `proiectcc-419616.datasetcarti.carti`
        WHERE published = @published
        LIMIT 10;
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("published", "INT64", published)
        ]
    )

    query_job = client.query(query, job_config=job_config)  # API request with parameters
    results = query_job.result()  # Waits for the query to finish

    books = [dict(row) for row in results]  # Convert the results to a list of dictionaries to jsonify it later

    return jsonify(books)

"""
@app.route('/api/v2/resources/books', methods=['POST'])
def add_book():

    # Receives the data in JSON format in a HTTP POST request
    if not request.is_json:
        return "<p>The content isn't of type JSON<\p>"

    content = request.get_json()
    title = content.get('title')
    author = content.get('author')
    published = content.get('published')
    first_sentence = content.get('first_sentence')

    # Save the data in db
    db_path = os.path.join('db', 'books.db')    
    conn = sqlite3.connect(db_path)
    query = f'INSERT INTO books (title, author, published, first_sentence) \
              VALUES ("{title}", "{author}", "{published}", "{first_sentence}");'

    cur = conn.cursor()
    cur.execute(query)
    conn.commit()

    return jsonify(request.get_json())

"""
# A method that runs the application server.
if __name__ == "__main__":
    # Threaded option to enable multiple instances for multiple user access support
    app.run(debug=False, threaded=True, port=5000)