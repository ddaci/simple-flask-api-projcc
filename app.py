from flask import Flask, request, jsonify
import sqlite3
import os
from google.cloud import bigquery

# Init app
app = Flask(__name__)

"""
#functie pentru formatarea rezultatelor interogarii bazei de date SQQLite-nu avem nevoie pt gcp si bigquery
def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d
"""


# Flask maps HTTP requests to Python functions.
# The process of mapping URLs to functions is called routing.

# Ruta pentru datele BigQuery face acelasi lucru ca si  A route to return all of available entries in our catalog sqlite
@app.route('/api/v2/resources/bigquery-data', methods=['GET'])
def get_bigquery_data():
    client = bigquery.Client()

    # Replace `your-project`, `your-dataset`, and `your-table` with your actual project ID, dataset name, and table name.
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


"""
# A route to return all of available entries in our catalog.
@app.route('/api/v2/resources/books/all', methods=['GET'])
def api_all():
    db_path = os.path.join('db', 'books.db')    
    conn = sqlite3.connect(db_path)
    # returns items from the database as dictionaries rather than lists
    conn.row_factory = dict_factory
    cur = conn.cursor()
    all_books = cur.execute('SELECT * FROM books;').fetchall()

    return jsonify(all_books)
"""


@app.errorhandler(404)
def page_not_found(e):
    return "<h1>404</h1><p>The resource could not be found</p>", 404


"""
Această rută dintr-o aplicație Flask definește un endpoint API care 
permite utilizatorilor să filtreze și să caute cărți într-o bază de date 
pe baza unor parametri specifici, cum ar fi id, published (data publicării), 
și author (autor). Dacă nu sunt furnizați parametri sau dacă combinația de
parametri nu returnează niciun rezultat, se returnează un răspuns 404 Not Found.

@app.route('/api/v2/resources/books', methods=['GET'])
def api_filter():
    query_parameters = request.args

    id = query_parameters.get('id')
    published = query_parameters.get('published')
    author = query_parameters.get('author')

    query = 'SELECT * FROM books WHERE'
    to_filter = []

    if id:
        query += ' id=? AND'
        to_filter.append(id)

    if published:
        query += ' published=? AND'
        to_filter.append(published)

    if author:
        query += ' author=? AND'
        to_filter.append(author)

    if not(id or published or author):
        return page_not_found(404)

    query = query[:-4] + ';'

    db_path = os.path.join('db', 'books.db')    
    conn = sqlite3.connect(db_path)
    conn.row_factory = dict_factory
    cur = conn.cursor()

    results = cur.execute(query, to_filter).fetchall()

    return jsonify(results)
"""


@app.route('/api/v2/resources/books', methods=['GET'])
def api_filter():
    client = bigquery.Client()

    query_parameters = request.args
    id = query_parameters.get('id')
    published = query_parameters.get('published')
    author = query_parameters.get('author')

    # Construiește interogarea BigQuery
    query = 'SELECT * FROM proiectcc-419616.datasetcarti.carti WHERE'
    conditions = []

    if id:
        conditions.append(f"id = '{id}'")

    if published:
        conditions.append(f"published = '{published}'")

    if author:
        conditions.append(f"author = '{author}'")

    if not conditions:
        return page_not_found(404)

    query += ' AND '.join(conditions) + ' LIMIT 10;'

    query_job = client.query(query)
    results = query_job.result()

    # Convertirea rezultatelor într-o listă de dicționare pentru a le putea serializa ca JSON
    books = [dict(row) for row in results]

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