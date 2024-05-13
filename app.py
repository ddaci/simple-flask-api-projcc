from flask import Flask, request, jsonify
from google.cloud import bigquery
from google.cloud import exceptions

app = Flask(__name__)




#Ruta pentru datele BigQuery face acelasi lucru ca si  A route to return all of available entries in our catalog sqlite
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

@app.errorhandler(404)
def page_not_found(e):
    return "<h1>404</h1><p>The resource could not be found</p>", 404


@app.route('/api/v2/resources/books', methods=['GET'])
def api_filter():
    client = bigquery.Client()
    query_parameters = request.args

    id = query_parameters.get('id')
    published = query_parameters.get('published')
    author = query_parameters.get('author')

    # Construiește interogarea BigQuery
    base_query = 'SELECT * FROM `proiectcc-419616.datasetcarti.carti` WHERE'
    query_conditions = []
    params = []

    if id:
        query_conditions.append('id = @id')
        params.append(bigquery.ScalarQueryParameter('id', 'STRING', id))

    if published:
        query_conditions.append('published = @published')
        params.append(bigquery.ScalarQueryParameter('published', 'STRING', published))

    if author:
        query_conditions.append('author = @author')
        params.append(bigquery.ScalarQueryParameter('author', 'STRING', author))

    # Verifică dacă există condiții de adăugat la interogare
    if not query_conditions:
        return page_not_found(404)

    # Finalizează construcția interogării cu condițiile specifice
    final_query = f"{base_query} {' AND '.join(query_conditions)} LIMIT 10;"
    job_config = bigquery.QueryJobConfig(query_parameters=params)

    try:
        query_job = client.query(final_query, job_config=job_config)
        results = query_job.result()
        books = [dict(row) for row in results]
        return jsonify(books)
    except exceptions.GoogleCloudError as e:
        return jsonify({"error": str(e)}), 500
    except Exception as e:
        return jsonify({"error": "An unexpected error occurred"}), 500

# A method that runs the application server.
if __name__ == "__main__":
    # Threaded option to enable multiple instances for multiple user access support
    app.run(debug=False, threaded=True, port=5000)