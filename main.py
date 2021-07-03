from Database import Database
from google.cloud import bigquery
from google.cloud.bigquery import dataset
from upload import Upload
import os

from flask import Flask, render_template
app = Flask(__name__)

client = bigquery.Client(project='cc-ass1-bigquery')
database = Database(client)

#
# Routes
#
@app.route('/')
@app.route('/index')
def index():

    return render_template('index.html')

@app.route('/results')
def results():
    resultsPartOne = database.PartOne()
    resultsPartTwo = database.PartTwo()
    resultsPartThree = database.PartThree()
    return render_template('results.html', 
                            resultsPartOne=resultsPartOne,
                            resultsPartTwo=resultsPartTwo,
                            resultsPartThree=resultsPartThree)

#
# Initialising datasets (should only call once)
#
def init():
    client = bigquery.Client(project='cc-ass1-bigquery')

    datasetID = 'task2'

    country_classification = 'country_classification'
    gsquarterlySeptember20 = 'gsquarterlySeptember20'
    services_classification = 'services_classification'

    client.create_dataset(datasetID)

    upload = Upload(bigquery, client)

    country_classification_schema = [bigquery.SchemaField("country_code", "STRING"), 
                                    bigquery.SchemaField("country_label", "STRING")]

    gsquarterlySeptember20_schema = [bigquery.SchemaField("time_ref", "INT64"),
                                    bigquery.SchemaField("account", "STRING"),
                                    bigquery.SchemaField("code", "STRING"),
                                    bigquery.SchemaField("country_code", "STRING"),
                                    bigquery.SchemaField("product_type", "STRING"),
                                    bigquery.SchemaField("value", "FLOAT64"),
                                    bigquery.SchemaField("status", "STRING")]

    services_classification_schema = [bigquery.SchemaField("code", "STRING"),
                                    bigquery.SchemaField("service_label", "STRING")]

    upload.ReadAndUpload(os.path.join('data', country_classification + '.csv'),  datasetID, country_classification, country_classification_schema)
    upload.ReadAndUpload(os.path.join('data', gsquarterlySeptember20 + '.csv'), datasetID, gsquarterlySeptember20, gsquarterlySeptember20_schema)
    upload.ReadAndUpload(os.path.join('data', services_classification + '.csv'), datasetID, services_classification, services_classification_schema)

if __name__ == '__main__':
    #init()
    app.run(host='127.0.0.1', port=8181, debug=True)