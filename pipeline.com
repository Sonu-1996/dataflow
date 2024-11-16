import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToBigQuery
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions
import csv

# BigQuery schema definition
BQ_SCHEMA = {
    'fields': [
        {'name': 'id', 'type': 'STRING'},
        {'name': 'name', 'type': 'STRING'},
        {'name': 'age', 'type': 'STRING'}
    ]
}

# Transformation function to extract and process the CSV row data
def process_csv_row(row):
    # Assuming 'row' is a CSV line that needs to be split
    csv_columns = row.split(',')
    
    # Apply transformation: Extract the necessary columns (id, name, age)
    return {
        'id': csv_columns[0],  # id column
        'name': csv_columns[1],  # name column
        'age': csv_columns[2],   # age column
    }

def run_pipeline():
    # Pipeline options (adjust with your GCP project and settings)
    options = PipelineOptions(
        flags=None,
        project='your-gcp-project-id',  # replace with your Google Cloud project ID
        temp_location='gs://your-bucket-name/temp/',  # temp GCS location for intermediate files
        region='us-central1',
        runner='DataflowRunner'  # or use 'DirectRunner' for local execution
    )

    with beam.Pipeline(options=options) as p:
        # Read CSV file from GCS
        rows = (
            p
            | 'Read CSV' >> ReadFromText('gs://your-bucket-name/path/to/input.csv')  # Update path to GCS CSV
            | 'Parse CSV and Transform Data' >> beam.Map(process_csv_row)  # Apply transformation function
        )

        # Write the processed data to BigQuery
        (
            rows
            | 'Write to BigQuery' >> WriteToBigQuery(
                'your-project-id:your_dataset.your_table',  # BigQuery table reference
                schema=BQ_SCHEMA,  # Provide schema for BigQuery
                write_disposition=BigQueryDisposition.WRITE_APPEND  # You can change this based on your needs
            )
        )

if __name__ == '__main__':
    run_pipeline()

