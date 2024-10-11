import apache_beam as beam
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
import json
import argparse

class TransformSalesData(beam.DoFn):
    def process(self, element):
        values = element.split(',')
        obj = {
            'date': values[0],
            'product_id': values[1],
            'product_name': values[2],
            'category': values[3],
            'quantity': int(values[4]),
            'unit_price': float(values[5]),
            'total_sales': float(values[6])
        }
        
        # Add any additional transformations here
        obj['year'] = obj['date'].split('-')[0]
        obj['month'] = obj['date'].split('-')[1]
        
        return [obj]

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True, help='Input file pattern')
    parser.add_argument('--output', required=True, help='Output BigQuery table')
    parser.add_argument('--temp_location', required=True, help='GCS Temp directory')
    known_args, pipeline_args = parser.parse_known_args(argv)

    schema_json = json.dumps({
        "fields": [
            {"name": "date", "type": "DATE"},
            {"name": "product_id", "type": "STRING"},
            {"name": "product_name", "type": "STRING"},
            {"name": "category", "type": "STRING"},
            {"name": "quantity", "type": "INTEGER"},
            {"name": "unit_price", "type": "FLOAT"},
            {"name": "total_sales", "type": "FLOAT"},
            {"name": "year", "type": "STRING"},
            {"name": "month", "type": "STRING"}
        ]
    })

    p = beam.Pipeline(argv=pipeline_args)

    (p
     | 'Read CSV' >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)
     | 'Transform' >> beam.ParDo(TransformSalesData())
     | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
         known_args.output,
         schema=parse_table_schema_from_json(schema_json),
         create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
         write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
         custom_gcs_temp_location=known_args.temp_location)
    )

    p.run()

if __name__ == '__main__':
    run()