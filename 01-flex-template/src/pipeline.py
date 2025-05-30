import argparse
import logging
import requests
import apache_beam as beam
 
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
 
 
class CustomPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # # Set the required arguments
        parser.add_value_provider_argument(
            "--apiEndpoint", help="API Endpoint for the data", required=True
        )
        parser.add_value_provider_argument(
            "--fieldsToExtract",
            help="Fields to extract from the JSON response",
            required=True,
        )
        parser.add_value_provider_argument(
            "--custom_gcs_temp_location",
            help="GCS Temp location for Dataflow",
            required=True,
        )
        parser.add_value_provider_argument(
            "--dataset", help="BigQuery Dataset", required=True
        )
        parser.add_value_provider_argument(
            "--table", help="BigQuery Table", required=True
        )
        parser.add_value_provider_argument(
            "--eventNumber", help="Event number (round) of the data",required=True,
        )
 
# Classe para extrair campos, inclusive de listas
class ExtractFields(beam.DoFn):
    def __init__(self, fields_to_extract):
        self.fields_to_extract = fields_to_extract

    def process(self, element):
        logging.info(f"📦 [ExtractFields] Processing element: {element}")
        fields_to_extract = self.fields_to_extract.split(",")
        extracted_data = {}

        for field in fields_to_extract:
            value = self.get_value(element, field.split("."))
            extracted_data[field] = value
            logging.debug(f"🔍 [ExtractFields] Field: {field} -> Value: {value}")

        logging.info(f"✅ [ExtractFields] Extracted data: {extracted_data}")
        yield extracted_data

    def get_value(self, data, field_parts):
        if data is None:
            return None

        current = data
        for part in field_parts:
            if isinstance(current, list):
                return [self.get_value(item, [part] + field_parts[1:]) for item in current]
            elif isinstance(current, dict):
                current = current.get(part)
            else:
                return None
        return current

class AddEventNumber(beam.DoFn):
    def __init__(self, event_number):
        self.event_number = event_number

    def process(self, element):
        new_element = dict(element)
        new_element["event_number"] = self.event_number.get()
        yield new_element

# Classe nova para tratar campos complexos (como listas)
class TransformComplexFields(beam.DoFn):
    def process(self, element):
        import json
        logging.info(f"🔧 [TransformComplexFields] Processing: {element}")

        transformed = dict(element)

        for key, value in transformed.items():
            if isinstance(value, (list, dict)):
                try:
                    transformed[key] = json.dumps(value, ensure_ascii=False)
                    logging.debug(f"🧠 [TransformComplexFields] Serialized {key}: {transformed[key]}")
                except Exception as e:
                    logging.error(f"❌ [TransformComplexFields] Error serializing {key}: {e}")
                    transformed[key] = str(value)

        logging.info(f"✅ [TransformComplexFields] Result: {transformed}")
        yield transformed


# Classe para adicionar datetime e date
class AddDatetimeAndDate(beam.DoFn):
    def process(self, element):
        from datetime import datetime

        logging.info(f"⏰ [AddDatetimeAndDate] Processing: {element}")
        new_element = dict(element)
        current_time = datetime.utcnow()

        new_element["load_datetime"] = current_time.isoformat()
        new_element["load_date"] = current_time.date().isoformat()

        logging.info(f"✅ [AddDatetimeAndDate] Result with dates: {new_element}")
        yield new_element


# Função que faz o request na API
def fetch_api(endpoint):
    logging.info(f"🌐 [FetchAPI] Requesting API: {endpoint}")
    response = requests.get(endpoint)
    if response.status_code == 200:
        data = response.json()
        logging.info(f"✅ [FetchAPI] Success. Data length: {len(data) if isinstance(data, list) else '1'}")
        return data
    else:
        logging.error(f"❌ [FetchAPI] Failed with status {response.status_code}")
        return []


# Função principal do pipeline
def run(argv=None):
    from apache_beam.options.pipeline_options import PipelineOptions

    class CustomOptions(PipelineOptions):
        @classmethod
        def _add_argparse_args(cls, parser):
            parser.add_argument("--apiEndpoint", type=str, required=True)
            parser.add_argument("--fieldsToExtract", type=str, required=True)
            parser.add_argument("--dataset", type=str, required=True)
            parser.add_argument("--table", type=str, required=True)
            parser.add_argument("--custom_gcs_temp_location", type=str, required=True)

    pipeline_options = PipelineOptions(argv)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    custom_options = pipeline_options.view_as(CustomOptions)

    logging.getLogger().setLevel(logging.INFO)

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Read API" >> beam.Create([custom_options.apiEndpoint.get()])
            | "HTTP GET" >> beam.ParDo(lambda url: requests.get(url).json())
            | "Extract Fields" >> beam.ParDo(ExtractFields(custom_options.fieldsToExtract.get()))
            | "Add Event Number" >> beam.ParDo(AddEventNumber(custom_options.eventNumber))
            | "Add Datetime and Date" >> beam.ParDo(AddDatetimeAndDate())
            | "Write to BigQuery"
            >> beam.io.WriteToBigQuery(
                table=custom_options.table.get(),
                dataset=custom_options.dataset.get(),
                schema="SCHEMA_AUTODETECT",
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                custom_gcs_temp_location=custom_options.custom_gcs_temp_location.get(),
            )
        )
 
    # Run the pipeline
    result = p.run()
    result.wait_until_finish()
 
 
if __name__ == "__main__":
    run()