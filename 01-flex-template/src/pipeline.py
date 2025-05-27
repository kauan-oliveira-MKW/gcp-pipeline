import argparse
import apache_beam as beam

from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class CustomPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
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


class FetchAPIData(beam.DoFn):
    def __init__(self, api_endpoint):
        self.api_endpoint = api_endpoint

    def process(self, unused_element):
        import requests

        url = self.api_endpoint.get()
        if url is not None:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                for item in data:
                    yield item


# Extrai campos específicos de cada elemento JSON
class ExtractFields(beam.DoFn):
    def __init__(self, fields_to_extract):
        self.fields_to_extract = fields_to_extract

    def process(self, element):
        try:
            fields_to_extract = self.fields_to_extract.get().split(",")
            extracted_data = {}

            for field in fields_to_extract:
                field_parts = field.split(".")
                current_data = element
                for part in field_parts:
                    if part in current_data:
                        current_data = current_data[part]
                    else:
                        current_data = None
                        break

                if current_data is not None:
                    extracted_data[field] = current_data

            if extracted_data:
                yield extracted_data
        except Exception as e:
            pass


# Adiciona timestamp atual
class AddDatetimeAndDate(beam.DoFn):
    def process(self, element):
        from datetime import datetime

        new_element = dict(element)
        current_time = datetime.utcnow()

        new_element["load_datetime"] = str(current_time)
        new_element["load_date"] = str(current_time.date())

        yield new_element


def run(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    custom_options = pipeline_options.view_as(CustomPipelineOptions)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Start pipeline" >> beam.Create([None])
            | "Fetch API Data" >> beam.ParDo(FetchAPIData(custom_options.apiEndpoint))
            | "Extract Fields" >> beam.ParDo(ExtractFields(custom_options.fieldsToExtract))
            | "Add Timestamp" >> beam.ParDo(AddDatetimeAndDate())
            | "Write to BigQuery"
            >> beam.io.WriteToBigQuery(
                table=lambda element, dataset=custom_options.dataset, table=custom_options.table:
                    f"{dataset.get()}.{table.get()}",
                schema="SCHEMA_AUTODETECT",
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                custom_gcs_temp_location=custom_options.custom_gcs_temp_location,
            )
        )


if __name__ == "__main__":
    run()
