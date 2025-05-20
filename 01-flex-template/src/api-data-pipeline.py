import apache_beam as beam
import requests
 
# Define a custom DoFn to extract the specified fields from nested JSON
class ExtractFields(beam.DoFn):
    def __init__(self, fields_to_extract):
        self.fields_to_extract = fields_to_extract
 
    def process(self, element):
        import json
 
        try:
            # creating a list of fields to extract from string
            fields_to_extract = self.fields_to_extract.split(",")
 
            extracted_data = {}
            for field in fields_to_extract:
                # Split the field name by '.' to navigate nested structures
                field_parts = field.split(".")
                current_data = element
                for part in field_parts:
                    if part in current_data:
                        current_data = current_data[part]
                    else:
                        # Field not found, skip this field
                        current_data = None
                        break
 
                if current_data is not None:
                    extracted_data[field] = current_data
 
            if extracted_data:
                yield extracted_data
        except (json.JSONDecodeError, ValueError) as e:
            # Handle JSON decoding errors here
            pass
 
class AddDatetimeAndDate(beam.DoFn):
    def process(self, element):
        from datetime import datetime
 
        new_element = dict(element)  # Create a copy of the original dictionary
 
        current_time = datetime.now()
 
        # Add a DATETIME column with the current timestamp
        new_element["load_datetime"] = current_time
        new_element["load_date"] = current_time.date()
 
        yield new_element  # Emit the new dictionary with the added columns
 
fields_to_extract = "id,code"
 
with beam.Pipeline() as p:
 
    json_data = (
        p | "Read API" >> beam.Create(["https://fantasy.premierleague.com/api/fixtures/?event=4"])
        | "HTTP GET" >> beam.ParDo(lambda url: requests.get(url).json())
        | "Extract Fields" >> beam.ParDo(ExtractFields(fields_to_extract))
        | "AddDatetimeAndDate" >> beam.ParDo(AddDatetimeAndDate())        
        | "Print" >> beam.Map(print)
    )
 
p.run()
 
# fields_to_extract = "name"
 
# with beam.Pipeline() as p:
 
#     json_data = (
#         p | "Read API" >> beam.Create(["https://api.punkapi.com/v2/beers"])
#         | "HTTP GET" >> beam.ParDo(lambda url: requests.get(url).json())
#         | "Extract Fields" >> beam.ParDo(ExtractFields(fields_to_extract))
#         # | "AddDatetimeAndDate" >> beam.ParDo(AddDatetimeAndDate())        
#         | "Print" >> beam.Map(print)
#     )
 
# p.run()