# Code based on Apache Beam's Programming Guide: https://beam.apache.org/documentation/programming-guide/ 
from __future__ import absolute_import

import argparse
import logging
import json
import ast

from past.builtins import unicode

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam import pvalue
from apache_beam.examples.wordcount import WordExtractingDoFn
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions

class Split(beam.DoFn):
    '''
    Inherited from beam's DoFn class
    Logic to conditionally split the incoming payload and produce different tagged PValues.
    '''

    def process(self, element):
        '''
        Overriding process method of beam's DoFn class
        element - JSON string
        '''
        json_data = json.loads(element) # loading JSON string into a dictionary        
        if json_data["event_name"] == 'super duper event - 1': # Check if event_name matches pre-decided tag
            # Some pre-processing steps here, if needed; Make sure contents of json_data["payload"] matches the row specification of BigQuery table
            yield pvalue.TaggedOutput('super duper', ast.literal_eval(json_data["payload"])) # return a generator object that produces tagged pValues.
        
        # elif json_data["event_name"] == 'super duper event - 2': ... # can add multiple conditions for different event types and return its corresponding generator object

def run(argv=None):
  '''
  Defines pipeline
  '''  
  # Gather command line arguments
  parser = argparse.ArgumentParser()
  parser.add_argument('--input_topic', required=True, help=('Input PubSub topic of the form "projects/<PROJECT>/topics/<TOPIC>".')) # topic to listen to
  known_args, pipeline_args = parser.parse_known_args(argv)

  # defining the pipeline
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True # We use the save_main_session option because one or more DoFn's in this workflow rely on global context (e.g., a module imported at module level).
  pipeline_options.view_as(StandardOptions).streaming = True
  p = beam.Pipeline(options=pipeline_options)

  messages = (p | beam.io.ReadFromPubSub(topic=known_args.input_topic).with_output_types(bytes)) # Read from PubSub PCollection
  lines = messages | 'decode' >> beam.Map(lambda x: x.decode('utf-8')) # Decoding message from bytes to utf-8 text (JSON String)
  payload = lines | beam.ParDo(Split()).with_outputs('super duper') # Applying custom ParDo function using Split class, to conditionally split the PCollection a tagged output
  # payload = lines | beam.ParDo(Split()).with_outputs('super duper', 'super duper 2', ...) # Applying custom ParDo function using Split class, to conditionally split the PCollection into different tagged outputs
  payload['super duper'] | "tf1" >> beam.io.gcp.bigquery.WriteToBigQuery(table='[PROJECT_ID]:[DATASET_NAME].[TABLE_NAME]') # Routing tagged output to its corresponding BigQuery table
  # payload['super duper 2'] | "tf2" >> beam.io.gcp.bigquery.WriteToBigQuery(table='...')
  
  result = p.run() # Execute the pipeline using DirectRunner
  result.wait_until_finish() # The run method is asynchronous. If you d like a blocking execution instead, run your pipeline appending the waitUntilFinish method
  
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()