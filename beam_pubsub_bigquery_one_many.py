# https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/streaming_wordcount.py

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# https://stackoverflow.com/questions/53285726/conditional-statement-python-apache-beam-pipeline
# python beam_pubsub_bigquery_one_many.py --input_topic 'projects/ornate-lead-227417/topics/sample_topic' --runner DirectRunner

"""A streaming word-counting workflow.
"""

from __future__ import absolute_import

import argparse
import logging
import json

from past.builtins import unicode

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam import pvalue
from apache_beam.examples.wordcount import WordExtractingDoFn
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions

class Split(beam.DoFn):
    
    OUTPUT_TAG_PS1 = 'super_duper_event - 1'
    OUTPUT_TAG_PS2 = 'super_duper_event - 2'
    o1 = [{'string1': 'Ut', 'string2': 'ah', 'int1': 123}, {'string1': 'Cali', 'string2': 'fornia', 'int1': 124}]
    o2 = [{'string1': 'Baja', 'num1': 125}]
    def process(self, element):
        print(json.loads(element))
        json_data = json.loads(element)
        if json_data["event_name"] == self.OUTPUT_TAG_PS1:
            for i in self.o1:
                yield pvalue.TaggedOutput(self.OUTPUT_TAG_PS1, i)
        elif json_data["event_name"] == self.OUTPUT_TAG_PS2:
            for i in self.o2:
                yield pvalue.TaggedOutput(self.OUTPUT_TAG_PS2, i)

def run(argv=None):
  """Build and run the pipeline."""
  parser = argparse.ArgumentParser()
  group = parser.add_mutually_exclusive_group(required=True)
  group.add_argument(
      '--input_topic',
      help=('Input PubSub topic of the form '
            '"projects/<PROJECT>/topics/<TOPIC>".'))
  group.add_argument(
      '--input_subscription',
      help=('Input PubSub subscription of the form '
            '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."'))
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  pipeline_options.view_as(StandardOptions).streaming = True
  p = beam.Pipeline(options=pipeline_options)

  # Read from PubSub into a PCollection.
  if known_args.input_subscription:
    messages = (p
                | beam.io.ReadFromPubSub(
                    subscription=known_args.input_subscription)
                .with_output_types(bytes))
  else:
    messages = (p
                | beam.io.ReadFromPubSub(topic=known_args.input_topic)
                .with_output_types(bytes))
  lines = messages | 'decode' >> beam.Map(lambda x: x.decode('utf-8'))
  quotes = lines | beam.ParDo(Split()).with_outputs(Split.OUTPUT_TAG_PS1, Split.OUTPUT_TAG_PS2)
  
  quotes[Split.OUTPUT_TAG_PS1] | "tf1" >> beam.io.gcp.bigquery.WriteToBigQuery(table='ornate-lead-227417:sdataset.stable')
  quotes[Split.OUTPUT_TAG_PS2] | "tf2" >> beam.io.gcp.bigquery.WriteToBigQuery(table='ornate-lead-227417:sdataset.stable2')

  result = p.run()
  result.wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()