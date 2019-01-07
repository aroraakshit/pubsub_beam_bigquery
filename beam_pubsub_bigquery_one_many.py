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
# python beam_pubsub_bigquery_one_many.py --input_topic 'projects/ornate-lead-227417/topics/sample_topic' --runner DirectRunner

"""A streaming word-counting workflow.
"""

from __future__ import absolute_import

import argparse
import logging

from past.builtins import unicode

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.examples.wordcount import WordExtractingDoFn
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions

class ComputeWordLengthFn(beam.DoFn):
  def process(self, element):
    return [{'string1': 'MG', 'string2': 'oujiogyhiut', 'int1': 333},
    {'string1': 'Yo', 'string2': 'Do, ujgiyuhuhThere is no try.', 'int1': 2}]

class ComputeWordLengthFn1(beam.DoFn):
  def process(self, element):
    return [{'string1': 'MG', 'num1': 333}]


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

  quotes1 = messages | beam.ParDo(ComputeWordLengthFn())
  quotes2 = messages | beam.ParDo(ComputeWordLengthFn1())
  
  quotes1 | "tf1" >> beam.io.gcp.bigquery.WriteToBigQuery(table='ornate-lead-227417:sdataset.stable', project='ornate-lead-227417')
  quotes2 | "tf2" >> beam.io.gcp.bigquery.WriteToBigQuery(table='ornate-lead-227417:sdataset.stable2', project='ornate-lead-227417')

  result = p.run()
  result.wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()