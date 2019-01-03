# dead code

python -m apache_beam.examples.wordcount --input test.csv --output sample.txt

python beam_stream_proc.py --output_topic 'projects/ornate-lead-227417/topics/sample_topic1' --input_subscription 'projects/ornate-lead-227417/subscriptions/my-sub'

gcloud beta dataflow jobs run bi_ps_to_bq \
    --max-workers=1 \
    --region=us-central1 \
    --gcs-location gs://dataflow-templates/latest/PubSub_to_BigQuery \
    --parameters \
inputTopic=projects/bi-project-224423/topics/polls-analytics,\
outputTableSpec=bi-project-224423:poll_analytics_raw.events

#    --experiments=enable_streaming_engine \
#    --workerMachineType=n1-standard-2 \
