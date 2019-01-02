gcloud beta dataflow jobs run bi_ps_to_bq \
    --max-workers=1 \
    --region=us-central1 \
    --gcs-location gs://dataflow-templates/latest/PubSub_to_BigQuery \
    --parameters \
inputTopic=projects/bi-project-224423/topics/polls-analytics,\
outputTableSpec=bi-project-224423:poll_analytics_raw.events

#    --experiments=enable_streaming_engine \
#    --workerMachineType=n1-standard-2 \
