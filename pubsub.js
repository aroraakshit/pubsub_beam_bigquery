'use strict';

const { PubSub } = require('@google-cloud/pubsub'); // Imports the Google Cloud client library
const credentials = require('/path/to/credentials.json'); // points to credentials

const pubsubClient = new PubSub({ projectId:credentials.project_id, credentials:credentials }); // Instantiates a client
const topicName = 'projects/[PROJECT_ID]/topics/[TOPIC_NAME]';
const subscriptionName = 'projects/[PROJECT_ID]/subscriptions/[SUBSCRIPTION_NAME]';

async function test_publish() {
  const topic = pubsubClient.topic(topicName); // Get an already created subscription...
  const publisher = await topic.publisher();
  // console.log(publisher);
  var ts = new Date().toISOString().slice(0, 19).replace('T', ' ') ; // convert to date-time used by BigQuery (SQL)
  var event_name = "super duper event - 1";
  var payload_ = "{'string': 'CO', 'int': '100', 'timestamp': '" + ts + "'}"; // According to the schema of row in BigQuery, unless some pre-processing is due in Apache Beam component
  var json = JSON.stringify({event_name:event_name,payload:payload_});

  console.log("Publishing:");
  console.log(json);

  // Publish (data must be a Buffer)
  const data = Buffer.from(json);
  var msgId = await publisher.publish(data);
  console.log(msgId);
}

async function test_subscribe() {
  // Get an already created subscription...
  const topic = pubsubClient.topic(topicName);

  // Subscribing
  const subsc = await topic.subscription(subscriptionName);
  
  // console.log(subsc);
  const messageHandler = message => {
      console.log(`Received message ${message.id}:`);
      console.log(`\tData: ${message.data}`);
      console.log(`\tAtributes ${message.attributes}`);
      message.ack();
  };
  subsc.on(`message`, messageHandler);
  setTimeout(() => {
    subsc.removeListener('message', messageHandler);
    // console.log("message received");
  }, 20*1000); // Wait for 20 seconds
  
}

// Handling command line arguments
if (process.argv[2] == "publish") {
  test_publish().then(function() {
    console.log("Published.");
    test_subscribe();
    process.exit();
  });
}
else if (process.argv[2] == "subscribe") {
  test_subscribe();
}