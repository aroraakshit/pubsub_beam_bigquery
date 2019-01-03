// Imports the Google Cloud client library
const { PubSub } = require('@google-cloud/pubsub');

const credentials = require('./cred.json');

// Instantiates a client
const pubsubClient = new PubSub({ projectId:credentials.project_id, credentials:credentials });

// The name for the new topic
const topicName = 'projects/ornate-lead-227417/topics/sample_topic';
const subscriptionName = 'projects/ornate-lead-227417/subscriptions/my-sub';

// Creates the new topic
/*
pubsubClient
  .createTopic(topicName)
  .then(results => {
    const topic = results[0];
    console.log(`Topic ${topic.name} created.`);
  })
  .catch(err => {
    console.error('ERROR:', err);
  });
*/

async function test_publish() {

  // Get an already created subscription...
  const topic = pubsubClient.topic(topicName);

  const publisher = await topic.publisher();
  console.log(publisher);

  var ts_event = (new Date()).toISOString();
  // var ts_event = (new Date()).getTime()*1000;
  var event_name = "super_duper_event";

  var json = JSON.stringify({ts_event: ts_event, ts_stored:ts_event, event_name:event_name});

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

  // Subscribing. Something like this, but not exactly sure how get() works yet...
  const subsc = await topic.subscription(subscriptionName);
//   const subsc = await pubsubClient.subscription(subscriptionName);
  console.log(subsc);
  const messageHandler = message => {
      console.log(`Received message ${message.id}:`);
      console.log(`\tData: ${message.data}`);
      console.log(`\tAtributes ${message.attributes}`);
      message.ack();
  };
  subsc.on(`message`, messageHandler);
  setTimeout(() => {
    subsc.removeListener('message', messageHandler);
    console.log("message received");
  }, 20*1000);
  
}


test_publish().then(function() {
  console.log("Published....");
  test_subscribe();
  process.exit();
});

// test_subscribe();