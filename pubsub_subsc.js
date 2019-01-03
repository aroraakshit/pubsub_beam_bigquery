// Imports the Google Cloud client library
const { PubSub } = require('@google-cloud/pubsub');

const credentials = require('./cred.json');

// Instantiates a client
const pubsubClient = new PubSub({ projectId:credentials.project_id, credentials:credentials });

// The name for the new topic
const topicName = 'projects/ornate-lead-227417/topics/sample_topic1';
const subscriptionName = 'projects/ornate-lead-227417/subscriptions/my-sub1';


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
  }, 120*1000);
  
}



test_subscribe();