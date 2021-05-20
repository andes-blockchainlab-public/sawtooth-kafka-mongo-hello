require('dotenv').config()

const kafka = require('kafka-node');
const Producer = kafka.Producer;
const Consumer = kafka.Consumer;
const _ = require('underscore');
const util = require('util');

console.log('KAFKA_CONSUMER:', process.env.KAFKA_CONSUMER);
console.log('KAFKA_PRODUCER:', process.env.KAFKA_PRODUCER);

if(!process.env.KAFKA_CONSUMER){
  console.log('No KAFKA_CONSUMER');
  process.exit(1);
}

if(!process.env.KAFKA_PRODUCER){
  console.log('No KAFKA_PRODUCER');
  process.exit(1);
}

const TOPIC = 'mytopic';
let admin;

let clientProducer;
let clientConsumer;
let producer;
let consumer;

async function getTopics(){
  let res = await new Promise((resolve, reject) => {
    admin.listTopics((err, res) => {
      if(err){
        return reject(err);
      }
      resolve(res);
    });
  });
  // console.log(JSON.stringify(res, null, 2));
  return _.chain(res[1].metadata)
    .keys()
    .filter(k => k[0] !== '_')
    .value();
}

async function createTopic(topic){

  var topics = [{
    topic: topic,
    partitions: 1,
    replicationFactor: 1
  }];
  let r = await new Promise((resolve, reject) => {
    admin.createTopics(topics,(err, res) => {
      if(err){
        return reject(err);
      }
      if(res.length > 0){
        return reject(new Error(res[0].error));
      }
      resolve(res);
    });
  });

  let allTopics = await getTopics();
  if(!_.some(allTopics, t => t === topic)){
    throw new Error(`topic ${topic} could not be created`);
  }
}

function initProducer(){
  clientProducer = new kafka.KafkaClient({kafkaHost: process.env.KAFKA_PRODUCER});
  producer = new Producer(clientProducer);

  return new Promise((resolve) => {
    producer.on('ready', function () {
      resolve();
    });
  });
}

function initConsumer(){
  clientConsumer = new kafka.KafkaClient({kafkaHost: process.env.KAFKA_CONSUMER});
  consumer = new Consumer(
    clientConsumer,
    [
      { topic: TOPIC, partition: 0 }
    ],
    {
      autoCommit: false
    }
  );

  // consumer = new Consumer(
  //   clientConsumer,
  //   [
  //       { topic: TOPIC, partition: 0, offset: 1}
  //   ],
  //   {
  //       autoCommit: false,
  //       fromOffset: true
  //   }
  // );
}


module.exports = async () => {
  const client = new kafka.KafkaClient({kafkaHost: process.env.KAFKA_PRODUCER});
  admin = new kafka.Admin(client); // client must be KafkaClient
  
  let offset = new kafka.Offset(client);
  offset.fetchLatestOffsets(['mybest'], (err, data) => {
    console.log('----');
    console.log(data);
    console.log('----');
  });

  try{
    await createTopic(TOPIC);
  }
  catch(err){
    if(err.message === `Topic '${TOPIC}' already exists.`){
      console.log(err.message);
    }
    else{
      console.log(err);
      process.exit(0);
    }
  }

  await initProducer();
  await initConsumer();
  let success = false;
  const payloads = [
    { topic: TOPIC, messages: 'hi', partition: 0 }
  ];

  producer.send(payloads, function (err, data) {
    if(err){
      return console.log(err);
    }
    // console.log(data);
  });
  
  producer.on('error', function (err) {});

  consumer.on('message', (msg) => {
    console.log(msg);
    success = true;
  });

  await new Promise((resolve) => setTimeout(resolve, 500));

  clientProducer.close();


  consumer.commit(() => {
    clientConsumer.close();
  });

  client.close();

  if(success){
    console.log('SUCCESS');
  }
}


module.exports();

