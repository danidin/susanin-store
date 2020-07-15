const zmq = require('zeromq')
const sock = zmq.socket('rep')
const endpoint = 'tcp://127.0.0.1:2000';

const assert = require('assert')
const mongodb = require('mongodb')
const MongoClient = mongodb.MongoClient
const client = new MongoClient('mongodb://localhost:27017')

let db
client.connect(function(err) {
  assert.equal(null, err);
  console.log('Connected successfully to mongo server');
  db = client.db('susanin-data');
});

sock.bind(endpoint)
sock.on('message', (message) => {
  const parsed = JSON.parse(message)

  console.log('Susanin store: got message: ', parsed)

  if (parsed.action === 'read') {
    const collection = db.collection('emails');
    collection.find({}).toArray(function(err, emails) {
      assert.equal(err, null);
      sock.send(JSON.stringify(emails))
    });
  }
  else if(parsed.action === 'create') {
    const collection = db.collection('emails');

    collection.insertOne(parsed.payload, function(err, result) {
      assert.equal(err, null);
      assert.equal(1, result.result.n);
      assert.equal(1, result.ops.length);
      console.log('Created email');
      sock.send(JSON.stringify({ "status": "OK" }))
    });
  }
  else if (parsed.action === 'delete') {
    const collection = db.collection('emails');

    collection.deleteOne({ _id: new mongodb.ObjectID(parsed.payload) }, function(err, result) {
      assert.equal(err, null);
      console.log('Deleted email id=' + parsed.payload);
      sock.send(JSON.stringify({ "status": "OK" }))
    });
  }
  else{
    sock.send(JSON.stringify({ "status": "Susanin store: action not supported" }))
  }
})

console.log('Susanin store listening on ' + endpoint)
