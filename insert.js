const inputFilePath = './data/hits3/smallfile-1.txt'


const readline = require('readline');
const fs = require('fs');
const MongoClient = require('mongodb').MongoClient;

// const url = 'mongodb+srv://developertweetsy:0cGLQqe2zIWGn4zU@cluster0.ucbom0z.mongodb.net/tweetsy_leadforest_test';
// const url = 'mongodb+srv://doadmin:3cKA16a270dJ4Wo9@leadforest-twitter-data-986e4fc6.mongo.ondigitalocean.com/?authMechanism=DEFAULT';
const url = 'mongodb+srv://doadmin:fKH124W03569JLUP@leadforest-twitter-user-data-762016af.mongo.ondigitalocean.com/?authMechanism=DEFAULT';

const batchSize = 10000; // Change this to the batch size you want to use

const client = new MongoClient(url, { useNewUrlParser: true, useUnifiedTopology: true,keepAlive: 1, connectTimeoutMS: 30000 });
const rl = readline.createInterface({
  input: fs.createReadStream(inputFilePath) 
});

let batch = [];
rl.on('line', (line) => {
  const [emailData, screenNameData] = line.split('ScreenName:');
  const trimmedEmail = emailData?.split(" - ")[0]?.split(":")[1]?.trim()
  const trimmedScreenName = screenNameData?.split(' - ')[0]?.trim()
  if(trimmedEmail && trimmedScreenName){
     batch.push({ screenName:trimmedScreenName, email:trimmedEmail });
     console.log("pushing",batch.length, { screenName:trimmedScreenName, email:trimmedEmail, fileName: inputFilePath })
  }
 
//   if (batch.length === batchSize) {
//     insertBatch(batch);
//     batch = [];
//   }
});

rl.on('close', () => {
  if (batch.length > 0) {
    console.log('total length', batch.length)
    insertBatch(batch);
  }
});

async function insertBatch(batch) {
  try {
    await client.connect();
    const db = client.db('twitter_data'); 
    const collection = db.collection('users'); 
    
    rese = await collection.insertMany(batch , { unique : true } );
    console.log('inserted batch', batch.length,'=', rese.insertedCount)
  } finally {
    await client.close();
  }
}
