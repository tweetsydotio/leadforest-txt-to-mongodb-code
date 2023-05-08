const inputFilePath = './data/hits8/smallfile-4.txt'

const readline = require('readline');
const fs = require('fs');
const MongoClient = require('mongodb').MongoClient;

const url = 'mongodb+srv://doadmin:fKH124W03569JLUP@leadforest-twitter-user-data-762016af.mongo.ondigitalocean.com/?authMechanism=DEFAULT';

const batchSize = 10000;

const client = new MongoClient(url, { useNewUrlParser: true, useUnifiedTopology: true, keepAlive: 1, connectTimeoutMS: 30000 });
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
    const collection = db.collection('twitter_users');

    // create index on screenName field
    await collection.createIndex({ screenName: 1 });
    await collection.createIndex({ email: 1 });

    // insert batch
    rese = await collection.insertMany(batch, { unique : true } );
    console.log('inserted batch', batch.length,'=', rese.insertedCount)
  }catch(err){
    console.log('err occured')
  } finally {
    await client.close();
  }
}
