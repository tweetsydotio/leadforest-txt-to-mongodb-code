const input = './Hits (3).txt';
const outputPrefix = './data/hits3/smallfile';

const readline = require('readline');
const fs = require('fs');


const batchSize = 1000000; // number of lines per output file
let counter = 0;
let batchCounter = 1;

const inputReader = readline.createInterface({
  input: fs.createReadStream(input, { encoding: 'utf8' }),
  crlfDelay: Infinity
});

let outputWriter = null;
inputReader.on('line', function(line) {
  if (counter === 0 || counter % batchSize === 0) {
    // Start a new output file
    const outputName = `${outputPrefix}-${batchCounter}.txt`;
    console.log(outputName,' created!', ' counter = ' + counter )
    outputWriter = fs.createWriteStream(outputName, { encoding: 'utf8' });
    batchCounter++;
  }
  outputWriter.write(line + '\n', 'utf8');
  counter++;
});

inputReader.on('close', function() {
  if (outputWriter) {
    outputWriter.end();
  }
  console.log(`${counter} lines were written to output files.`);
});
