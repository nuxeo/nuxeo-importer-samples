const avro = require('avsc');
const path = require('path')
const fs = require('fs')


function read_schema() {
    const pavsc = path.resolve(__dirname, '../message.avsc')
    const avsc = JSON.parse(fs.readFileSync(pavsc));
    return avro.parse(avsc)
}

console.log(read_schema())