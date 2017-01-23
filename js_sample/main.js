const avro = require('avsc');
const path = require('path');
const fs = require('fs');
const walk = require('walk');


function read_schema() {
    const pavsc = path.resolve(__dirname, '../message.avsc')
    const avsc = JSON.parse(fs.readFileSync(pavsc));
    return avro.parse(avsc)
}

function walk_through() {
    var walker = walk.walk(__dirname + '/..');
    walker.on('file', function(root, stat, next) {
        console.log(root + ':' + stat.name);
        next();
    })
}

console.log(read_schema())
walk_through();