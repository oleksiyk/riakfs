"use strict";

var Promise = require('bluebird');

var mmm = require('mmmagic'),
    Magic = mmm.Magic,
    magic = new Magic(mmm.MAGIC_MIME_TYPE);

var Chunk = function(file, n, riakfs) {
    if(!(this instanceof Chunk)) {
        return new Chunk(file, n, riakfs);
    }

    this.file = file;
    this.n = n;
    this.key = file.id + ':' + n;
    this.riak = riakfs.riak;
    this.bucket = riakfs.chunksBucket;

    this.position = 0;
    this.length = 0;
    this.modified = false;
    this.vclock = undefined;
}

Chunk.CHUNK_SIZE = 256 * 1024;

module.exports = Chunk;

Chunk.prototype.load = function() {
    var self = this;

    return self.riak.get({
        bucket: self.bucket,
        key: self.key,
        deletedvclock: true
    }).then(function(reply) {
        if(!reply || !reply.content){
            throw new Error('Chunk not found: ' + self.bucket + '/' + self.key)
        }
        self.data = reply.content[0].value;
        self.position = self.data.length
        self.length = self.data.length;
        self.modified = false;
        self.vclock = reply.vclock;
    })
}

var mimetype = function (buffer) {
    return new Promise(function(resolve, reject) {
        magic.detect(buffer, function(err, result) {
            if (err) {
                return reject(err)
            }
            resolve(result)
        })
    })
}

Chunk.prototype.save = function() {
    var self = this;

    if(!self.modified){
        return Promise.resolve()
    }

    var data = self.data.slice(0, self.length)

    return self.riak.put({
        bucket: self.bucket,
        key: self.key,
        vclock: self.vclock,
        content: {
            value: data,
            content_type: 'binary/octet-stream',
            usermeta: [{
                key: 'length',
                value: data.length
            }]
        }
    }).then(function() {
        self.mofified = false;
        if(self.n === 0){
            return mimetype(data)
                .then(function(_mt) {
                    self.file.contentType = _mt;
                })
                .catch(function() {})
        }
    })
}

Chunk.prototype.write = function(buffer, position) {
    var self = this;

    self.modified = true;

    // allocate new buffer
    if(!self.data){
        self.data = new Buffer(Chunk.CHUNK_SIZE);
    }

    // chunk data is appended, grow the buffer
    if(self.data.length < Chunk.CHUNK_SIZE){
        var newBuf = new Buffer(Chunk.CHUNK_SIZE)
        self.data.copy(newBuf)
        self.data = newBuf
    }

    if(position > self.length){
        return 0
    }

    self.position = position;

    var bytesToWrite = Chunk.CHUNK_SIZE - self.position
    if(bytesToWrite > buffer.length){
        bytesToWrite = buffer.length
    }

    if(bytesToWrite === 0){
        return 0
    }

    buffer.copy(self.data, self.position, 0, bytesToWrite)
    self.position += bytesToWrite

    if(self.position > self.length){
        self.length = self.position
    }

    return bytesToWrite;
}

Chunk.prototype.read = function(buffer, position, length) {
    var self = this;

    if(position >= self.data.length){
        return 0
    }

    var bytesToRead = self.data.length - position
    if(bytesToRead > length){
        bytesToRead = length
    }

    self.data.slice(position, position + bytesToRead).copy(buffer)
    self.position += bytesToRead

    return bytesToRead
}
