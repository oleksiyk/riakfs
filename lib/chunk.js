'use strict';

var Promise = require('bluebird');

var mmm = require('mmmagic'),
    Magic = mmm.Magic,
    magic = process.env.MMMAGIC_PATH ? new Magic(process.env.MMMAGIC_PATH, mmm.MAGIC_MIME_TYPE) : new Magic(mmm.MAGIC_MIME_TYPE);

var Chunk = function (file, n, riakfs) {
    this.file = file;
    this.n = n;
    this.key = file.id + ':' + n;
    this.riak = riakfs.riak;
    this.bucket = riakfs.chunksBucket;
    this.bucketType = riakfs.options.chunksType;

    this.length = 0;
    this.modified = false;
    this.vclock = undefined;
};

Chunk.CHUNK_SIZE = 256 * 1024;

module.exports = Chunk;

Chunk.prototype.load = function () {
    var self = this;

    return self.riak.get({
        bucket: self.bucket,
        key: self.key,
        deletedvclock: true,
        type: self.bucketType
    }).then(function (reply) {
        if (reply && reply.content) {
            self.data = reply.content[0].value || new Buffer(0);
            self.length = self.data.length;
            self.modified = false;
        }
        self.vclock = reply ? reply.vclock : undefined;
    });
};

function mimetype(buffer) {
    return new Promise(function (resolve, reject) {
        magic.detect(buffer, function (err, result) {
            if (err) {
                return reject(err);
            }
            return resolve(result || 'application/octet-stream');
        });
    });
}

Chunk.prototype.save = function () {
    var self = this, data;

    if (!self.modified) {
        return Promise.resolve();
    }

    data = self.data.slice(0, self.length);

    return self.riak.update({
        bucket: self.bucket,
        key: self.key,
        vclock: self.vclock,
        pw: 2,
        content: {
            value: data,
            content_type: 'binary/octet-stream',
            usermeta: [{
                key: 'length',
                value: '' + data.length
            }]
        },
        type: self.bucketType
    }).then(function () {
        self.modified = false;
        if (self.n === 0) {
            return mimetype(data)
                .then(function (_mt) {
                    self.file.contentType = _mt;
                })
                .catch(function () {});
        }
        return null;
    });
};

Chunk.prototype.write = function (buffer, position) {
    var self = this, newBuf, bytesToWrite;

    self.modified = true;

    // allocate new buffer
    if (!self.data) {
        self.data = new Buffer(Chunk.CHUNK_SIZE);
    }

    // chunk data is appended, grow the buffer
    if (self.data.length < Chunk.CHUNK_SIZE) {
        newBuf = new Buffer(Chunk.CHUNK_SIZE);
        self.data.copy(newBuf);
        self.data = newBuf;
    }

    if (position > self.length) {
        return 0;
    }

    bytesToWrite = Chunk.CHUNK_SIZE - position;
    if (bytesToWrite > buffer.length) {
        bytesToWrite = buffer.length;
    }

    if (bytesToWrite === 0) {
        return 0;
    }

    buffer.copy(self.data, position, 0, bytesToWrite);

    if ((position + bytesToWrite) > self.length) {
        self.length = position + bytesToWrite;
    }

    return bytesToWrite;
};

Chunk.prototype.read = function (buffer, position, length) {
    var self = this, bytesToRead;

    if (!self.data || position >= self.data.length) {
        return 0;
    }

    bytesToRead = self.data.length - position;
    if (bytesToRead > length) {
        bytesToRead = length;
    }

    self.data.copy(buffer, 0, position, position + bytesToRead);

    return bytesToRead;
};

Chunk.prototype.delete = function () {
    var self = this;

    return self.riak.get({
        bucket: self.bucket,
        key: self.key,
        head: true,
        deletedvclock: true,
        type: self.bucketType
    }).then(function (reply) {
        if (reply && reply.vclock) {
            return self.riak.del({
                bucket: self.bucket,
                key: self.key,
                vclock: reply.vclock,
                type: self.bucketType
            });
        }
        return null;
    });
};
