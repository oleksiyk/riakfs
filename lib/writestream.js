'use strict';

var util = require('util');
var stream = require('stream');
var _ = require('lodash');
var EventEmitter = require('events').EventEmitter;

var RiakFsWriteStream = function (riakfs, _path, options) {
    options = _.defaults(options || {}, {
        flags: 'w',
        encoding: null,
        highWaterMark: 256 * 1024,
        decodeStrings: true
    });

    stream.Writable.call(this, options);

    this.flags = options.flags;
    this.riakfs = riakfs;
    this.path = _path;
    this.bytesWritten = 0;

    this.open();
};

util.inherits(RiakFsWriteStream, stream.Writable);

module.exports = RiakFsWriteStream;

RiakFsWriteStream.prototype.destroySoon = RiakFsWriteStream.prototype.end;


// catch Writable `finish` event and close everything first
// this is a temporary fix for
// https://github.com/nodejs/node/issues/2994
// https://github.com/nodejs/node/issues/4672
// https://github.com/nodejs/node/pull/2314
RiakFsWriteStream.prototype.emit = function () {
    var self = this, args = arguments, event = args[0];
    if (event !== 'finish') {
        EventEmitter.prototype.emit.apply(self, args);
    } else {
        self.once('close', function () {
            EventEmitter.prototype.emit.apply(self, args);
        });
        self.close();
    }
};

RiakFsWriteStream.prototype.destroy = function () {
    if (this.destroyed) {
        return;
    }

    this.destroyed = true;

    if (this.fd) {
        this.close();
    }
};

RiakFsWriteStream.prototype.open = function () {
    var self = this;

    if (self._opening || self.fd) {
        return;
    }
    self._opening = true;

    self.riakfs.open(self.path, self.flags)
        .then(function (fd) {
            self.fd = fd;
            self.emit('open', self.fd);
            return null;
        })
        .catch(function (err) {
            self.emit('error', err);
        });
};

RiakFsWriteStream.prototype.close = function () {
    var self = this;

    function _close() {
        self.riakfs.close(self.fd)
            .then(function () {
                self.fd = null;
                self.emit('close');
            })
            .catch(function (err) {
                self.emit('error', err);
            });
    }

    if (self._closed) {
        return process.nextTick(self.emit.bind(self, 'close'));
    }

    if (!self.fd) {
        return self.once('open', _close);
    }

    self._closed = true;

    _close();
};

RiakFsWriteStream.prototype._write = function (chunk, encoding, callback) {
    var self = this;

    if (!self.fd) {
        return self.once('open', function () {
            self._write(chunk, encoding, callback);
        });
    }

    self.riakfs.write(self.fd, chunk, 0, chunk.length, null)
        .then(function (written) {
            self.bytesWritten += written;
            return true;
        })
        .nodeify(callback);
};
