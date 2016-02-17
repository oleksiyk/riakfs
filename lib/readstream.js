'use strict';

var util = require('util');
var stream = require('stream');
var _ = require('lodash');

var RiakFsReadStream = function (riakfs, _path, options) {
    options = _.defaults(options || {}, {
        flags: 'r',
        encoding: null,
        fd: null,
        autoClose: true
    });

    stream.Readable.call(this, options);

    this.flags = options.flags;
    this.autoClose = options.autoClose;
    this.riakfs = riakfs;
    this.path = _path;
    this.bytesRead = 0;

    if (options.fd) {
        this.fd = options.fd;
        this.emit('open', this.fd);
    } else {
        this.open();
    }

    this.once('finish', this.close);
};

util.inherits(RiakFsReadStream, stream.Readable);

module.exports = RiakFsReadStream;

RiakFsReadStream.prototype.destroy = function () {
    if (this.destroyed) {
        return;
    }

    this.destroyed = true;

    if (this.fd) {
        this.close();
    }
};

RiakFsReadStream.prototype.open = function () {
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
            if (self.autoClose) {
                self.close();
            }
            self.emit('error', err);
        });
};

RiakFsReadStream.prototype.close = function () {
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

RiakFsReadStream.prototype._read = function (size) {
    var self = this, buffer;

    if (!self.fd) {
        return self.once('open', function () {
            self._read(size);
        });
    }

    if (self.destroyed) {
        return undefined;
    }

    if (size > self.fd.file.size) {
        size = self.fd.file.size;
    }

    buffer = new Buffer(size);

    self.riakfs.read(self.fd, buffer, 0, size, null)
        .then(function (read) {
            self.bytesRead += read;
            if (read > 0) {
                return self.push(buffer.slice(0, read));
            }
            self.push(null);
        })
        .catch(function (err) {
            if (self.autoClose) {
                self.close();
            }
            self.emit('error', err);
        });
};
