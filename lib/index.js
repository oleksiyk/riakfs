'use strict';

var path    = require('path');
var _       = require('lodash');
var Promise = require('bluebird');
var util    = require('util');
var uid2    = require('uid2');
var Riak  = require('no-riak');

var RiakFsError            = require('./error');
var RiakFsWriteStream      = require('./writestream');
var RiakFsReadStream       = require('./readstream');
var Stats                  = require('./stats');
var Chunk                  = require('./chunk');

var RiakFs = function (options, riak) {
    var self = this;

    self.options = options;
    self.riak = riak || new Riak.Client(options.riak);
    self.filesBucket = options.root + '.files';
    self.chunksBucket = options.root + '.chunks';
    self.statsBucket = options.root + '.stats';
    self.directoryIndex = options.root.toLowerCase() + '.files_directory_bin';
};

exports.create = function (options, riak, callback) {
    return Promise.try(function () {
        if (typeof riak === 'function') {
            callback = riak;
            riak = undefined;
        }
        options = _.defaultsDeep(options || {}, {
            root: 'fs',
            metaType: 'default',
            chunksType: 'default',
            statsType: 'default',
            riak: {
                connectionString: '127.0.0.1:8087'
            },
            events: false,
            shared: {
                fs: false
            },
            quotacheck: false,
            trash: false
        });

        return new RiakFs(options, riak);
    }).nodeify(callback);
};

require('util').inherits(RiakFs, require('events').EventEmitter);

function _normalizePath(_path) {
    return path.normalize(_path).replace(/(.+)\/$/, '$1');
}

RiakFs.prototype._removeChunks = function (file) {
    var self = this;
    return Promise.map(_.range(Math.ceil(file.size / Chunk.CHUNK_SIZE)), function (n) {
        return new Chunk(file, n, self).delete();
    }, { concurrency: 10 });
};

RiakFs.prototype.unlink = function (_filename, callback) {
    var self = this, file;

    return self._readLink(_filename).spread(function (fs, filename, readOnly) {
        if (readOnly) {
            throw new RiakFsError('EACCES', 'Permission denied: ' + _filename);
        }

        return fs.riak.get({
            bucket: fs.filesBucket,
            key: filename,
            type: fs.options.metaType
        })
        .then(function (reply) {
            if (!reply || !reply.content) {
                throw new RiakFsError('ENOENT', 'No such file or directory: ' + filename);
            }

            file = reply.content[0].value;

            if (file.isDirectory) {
                throw new RiakFsError('EISDIR', 'File is a directory: ' + filename);
            }

            if (fs.options.trash === true && !/^\/\.Trash(\/|$)/.test(filename)) {
                return (function _test(_path, attempt) {
                    var testPath;

                    attempt = attempt || 0;
                    testPath = _path + (attempt > 0 ? ('.' + attempt) : '');
                    return fs.stat(testPath)
                    .then(function () {
                        if (attempt >= 9) { // 10 copies max
                            return fs.unlink(testPath).then(function () {
                                return _normalizePath(testPath);
                            });
                        }
                        return _test(_path, ++attempt);
                    })
                    .catch(function (err) {
                        if (err.code !== 'ENOENT') {
                            throw err;
                        }
                        return _normalizePath(testPath);
                    });
                }('/.Trash/' + filename))
                .then(function (_path) {
                    return fs.makeTree(path.dirname(_path))
                    .then(function () {
                        return fs._rename(filename, _path);
                    });
                });
            }

            return Promise.all([
                fs._removeChunks(file),
                fs.riak.del({
                    bucket: fs.filesBucket,
                    key: filename,
                    vclock: reply.vclock,
                    type: fs.options.metaType
                })
            ])
            .then(function () {
                if (fs.options.events) {
                    fs.emit('delete', filename, new Stats(file, false));
                }
                return fs._updateStats([-1 * file.size], [-1], [fs.options.trash ? (-1 * file.size) : 0]);
            });
        });
    })
    .return(null)
    .nodeify(callback);
};

RiakFs.prototype.readdir2 = function (__path, max, marker, callback) {
    var self = this;

    return self._readLink(__path).spread(function (fs, _path) {
        var searchParams = {
            bucket: fs.filesBucket,
            index: fs.directoryIndex,
            qtype: 0,
            key: _path,
            pagination_sort: true,
            type: fs.options.metaType
        };

        if (max) {
            searchParams.max_results = max;
        }

        if (marker) {
            searchParams.continuation = marker;
        }

        return Promise.all([
            fs.riak.get({
                bucket: fs.filesBucket,
                key: _path,
                type: fs.options.metaType
            }),
            fs.riak.index(searchParams)
        ])
        .spread(function (reply, search) {
            if (_path !== '/') {
                if (!reply || !reply.content) {
                    throw new RiakFsError('ENOENT', 'No such file or directory:' + __path);
                }
                if (!reply.content[0].value.isDirectory) {
                    throw new RiakFsError('ENOTDIR', 'Not a directory: ' + __path);
                }
            }

            if (search.results.length) {
                search.results = search.results.map(function (f) {
                    return path.basename(f);
                });
            }
            if (search.results.length > 1000) {
                if (search.results.length > 10000) {
                    self.emit('dirsizelimit', __path, search.results.length, 10000);
                } else if (search.results.length > 5000) {
                    self.emit('dirsizelimit', __path, search.results.length, 5000);
                } else if (search.results.length > 3000) {
                    self.emit('dirsizelimit', __path, search.results.length, 3000);
                } if (search.results.length > 1000) {
                    self.emit('dirsizelimit', __path, search.results.length, 1000);
                }
            }
            return search;
        });
    })
    .nodeify(callback);
};


RiakFs.prototype.readdir = function (_path, callback) {
    var self = this;

    return self.readdir2(_path).then(function (search) {
        return search.results;
    }).nodeify(callback);
};

RiakFs.prototype.mkdir = function (__path, mode, callback) {
    var self = this;

    if (typeof mode === 'function') {
        callback = mode;
    }

    if (__path.length > 4096) {
        return Promise.reject(new RiakFsError('ENAMETOOLONG', 'Path name exceeded 4096 characters')).nodeify(callback);
    }

    function _mkdir() {
        return self._readLink(__path).spread(function (fs, _path, readOnly) {
            var d = path.dirname(_path), p = Promise.resolve(true);

            if (_path === '/') {
                throw new RiakFsError('EEXIST', 'Path already exists: /');
            }

            if (readOnly) {
                throw new RiakFsError('EACCES', 'Readonly share: ' + __path);
            }

            if (d !== '/') {
                p = fs.riak.get({
                    bucket: fs.filesBucket,
                    key: d,
                    type: fs.options.metaType
                });
            }

            return Promise.all([
                fs.riak.get({
                    bucket: fs.filesBucket,
                    key: _path,
                    deletedvclock: true,
                    type: fs.options.metaType
                }), p
            ]).spread(function (reply, parentReply) {
                var value;

                if (d !== '/') {
                    if (!parentReply || !parentReply.content || !parentReply.content[0].value) {
                        throw new RiakFsError('ENOENT', 'No such file or directory: ' + path.dirname(__path));
                    }

                    if (!parentReply.content[0].value.isDirectory) {
                        throw new RiakFsError('ENOTDIR', 'A component of the path prefix is not a directory: ' + __path);
                    }
                }

                if (reply && reply.content) {
                    throw new RiakFsError('EEXIST', 'Path already exists: ' + __path);
                }

                value = {
                    ctime: new Date().toISOString(),
                    mtime: new Date().toISOString(),
                    isDirectory: true
                };

                p = fs.riak.put({
                    bucket: fs.filesBucket,
                    key: _path,
                    vclock: reply ? reply.vclock : undefined, // tombstone vclock
                    content: {
                        value: value,
                        indexes: [{
                            key: fs.directoryIndex,
                            value: d
                        }]
                    },
                    type: fs.options.metaType
                });

                if (fs.options.events) {
                    p = p.then(function () {
                        fs.emit('new', _path, new Stats(value, false));
                        return null;
                    });
                }

                return p;
            });
        }).return(null);
    }

    if (typeof self.options.quotacheck === 'function' && !/^\/\.Trash(\/|$)/.test(__path)) {
        return self.options.quotacheck().then(function (ok) {
            if (ok === false) {
                throw new RiakFsError('EDQUOT', 'Quota exceeded');
            }
            return _mkdir();
        }).nodeify(callback);
    }
    return _mkdir().nodeify(callback);
};

RiakFs.prototype.rmdir = function (__path, callback) {
    return this._readLink(__path).spread(function (fs, _path, readOnly) {
        if (_path === '/') {
            throw new RiakFsError('EACCES', 'Cannot remove /');
        }

        if (readOnly) {
            throw new RiakFsError('EACCES', 'Readonly share: ' + __path);
        }

        return Promise.all([
            fs.riak.get({
                bucket: fs.filesBucket,
                key: _path,
                type: fs.options.metaType
            }),
            fs.riak.index({
                bucket: fs.filesBucket,
                index: fs.directoryIndex,
                qtype: 0,
                max_results: 1,
                key: _path,
                type: fs.options.metaType
            })
        ])
        .spread(function (reply, search) {
            var dir, p;

            if (!reply || !reply.content) {
                throw new RiakFsError('ENOENT', 'No such file or directory:' + __path);
            }
            if (!reply.content[0].value.isDirectory) {
                throw new RiakFsError('ENOTDIR', 'Not a directory: ' + __path);
            }
            dir = reply.content[0].value;
            if (search.results.length) {
                throw new RiakFsError('ENOTEMPTY', 'Directory not empty: ' + __path);
            }

            if (dir.share && dir.share.to.length) {
                throw new RiakFsError('ESHARED', 'Directory is shared: ' + __path);
            }

            p = fs.riak.del({
                bucket: fs.filesBucket,
                key: _path,
                vclock: reply.vclock,
                type: fs.options.metaType
            });

            if (fs.options.events) {
                p = p.then(function () {
                    fs.emit('delete', _path, new Stats(reply.content[0].value, false));
                });
            }

            return p;
        });
    })
    .return(null)
    .nodeify(callback);
};

RiakFs.prototype.open = function (_filename, flags, mode, callback) {
    var self = this;

    function _open() {
        var d, p = Promise.resolve({ isDirectory: true }), options = {};

        if (typeof _filename === 'object') {
            options = _filename;
            _filename = options.filename;
        }

        if (typeof mode === 'function') {
            callback = mode;
        }

        if (!/^(r|r\+|w|wx|w\+|wx\+|a|ax|a\+|ax\+)$/.test(flags)) {
            return Promise.reject(new RiakFsError('EINVAL', 'Invalid flags given: ' + flags)).nodeify(callback);
        }

        if (_filename.length > 4096) {
            return Promise.reject(new RiakFsError('ENAMETOOLONG', 'Path name exceeded 4096 characters')).nodeify(callback);
        }

        if (path.basename(_filename).length > 255) {
            return Promise.reject(new RiakFsError('ENAMETOOLONG', 'File name exceeded 255 characters')).nodeify(callback);
        }

        return self._readLink(_filename).spread(function (fs, filename, readOnly) {
            filename = _normalizePath(filename);
            d = path.dirname(filename);

            if (d !== '/') {
                p = fs.riak.get({
                    bucket: fs.filesBucket,
                    key: d,
                    type: fs.options.metaType
                })
                .then(function (reply) {
                    if (reply && reply.content) {
                        return reply.content[0].value;
                    }
                    return null;
                });
            }

            return Promise.all([
                fs.riak.get({
                    bucket: fs.filesBucket,
                    key: filename,
                    deletedvclock: true,
                    type: fs.options.metaType
                }),
                p
            ])
            .spread(function (reply, parent) {
                var file = null, oldFile;

                if (reply && reply.content) {
                    file = reply.content[0].value;
                }
                if (!file && /^(r|r\+)$/.test(flags)) {
                    throw new RiakFsError('ENOENT', 'No such file or directory: ' + _filename);
                }
                if (file && file.isDirectory) {
                    throw new RiakFsError('EISDIR', 'File is a directory: ' + _filename);
                }
                if (!parent) {
                    throw new RiakFsError('ENOENT', 'No such file or directory: ' + path.dirname(_filename));
                }
                if (!parent.isDirectory) {
                    throw new RiakFsError('ENOTDIR', 'Not a directory: ' + path.dirname(_filename));
                }
                if (file && /^(wx|wx\+|ax|ax\+)$/.test(flags)) {
                    throw new RiakFsError('EEXIST', 'File already exists: ' + _filename);
                }

                if (/^(r\+|w|w\+|a|a\+|wx|wx\+|ax|ax\+)$/.test(flags)) {
                    oldFile = file ? _.cloneDeep(file) : null;
                    if (readOnly) {
                        throw new RiakFsError('EACCES', 'Permission denied: ' + _filename);
                    }
                    if (file && file.version > 1000) {
                        throw new RiakFsError('ETOOMANY', 'Too many overwrites: ' + _filename);
                    }
                    if (file && /^(w|w\+)$/.test(flags)) {
                        // truncate file, remove all existing chunks
                        return fs._removeChunks(file).then(function () {
                            file.size = 0;
                            file.contentType = undefined;
                            if (options.meta) {
                                file.meta = _.merge(file.meta || {}, options.meta);
                            }
                            return {
                                oldFile: oldFile,
                                flags: flags,
                                position: 0,
                                file: file,
                                fs: fs,
                                vclock: reply.vclock,
                                indexes: reply.content[0].indexes,
                                filename: filename
                            };
                        });
                    }
                    if (file && /^(a|a\+)$/.test(flags)) {
                        return {
                            oldFile: oldFile,
                            flags: flags,
                            position: file.size,
                            file: file,
                            fs: fs,
                            vclock: reply.vclock,
                            indexes: reply.content[0].indexes,
                            filename: filename
                        };
                    }
                    if (file && flags === 'r+') {
                        return {
                            oldFile: oldFile,
                            flags: flags,
                            position: 0,
                            file: file,
                            fs: fs,
                            vclock: reply.vclock,
                            indexes: reply.content[0].indexes,
                            filename: filename
                        };
                    }
                    if (!file) { // create new file
                        file = {
                            id: uid2(32),
                            ctime: new Date().toISOString(),
                            mtime: new Date().toISOString(),
                            size: 0,
                            contentType: 'binary/octet-stream',
                            version: -1
                        };
                        if (options.meta) {
                            file.meta = options.meta;
                        }

                        return fs.riak.put({
                            bucket: fs.filesBucket,
                            key: filename,
                            vclock: reply ? reply.vclock : undefined, // tombstone vclock
                            content: {
                                value: file,
                                indexes: [{
                                    key: fs.directoryIndex,
                                    value: d
                                }]
                            },
                            type: fs.options.metaType,
                            return_head: true
                        }).then(function (_reply) {
                            return {
                                isNew: true,
                                flags: flags,
                                position: 0,
                                filename: filename,
                                vclock: _reply.vclock,
                                indexes: _reply.content[0].indexes,
                                file: file,
                                fs: fs
                            };
                        });
                    }
                }

                // flags = r
                return {
                    flags: flags,
                    file: file,
                    fs: fs,
                    position: 0,
                    vclock: reply.vclock,
                    indexes: reply.content[0].indexes,
                    filename: filename
                };
            });
        });
    }

    if (typeof self.options.quotacheck === 'function' && /^(w|w\+|a|a\+|wx|wx\+|ax|ax\+)$/.test(flags)) {
        return self.options.quotacheck().then(function (ok) {
            if (ok === false) {
                throw new RiakFsError('EDQUOT', 'Quota exceeded');
            }
            return _open().nodeify(callback);
        });
    }
    return _open().nodeify(callback);
};

RiakFs.prototype.close = function (fd, callback) {
    var p;

    if (!fd || !fd.file || fd.closed) {
        return Promise.reject(new RiakFsError('EBADF', 'Invalid file descriptor')).nodeify(callback);
    }

    fd.closed = true;

    if (fd.chunk && fd.chunk.modified) {
        p = fd.chunk.save();
    } else {
        p = Promise.resolve();
    }

    if (fd.modified) {
        p = p.then(function () {
            fd.file.mtime = new Date().toISOString();
            fd.file.version += 1;

            /*if (fd.fs.options.events === true && fd.file.version > 1000) {
                fd.fs.emit('overwritelimit', fd.filename, fd.file.version);
            }*/

            return fd.fs.riak.put({
                bucket: fd.fs.filesBucket,
                key: fd.filename,
                vclock: fd.vclock,
                content: {
                    value: fd.file,
                    indexes: fd.indexes
                },
                type: fd.fs.options.metaType
            });
        });
    }

    if (fd.modified || fd.isNew) {
        p = p.tap(function () {
            if (fd.isNew) {
                if (fd.fs.options.events === true) {
                    fd.fs.emit('new', fd.filename, new Stats(fd.file, false));
                }
                if (fd.fs.options.trash === true && /^\/\.Trash(\/|$)/.test(fd.filename)) {
                    return fd.fs._updateStats([fd.file.size], [1], [fd.file.size]);
                }
                return fd.fs._updateStats([fd.file.size], [1]);
            }

            if (fd.fs.options.events === true) {
                fd.fs.emit('change', fd.filename, new Stats(fd.file, false), new Stats(fd.oldFile, false));
            }
            if (fd.fs.options.trash === true && /^\/\.Trash(\/|$)/.test(fd.filename)) {
                return fd.fs._updateStats([-1 * fd.oldFile.size, fd.file.size], [], [-1 * fd.oldFile.size, fd.file.size]);
            }
            return fd.fs._updateStats([-1 * fd.oldFile.size, fd.file.size], []);
        });
    }

    return p.return(null).nodeify(callback);
};

RiakFs.prototype.read = function (fd, buffer, offset, length, position, callback) {
    var p, n, oldPosition;

    offset = offset | 0;
    length = length | 0;

    if (!fd || fd.closed || !fd.file || !/^(w\+|wx\+|r|r\+|a\+|ax\+)$/.test(fd.flags)) {
        return Promise.reject(new RiakFsError('EBADF', 'Invalid file descriptor')).nodeify(callback);
    }

    if (!Buffer.isBuffer(buffer)) {
        return Promise.reject(new RiakFsError('EINVAL', 'buffer argument should be Buffer or String')).nodeify(callback);
    }

    if (buffer.length < offset + length) {
        return Promise.reject(new RiakFsError('EINVAL', 'buffer is too small')).nodeify(callback);
    }

    if (typeof position === 'undefined') {
        position = null;
    }

    if (typeof position === 'function') {
        callback = position;
        position = null;
    }

    if (position !== null) {
        position = position | 0;
        if (position > fd.file.size) {
            return Promise.reject(new RiakFsError('EINVAL', 'The specified file offset is invalid')).nodeify(callback);
        }
        fd.position = position;
    }

    if (length > fd.file.size - fd.position) {
        length = fd.file.size - fd.position;
    }

    if (fd.file.size === 0 || length === 0) {
        return Promise.resolve(0);
    }

    n = Math.floor(fd.position / Chunk.CHUNK_SIZE); oldPosition = fd.position;

    if (!fd.chunk || fd.chunk.n !== n) {
        fd.chunk = new Chunk(fd.file, n, fd.fs);
        p = fd.chunk.load();
    } else {
        p = Promise.resolve();
    }

    function _read(_buffer, _length) {
        var read = fd.chunk.read(_buffer, fd.position - (fd.chunk.n * Chunk.CHUNK_SIZE), _length);
        if (read === 0) {
            return Promise.resolve();
        }
        fd.position += read;
        _length -= read;

        if (_length > 0) {
            fd.chunk = new Chunk(fd.file, Math.floor(fd.position / Chunk.CHUNK_SIZE), fd.fs);
            return fd.chunk.load().then(function () {
                return _read(_buffer.slice(read), _length);
            });
        }
        return Promise.resolve();
    }

    return p
        .then(function () {
            return _read(buffer.slice(offset), length);
        })
        .then(function () {
            return fd.position - oldPosition;
        })
        .nodeify(callback);
};

RiakFs.prototype.write = function (fd, buffer, offset, length, position, callback) {
    var p, n;

    offset = offset | 0;
    length = length | 0;

    if (!fd || fd.closed || !fd.file || !/^(r\+|w|w\+|wx|wx\+|a|a\+|ax|ax\+)$/.test(fd.flags)) {
        return Promise.reject(new RiakFsError('EBADF', 'Invalid file descriptor')).nodeify(callback);
    }

    if (typeof buffer === 'string') {
        buffer = new Buffer(buffer, 'utf8');
    }

    if (!Buffer.isBuffer(buffer)) {
        return Promise.reject(new RiakFsError('EINVAL', 'buffer argument should be Buffer or String')).nodeify(callback);
    }

    if (typeof position === 'undefined') {
        position = null;
    }

    if (typeof position === 'function') {
        callback = position;
        position = null;
    }

    if (position !== null) {
        position = position | 0;
        if (position > fd.file.size) {
            return Promise.reject(new RiakFsError('EINVAL', 'The specified file offset is invalid')).nodeify(callback);
        }
        fd.position = position;
    }

    n = fd.position === 0 ? 0 : (Math.ceil(fd.position / Chunk.CHUNK_SIZE) - 1);

    if (!fd.chunk || fd.chunk.n !== n) {
        fd.chunk = new Chunk(fd.file, n, fd.fs);
        p = fd.chunk.load();
    } else {
        p = Promise.resolve();
    }

    fd.modified = true;

    function _write(_buffer) {
        var _ps;

        var written = fd.chunk.write(_buffer, fd.position - (fd.chunk.n * Chunk.CHUNK_SIZE));
        fd.position += written;

        if (fd.position > fd.file.size) {
            fd.file.size = fd.position;
        }

        if (written < _buffer.length) {
            _ps = fd.chunk.save();
            // we probably need to load() each chunk if we are updating existing file
            fd.chunk = new Chunk(fd.file, Math.floor(fd.position / Chunk.CHUNK_SIZE), fd.fs);
            return Promise.all([
                _ps,
                _write(_buffer.slice(written))
            ]);
        }
        return Promise.resolve();
    }

    return p
        .then(function () {
            return _write(buffer.slice(offset, offset + length));
        })
        .return(length)
        .nodeify(callback);
};

RiakFs.prototype._rename = function (_old, _new) {
    var self = this, file, vclock;

    return Promise.all([
        self.riak.get({
            bucket: self.filesBucket,
            key: _old,
            type: self.options.metaType
        }),
        self.riak.get({
            bucket: self.filesBucket,
            key: _new,
            head: true,
            deletedvclock: true,
            type: self.options.metaType
        })
    ])
    .spread(function (reply, reply2) { // create new file record
        if (!reply || !reply.content) {
            throw new RiakFsError('ENOENT', 'No such file or directory: ' + _old);
        }
        file = reply.content[0].value;
        vclock = reply.vclock;
        file.mtime = new Date().toISOString();

        return self.riak.put({
            bucket: self.filesBucket,
            key: _new,
            vclock: reply2 ? reply2.vclock : undefined, // for tombstones
            content: {
                value: file,
                indexes: [{
                    key: self.directoryIndex,
                    value: path.dirname(_new)
                }]
            },
            type: self.options.metaType
        });
    })
    .then(function () { // update shares
        if (file && typeof self.options.shared.fs === 'function') {
            if (file.share) { // source share, update destinations
                return Promise.map(file.share.to, function (to) {
                    return self.options.shared.fs(to.root).then(function (_tfs) {
                        var dstPath = '/Shared/' + to.alias;
                        return _tfs.riak.get({
                            bucket: _tfs.filesBucket,
                            key: dstPath,
                            type: _tfs.options.metaType
                        }).then(function (dreply) {
                            var tdir;

                            if (!dreply || !dreply.content) {
                                return undefined;
                            }
                            tdir = dreply.content[0].value;
                            if (!tdir || !tdir._sharedFrom || !tdir.isDirectory) {
                                return undefined;
                            }
                            tdir._sharedFrom.path = _new;
                            return _tfs.riak.put({
                                bucket: _tfs.filesBucket,
                                key: dstPath,
                                vclock: dreply.vclock,
                                content: {
                                    value: tdir,
                                    indexes: [{
                                        key: _tfs.directoryIndex,
                                        value: path.dirname(dstPath)
                                    }]
                                },
                                type: _tfs.options.metaType
                            });
                        });
                    });
                });
            } else if (file._sharedFrom) { // destination share, update source
                return self.options.shared.fs(file._sharedFrom.root).then(function (_tfs) {
                    return _tfs.riak.get({
                        bucket: _tfs.filesBucket,
                        key: file._sharedFrom.path,
                        type: _tfs.options.metaType
                    }).then(function (dreply) {
                        var tdir, shareInd;

                        if (!dreply || !dreply.content) {
                            return undefined;
                        }
                        tdir = dreply.content[0].value;
                        if (!tdir || !tdir.share || !tdir.isDirectory) {
                            return undefined;
                        }
                        shareInd = _.findIndex(tdir.share.to, { root: self.options.root });
                        if (shareInd === -1) {
                            return undefined;
                        }
                        tdir.share.to[shareInd].alias = path.basename(_new);
                        return _tfs.riak.put({
                            bucket: _tfs.filesBucket,
                            key: file._sharedFrom.path,
                            vclock: dreply.vclock,
                            content: {
                                value: tdir,
                                indexes: [{
                                    key: _tfs.directoryIndex,
                                    value: path.dirname(file._sharedFrom.path)
                                }]
                            },
                            type: _tfs.options.metaType
                        });
                    });
                });
            }
        }
        return null;
    })
    .then(function () { // recursively walk directory
        if (file && file.isDirectory) {
            return self.riak.index({
                bucket: self.filesBucket,
                index: self.directoryIndex,
                qtype: 0,
                key: _old,
                type: self.options.metaType
            }).then(function (search) {
                if (search) {
                    return Promise.map(search.results, function (key) {
                        return self._rename(key, key.replace(_old, _new));
                    }, { concurrency: 10 });
                }
                return null;
            });
        }
        return null;
    })
    .then(function () { // delete old file record
        return self.riak.del({
            bucket: self.filesBucket,
            key: _old,
            vclock: vclock,
            type: self.options.metaType
        });
    })
    .then(function () {
        if (self.options.events === true) {
            self.emit('rename', _old, _new, new Stats(file, false));
        }
        if (self.options.trash === true) {
            if (/^\/\.Trash(\/|$)/.test(_old) && !/^\/\.Trash(\/|$)/.test(_new)) {
                return self._updateStats([], [], [-1 * file.size]);
            }
            if (!/^\/\.Trash(\/|$)/.test(_old) && /^\/\.Trash(\/|$)/.test(_new)) {
                return self._updateStats([], [], [file.size]);
            }
        }
        return null;
    });
};

// cross filesystem
function _move(ofs, from, nfs, to) {
    return ofs.stat(from).then(function (stats) {
        if (stats.isFile()) {
            return nfs.makeTree(path.dirname(to)).then(function () {
                return new Promise(function (resolve, reject) {
                    var readStream = ofs.createReadStream(from);
                    var writeStream = nfs.createWriteStream(to);
                    readStream.on('error', reject);
                    writeStream.on('error', reject);
                    writeStream.on('close', resolve);
                    readStream.pipe(writeStream);
                });
            }).then(function () {
                return ofs.unlink(from);
            });
        }
        return ofs.readdir(from).then(function (list) {
            return Promise.reduce(list, function (a, f) {
                return _move(ofs, from + '/' + f, nfs, to + '/' + f);
            }, 0);
        }).then(function () {
            return ofs.rmdir(from);
        });
    });
}

RiakFs.prototype.rename = function (_oldName, _newName, callback) {
    var self = this;
    return Promise.all([
        this._readLink(_oldName),
        this._readLink(_newName)
    ]).spread(function (_old, _new) {
        var ofs = _old[0], nfs = _new[0], oldFile, oldName = _old[1], newName = _new[1];

        if (_old[2] || _new[2]) {
            throw new RiakFsError('EACCES', 'Permission denied');
        }

        if (oldName === newName) {
            return undefined;
        }

        if ((newName + '/').indexOf(oldName + '/') === 0) {
            throw new RiakFsError('EINVAL', 'old is a parent directory of new');
        }

        if (oldName === '/' || oldName === '/Shared' || oldName === '/.Trash') {
            throw new RiakFsError('EINVAL', 'Cannot rename / or /Shared or /.Trash');
        }

        return Promise.all([
            nfs.stat(newName).reflect(),
            nfs.stat(path.dirname(newName)).reflect(),
            ofs.stat(oldName).reflect()
        ])
        .spread(function (newFile, newDir, _oldFile) {
            if (_oldFile.isRejected() || newDir.isRejected()) {
                throw new RiakFsError('ENOENT', 'A component of the old path does not exist, or a path prefix of new does not exist');
            } else {
                oldFile = _oldFile.value();
                newDir = newDir.value();
            }

            if (!newDir.isDirectory()) {
                throw new RiakFsError('ENOTDIR', 'A component of new path prefix is not a directory');
            }

            if (oldFile.file.share && path.dirname(newName) !== '/Shared' && ofs.options.root !== self.options.root) {
                throw new RiakFsError('EINVAL', 'Cannot move shared directories');
            }

            if (newFile.isFulfilled()) {
                newFile = newFile.value();

                if (oldFile.isDirectory() && !newFile.isDirectory()) {
                    throw new RiakFsError('ENOTDIR', 'old is a directory, but new is not a directory');
                }
                if (newFile.isDirectory() && !oldFile.isDirectory()) {
                    throw new RiakFsError('EISDIR', 'new is a directory, but old is not a directory');
                }
                if (newFile.isDirectory()) {
                    return nfs.rmdir(newName);
                }
                return nfs.unlink(newName);
            }
            return null;
        })
        .then(function () {
            var p;

            if (oldFile.file.share) { // rename the destination share dir (not move)
                ofs = nfs;
                oldName = _oldName;
            }

            if (nfs.options.root === ofs.options.root) {
                p = nfs._rename(oldName, newName);
            } else {
                p = _move(ofs, oldName, nfs, newName);
            }

            return p;
        });
    })
    .return(null)
    .nodeify(callback);
};

RiakFs.prototype.stat = function (__path, callback) {
    var self = this, map;

    return self._readLink(__path).spread(function (fs, _path, readOnly) {
        if (_path === '/') {
            map = new Riak.CRDT.Map(self.riak, {
                bucket: self.statsBucket,
                type: self.options.statsType,
                key: 'stats'
            });
            return map.load().call('value').then(function (stats) {
                return new Stats({
                    isDirectory: true,
                    ctime: new Date(0),
                    mtime: new Date(0),
                    stats: {
                        storage: stats.storage ? stats.storage.toNumber() : 0,
                        trashStorage: stats.trashStorage ? stats.trashStorage.toNumber() : 0,
                        files: stats.files ? stats.files.toNumber() : 0
                    }
                });
            });
        }

        return fs.riak.get({
            bucket: fs.filesBucket,
            key: _path,
            type: fs.options.metaType
        })
        .then(function (reply) {
            if (!reply || !reply.content) {
                throw new RiakFsError('ENOENT', 'No such file or directory: ' + __path);
            }
            return new Stats(reply.content[0].value, readOnly);
        });
    })
    .nodeify(callback);
};

RiakFs.prototype.createWriteStream = function (_path, options) {
    return new RiakFsWriteStream(this, _path, options);
};

RiakFs.prototype.writeFile = function (filename, data, options, callback) {
    var self = this;

    if (typeof options === 'function') {
        callback = options;
    }

    options = _.defaults(options || {}, {
        flags: 'w',
        encoding: 'utf8'
    });

    if (!Buffer.isBuffer(data)) {
        data = new Buffer('' + data, options.encoding);
    }

    return self.open(filename, options.flags)
        .then(function (fd) {
            return self.write(fd, data, 0, data.length, null)
                .then(function () {
                    return self.close(fd);
                });
        })
        .nodeify(callback);
};

RiakFs.prototype.appendFile = function (filename, data, options, callback) {
    if (typeof options === 'function') {
        callback = options;
    }

    options = _.defaults(options || {}, {
        flags: 'a',
        encoding: 'utf8'
    });

    return this.writeFile(filename, data, options, callback);
};

RiakFs.prototype.createReadStream = function (_path, options) {
    return new RiakFsReadStream(this, _path, options);
};

RiakFs.prototype.readFile = function (filename, options, callback) {
    var self = this;

    if (typeof options === 'function') {
        callback = options;
    }

    options = _.defaults(options || {}, {
        flags: 'r',
        encoding: null
    });

    return self.open(filename, options.flags)
        .then(function (fd) {
            var data = new Buffer(fd.file.size);
            return self.read(fd, data, 0, fd.file.size, null)
                .then(function (bytesRead) {
                    data = data.slice(0, bytesRead);
                    return self.close(fd);
                })
                .then(function () {
                    if (!options.encoding) {
                        return data;
                    }
                    return data.toString(options.encoding);
                });
        })
        .nodeify(callback);
};

RiakFs.prototype.exists = function (_path, callback) {
    return this._readLink(_path).spread(function (fs, __path) {
        if (__path === '/') {
            return true;
        }

        return fs.riak.get({
            bucket: fs.filesBucket,
            key: __path,
            head: true,
            type: fs.options.metaType
        })
        .then(function (reply) {
            if (!reply || !reply.content) {
                return false;
            }
            return true;
        });
    })
    .nodeify(callback);
};

RiakFs.prototype.utimes = function (_path, atime, mtime, callback) {
    var self = this;

    return self.open(_path, 'r').then(function (fd) {
        return self.futimes(fd, atime, mtime, callback);
    });
};

RiakFs.prototype.futimes = function (fd, atime, mtime, callback) {
    if (!fd || fd.closed || !fd.file) {
        return Promise.reject(new RiakFsError('EBADF', 'Invalid file descriptor')).nodeify(callback);
    }

    if (atime !== null) {
        if (typeof atime === 'number') {
            atime = new Date(atime);
        }

        if (!util.isDate(atime)) {
            return Promise.reject(new Error('Invalid atime: ' + atime));
        }
        fd.file.atime = atime.toISOString();
    }

    if (mtime !== null) {
        if (typeof mtime === 'number') {
            mtime = new Date(mtime);
        }

        if (!util.isDate(mtime)) {
            return Promise.reject(new Error('Invalid mtime: ' + mtime));
        }

        fd.file.mtime = mtime.toISOString();
    }

    if (atime === null && mtime === null) {
        return Promise.resolve().nodeify(callback);
    }

    return fd.fs.riak.put({
        bucket: fd.fs.filesBucket,
        key: fd.filename,
        vclock: fd.vclock,
        content: {
            value: fd.file,
            indexes: [{
                key: fd.fs.directoryIndex,
                value: path.dirname(fd.filename)
            }]
        },
        type: fd.fs.options.metaType
    })
    .tap(function () {
        if (fd.fs.options.events === true) {
            fd.fs.emit('change', fd.filename, new Stats(fd.file, false));
        }
    })
    .return(null)
    .nodeify(callback);
};

RiakFs.prototype.makeTree = function (_path, callback) {
    var self = this;

    _path = _normalizePath(_path);

    return Promise.reduce(_path.split('/'), function (a, c) {
        var p;

        a.push(c);
        p = a.join('/');
        if (p) {
            return self.mkdir(p).catch(RiakFsError, function (err) {
                if (err.code !== 'EEXIST') {
                    throw err;
                }
            })
            .return(a);
        }
        return a;
    }, []).nodeify(callback);
};

RiakFs.prototype.rmTree = function (_path, callback) {
    var self = this;

    _path = _normalizePath(_path);

    function _rmTree(dir) {
        return self.readdir(dir).then(function (files) {
            return Promise.map(files, function (f) {
                return self.rmTree(path.normalize(dir + '/' + f));
            }, { concurrency: 10 });
        })
        .then(function () {
            if (dir === '/') {
                return null;
            }
            return self.rmdir(dir);
        });
    }

    return self.stat(_path).then(function (stats) {
        if (stats.isDirectory()) {
            return _rmTree(_path);
        }
        return self.unlink(_path);
    })
    .return(null)
    .nodeify(callback);
};

RiakFs.prototype.copy = function (from, to, callback) {
    var self = this;

    return self.stat(from).then(function (stats) {
        return new Promise(function (resolve, reject) {
            var readStream = self.createReadStream(from);
            var writeStream = self.createWriteStream({
                filename: to,
                meta: stats.file.meta
            });
            readStream.on('error', reject);
            writeStream.on('error', reject);
            writeStream.on('close', resolve);
            readStream.pipe(writeStream);
        });
    })
    .return(null)
    .nodeify(callback);
};

RiakFs.prototype.updateMeta2 = function (_filename, fn) {
    return this._readLink(_filename).spread(function (fs, filename, readOnly) {
        var file;
        if (readOnly) {
            throw new RiakFsError('EACCES', 'Permission denied: ' + _filename);
        }

        return fs.riak.get({
            bucket: fs.filesBucket,
            key: filename,
            type: fs.options.metaType
        })
        .then(function (reply) {
            if (!reply || !reply.content) {
                throw new RiakFsError('ENOENT', 'No such file or directory: ' + _filename);
            }
            file = reply.content[0].value;

            file.meta = fn(file.meta || {});

            return fs.riak.put({
                bucket: fs.filesBucket,
                key: filename,
                vclock: reply.vclock,
                content: {
                    value: file,
                    indexes: reply.content[0].indexes
                },
                type: fs.options.metaType
            });
        })
        .tap(function () {
            if (fs.options.events === true) {
                fs.emit('change', filename, new Stats(file, false));
            }
        });
    }).return(null);
};

RiakFs.prototype.setContentType = function (_filename, contentType) {
    return this._readLink(_filename).spread(function (fs, filename, readOnly) {
        var file;
        if (readOnly) {
            throw new RiakFsError('EACCES', 'Permission denied: ' + _filename);
        }

        return fs.riak.get({
            bucket: fs.filesBucket,
            key: filename,
            type: fs.options.metaType
        })
        .then(function (reply) {
            if (!reply || !reply.content) {
                throw new RiakFsError('ENOENT', 'No such file or directory: ' + _filename);
            }
            file = reply.content[0].value;

            file.contentType = contentType;

            return fs.riak.put({
                bucket: fs.filesBucket,
                key: filename,
                vclock: reply.vclock,
                content: {
                    value: file,
                    indexes: reply.content[0].indexes
                },
                type: fs.options.metaType
            });
        })
        .tap(function () {
            if (fs.options.events === true) {
                fs.emit('change', filename, new Stats(file, false));
            }
        });
    }).return(null);
};

RiakFs.prototype.updateMeta = function (filename, data, callback) {
    return this.updateMeta2(filename, function () {
        return data;
    }).nodeify(callback);
};

RiakFs.prototype.setMeta = function (filename, data, callback) {
    return this.updateMeta2(filename, function (meta) {
        return _.merge(meta, data);
    }).nodeify(callback);
};

RiakFs.prototype.fstat = function (fd, callback) {
    return Promise.resolve(new Stats(fd.file)).nodeify(callback);
};

RiakFs.prototype._readLink = function (_path) {
    var self = this, readOnly = false;

    _path = _normalizePath(_path);

    return Promise.try(function () {
        var p;
        if (typeof self.options.shared.fs === 'function' && /^\/Shared\//.test(_path)) {
            p = _path.split('/');
            p = '/' + p[1] + '/' + p[2];

            return self.riak.get({
                bucket: self.filesBucket,
                key: p,
                type: self.options.metaType
            })
            .then(function (reply) {
                var d;
                if (!reply || !reply.content) {
                    return self;
                }
                d = reply.content[0].value;
                if (d._sharedFrom) {
                    readOnly = d._sharedFrom.readOnly || false;
                    _path = _normalizePath(_path.replace(p, d._sharedFrom.path));
                    return self.options.shared.fs(d._sharedFrom.root);
                }
                return self;
            });
        }
        return self;
    })
    .then(function (_fs) {
        return [_fs, _path, readOnly];
    });
};

RiakFs.prototype.share = function (_path, root, alias, readOnly, callback) {
    var self = this, dstPath = '/Shared/' + alias, p = Promise.resolve(), _p;

    _path = _normalizePath(_path);

    if (typeof self.options.shared.fs !== 'function') {
        return Promise.reject(new RiakFsError('EINVAL', 'options.shared.fs is not a function')).nodeify(callback);
    }

    if (root === this.options.root) {
        return Promise.reject(new RiakFsError('EINVAL', 'Cannot share with itself')).nodeify(callback);
    }

    if (_path === '/' || _path === '/Shared') {
        return Promise.reject(new RiakFsError('EINVAL', 'Cannot share / or /Shared')).nodeify(callback);
    }

    if (/^\/Shared\/?/.test(_path)) {
        _p = _path.split('/');
        _p = '/' + _p[1] + '/' + _p[2];
        p = self.riak.get({
            bucket: self.filesBucket,
            key: _p,
            type: self.options.metaType
        }).then(function (reply) {
            if (!reply || !reply.content) {
                return;
            }
            if (reply.content[0].value._sharedFrom) {
                throw new RiakFsError('ESHARED', 'Cannot re-share');
            }
        });
    }

    if (/^\/\.Trash(\/|$)/.test(_path)) {
        return Promise.reject(new RiakFsError('EINVAL', 'Cannot share from /.Trash')).nodeify(callback);
    }

    if (/\//.test(alias)) {
        return Promise.reject(new RiakFsError('EINVAL', 'Alias cannot be a path')).nodeify(callback);
    }

    return p.then(function () {
        return Promise.all([
            self.riak.get({
                bucket: self.filesBucket,
                key: _path,
                type: self.options.metaType
            }),
            self.options.shared.fs(root)
        ]);
    })
    .spread(function (sreply, _tfs) {
        var dir;
        if (!sreply || !sreply.content) {
            throw new RiakFsError('ENOENT', 'No such file or directory: ' + _path);
        }
        dir = sreply.content[0].value;
        if (!dir.isDirectory) {
            throw new RiakFsError('ENOTDIR', 'Not a directory: ' + _path);
        }
        if (dir._sharedFrom) {
            throw new RiakFsError('EACCES', 'Permission denied: ' + _path);
        }
        if (!(_tfs instanceof RiakFs)) {
            throw new RiakFsError('EINVAL', 'options.shared.fs should return RiakFs object');
        }
        if (dir.share && dir.share.to.length) {
            if (_.find(dir.share.to, { root: root })) {
                throw new RiakFsError('ESHARED', 'Already shared');
            }
        }

        return _tfs.makeTree(dstPath).then(function () {
            return Promise.all([
                _tfs.riak.get({
                    bucket: _tfs.filesBucket,
                    key: dstPath,
                    type: _tfs.options.metaType
                }),
                _tfs.riak.index({
                    bucket: _tfs.filesBucket,
                    index: _tfs.directoryIndex,
                    qtype: 0,
                    max_results: 1,
                    key: dstPath,
                    type: _tfs.options.metaType
                })
            ])
            .spread(function (dreply, search) {
                var tdir;

                if (!dreply || !dreply.content) {
                    throw new RiakFsError('ENOENT', 'No such file or directory: ' + dstPath);
                }
                if (search.results.length) {
                    throw new RiakFsError('ENOTEMPTY', 'Directory not empty: ' + dstPath);
                }
                tdir = dreply.content[0].value;
                if (!tdir.isDirectory) {
                    throw new RiakFsError('ENOTDIR', 'Not a directory: ' + dstPath);
                }

                tdir._sharedFrom = {
                    root: self.options.root,
                    path: _path,
                    readOnly: readOnly || false
                };

                return _tfs.riak.put({
                    bucket: _tfs.filesBucket,
                    key: dstPath,
                    vclock: dreply.vclock,
                    content: {
                        value: tdir,
                        indexes: [{
                            key: _tfs.directoryIndex,
                            value: path.dirname(dstPath)
                        }]
                    },
                    type: _tfs.options.metaType
                });
            });
        })
        .then(function () {
            if (!dir.share) {
                dir.share = {
                    to: [],
                    owner: {
                        root: self.options.root,
                        path: _path
                    }
                };
            }

            dir.share.to.push({
                root: root,
                alias: alias,
                readOnly: readOnly || false
            });

            return self.riak.put({
                bucket: self.filesBucket,
                key: _path,
                vclock: sreply.vclock,
                content: {
                    value: dir,
                    indexes: [{
                        key: self.directoryIndex,
                        value: path.dirname(_path)
                    }]
                },
                type: self.options.metaType
            })
            .then(function () {
                if (self.options.events) {
                    self.emit('share:src', _path, root, dstPath);
                }
                if (_tfs.options.events) {
                    _tfs.emit('share:dst', dstPath, self.options.root, _path);
                }
            });
        });
    })
    .return(null)
    .nodeify(callback);
};

RiakFs.prototype.unshare = function (__path, root, callback) {
    var dir;
    if (typeof root === 'function') {
        callback = root;
    }
    if (typeof root !== 'string' || !root) {
        root = this.options.root;
    }

    __path = _normalizePath(__path);

    return this._readLink(__path).spread(function (fs, _path) {
        if (typeof fs.options.shared.fs !== 'function') {
            throw new RiakFsError('EINVAL', 'options.shared.fs is not a function');
        }

        if (_path === '/') {
            throw new RiakFsError('EINVAL', 'Cannot opearate on root');
        }

        return fs.riak.get({
            bucket: fs.filesBucket,
            key: _path,
            type: fs.options.metaType
        })
        .then(function (reply) {
            var shareInd;
            if (!reply || !reply.content) {
                throw new RiakFsError('ENOENT', 'No such file or directory: ' + _path);
            }
            dir = reply.content[0].value;
            if (!dir.isDirectory) {
                throw new RiakFsError('ENOTDIR', 'Not a directory: ' + _path);
            }
            if (dir.share && dir.share.to.length) {
                shareInd = _.findIndex(dir.share.to, { root: root });
                if (shareInd === -1) {
                    throw new RiakFsError('ENOTSHARED', _path + ' is not shared with ' + root);
                }
                __path = '/Shared/' + dir.share.to[shareInd].alias;
                dir.share.to.splice(shareInd, 1);
                if (dir.share.to.length === 0) {
                    delete dir.share;
                }
                return fs.riak.put({
                    bucket: fs.filesBucket,
                    key: _path,
                    vclock: reply.vclock,
                    content: {
                        value: dir,
                        indexes: [{
                            key: fs.directoryIndex,
                            value: path.dirname(_path)
                        }]
                    },
                    type: fs.options.metaType
                })
                .then(function () {
                    if (fs.options.events) {
                        fs.emit('unshare:src', _path, root, __path);
                    }
                });
            }
            throw new RiakFsError('ENOTSHARED', 'Not a shared directory: ' + _path);
        })
        .then(function () {
            return fs.options.shared.fs(root);
        })
        .then(function (_tfs) {
            return _tfs.riak.get({
                bucket: _tfs.filesBucket,
                key: __path,
                type: _tfs.options.metaType
            }).then(function (dreply) {
                if (!dreply || !dreply.content) {
                    throw new RiakFsError('ENOENT', 'No such file or directory: ' + __path);
                }
                if (!dreply.content[0].value.isDirectory) {
                    throw new RiakFsError('ENOTDIR', 'Not a directory: ' + __path);
                }
                return _tfs.riak.del({
                    bucket: _tfs.filesBucket,
                    key: __path,
                    vclock: dreply.vclock,
                    type: _tfs.options.metaType
                })
                .then(function () {
                    if (_tfs.options.events) {
                        _tfs.emit('delete', __path, new Stats(dreply.content[0].value));
                        _tfs.emit('unshare:dst', __path, fs.options.root, _path);
                    }
                });
            });
        });
    })
    .return(null)
    .nodeify(callback);
};

RiakFs.prototype.listAll = function (max, marker) {
    var self = this;

    return self.riak.index({
        bucket: self.filesBucket,
        index: '$bucket',
        qtype: 0,
        key: self.filesBucket,
        pagination_sort: true,
        type: self.options.metaType,
        max_results: max || 10,
        continuation: marker
    });
};

RiakFs.prototype._updateStats = function (storageOps, fileOps, trashStorageOps) {
    var self = this;

    var map = new Riak.CRDT.Map(self.riak, {
        bucket: self.statsBucket,
        type: self.options.statsType,
        key: 'stats'
    });

    fileOps = fileOps || [];
    trashStorageOps = trashStorageOps || [];

    return map
        .update('storage', storageOps.reduce(function (a, s) { return a.increment(s); }, new Riak.CRDT.Counter()))
        .update('trashStorage', trashStorageOps.reduce(function (a, s) { return a.increment(s); }, new Riak.CRDT.Counter()))
        .update('files', fileOps.reduce(function (a, s) { return a.increment(s); }, new Riak.CRDT.Counter()))
        .save();
};

RiakFs.prototype._rescanContentType = function (filename, callback) {
    var self = this;

    return self.open(filename, 'r+')
        .then(function (fd) {
            var size = _.min([Chunk.CHUNK_SIZE, fd.file.size]);
            var data = new Buffer(size);
            return self.read(fd, data, 0, size, null)
                .then(function (bytesRead) {
                    if (bytesRead === size) {
                        return self.write(fd, data, 0, size, 0);
                    }
                    return null;
                })
                .then(function () {
                    return self.close(fd);
                });
        })
        .nodeify(callback);
};

RiakFs.prototype.__fixSize = function (filename) {
    var self = this;

    return self.open(filename, 'r+')
        .then(function (fd) {
            var size = 0;
            return (function _loadChunk(n) {
                var chunk = new Chunk(fd.file, n, fd.fs);
                return chunk.load()
                    .then(function () {
                        size += chunk.length;
                        return _loadChunk(n + 1);
                    })
                    .catch(function () {
                        return null;
                    });
            }(0))
            .then(function () {
                fd.file.size = size;
                fd.modified = true;
                return self.close(fd);
            });
        });
};

/*RiakFs.prototype.truncate = function(_path, len, callback) {
}

RiakFs.prototype.ftruncate = function(fd, len, callback) {
}*/

/*RiakFs.prototype.symlink = function (dest, src, _type, callback) {
    // return Promise.resolve(new Stats(fd.file)).nodeify(callback);
};*/
