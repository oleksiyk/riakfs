"use strict";

/* jshint bitwise: false, maxparams: 6 */

var XRegExp = require('xregexp').XRegExp;
var path    = require('path');
var _       = require('lodash');
var Promise = require('bluebird');
var util    = require('util');
var uuid    = require('node-uuid');

var RiakFsError            = require('./error')
var RiakFsWriteStream      = require('./writestream')
var RiakFsReadStream       = require('./readstream')
var Stats                  = require('./stats')
var Chunk                  = require('./chunk')
var RiakFsSiblingsResolver = require('./resolver')

var RiakFs = function(options) {
    var self = this;

    if (!(this instanceof RiakFs)){
        return new RiakFs(options);
    }

    self.riak = require('./riak')(options.riak)
    self.filesBucket = options.root + '.files';
    self.chunksBucket = options.root + '.chunks';
    self.directoryIndex = options.root + '.files_directory_bin';
    self.chunksIndex = options.root + '.chunks_fileid_bin';

    self.resolver = new RiakFsSiblingsResolver(self)
}

module.exports = function(options, callback) {
    options = _.partialRight(_.merge, _.defaults)(options || {}, {
        root: 'fs',
        riak: {
            host: 'localhost',
            port: 8087
        }
    });

    var riakfs = new RiakFs(options)

    return riakfs._setup()
        .return(riakfs)
        .nodeify(callback)
};

RiakFs.prototype._setup = function() {
    var self = this;

    return Promise.all([
        self.riak.setBucket({
            bucket: self.filesBucket,
            props: {
                allow_mult: true,
                last_write_wins: false,
                n_val: 3,
                // backend: ''
            }
        }),
        self.riak.setBucket({
            bucket: self.chunksBucket,
            props: {
                allow_mult: false,
                last_write_wins: false,
                n_val: 3,
                // backend: ''
            }
        })
    ])
}

RiakFs.prototype._removeChunks = function(file) {
    var self = this;

    return self.riak.getIndexAll({
        bucket: self.chunksBucket,
        index: self.chunksIndex,
        qtype: 0,
        key: file.id
    }).then(function(search) {
        if(!search){
            return
        }
        return Promise.map(search.keys, function(key) {
            return self.riak.get({
                bucket: self.chunksBucket,
                key: key,
                head: true
            })
            .then(function(reply) {
                if(reply && reply.content){
                    return self.riak.del({
                        bucket: self.chunksBucket,
                        key: key,
                        vclock: reply.vclock
                    })
                }
            })
        })
    })
}

RiakFs.prototype.unlink = function(filename, callback) {
    var self = this;

    filename = path.normalize(filename)

    return self.riak.get({
        bucket: self.filesBucket,
        key: filename
    })
    .then(self.resolver.resolve(filename))
    .then(function(reply) {
        if(!reply || !reply.content){
            throw new RiakFsError('ENOENT', 'No such file or directory: ' + filename)
        }

        var file = reply.content[0].value;

        if(file.isDirectory){
            throw new RiakFsError('EISDIR', 'File is a directory: ' + filename)
        }

        return self._removeChunks(file).then(function() {
            return self.riak.del({
                bucket: self.filesBucket,
                key: filename,
                vclock: reply.vclock
            })
        })
        .return(reply.vclock)

    })
    .nodeify(callback)
};

RiakFs.prototype.readdir = function(_path, callback) {
    _path = path.normalize(_path)

    var self = this, d = path.dirname(_path);

    return Promise.all([
        self.riak.get({
            bucket: self.filesBucket,
            key: _path
        }).then(self.resolver.resolve(_path)),
        self.riak.getIndexAll({
            bucket: self.filesBucket,
            index: self.directoryIndex,
            qtype: 0,
            key: _path
        })
    ])
    .spread(function(reply, search) {
        var files = [];
        if(d !== '/'){
            if(!reply || !reply.content){
                throw new RiakFsError('ENOENT', 'No such file or directory:' + _path)
            }
            if(!reply.content[0].value.isDirectory){
                throw new RiakFsError('ENOTDIR', 'Not a directory: ' + _path)
            }
        }

        if(search && search.keys && search.keys.length){
            files = search.keys.map(function(f) {
                return path.basename(f)
            })
        }
        return files
    })
    .nodeify(callback)
};

RiakFs.prototype.mkdir = function(_path, mode, callback) {
    var self = this, d = path.dirname(_path),
        p = Promise.resolve(true);

    if(typeof mode === 'function'){
        callback = mode
    }

    if (d !== '/') {
        p = self.riak.get({
            bucket: self.filesBucket,
            key: d
        })
        .then(self.resolver.resolve(d))
    }

    return Promise.all([
        self.riak.get({
            bucket: self.filesBucket,
            key: _path,
            deletedvclock: true
        }), p
    ]).spread(function(reply, parentReply) {
        if (d !== '/') {
            if(!parentReply || !parentReply.content || !parentReply.content[0].value){
                throw new RiakFsError('ENOENT', 'No such file or directory: ' + d)
            }

            if(!parentReply.content[0].value.isDirectory){
                throw new RiakFsError('ENOTDIR', 'A component of the path prefix is not a directory: ' + d)
            }
        }

        if(reply && reply.content){
            throw new RiakFsError('EEXIST', 'Path already exists: ' + _path)
        }

        return self.riak.put({
            bucket: self.filesBucket,
            key: _path,
            vclock: reply.vclock, // tombstone vclock
            content: {
                value: JSON.stringify({
                    ctime: new Date(),
                    mtime: new Date(),
                    isDirectory: true
                }),
                content_type: 'application/json',
                indexes: [{
                    key: self.directoryIndex,
                    value: d
                }]
            }
        })
    })
    .nodeify(callback)
};

RiakFs.prototype.rmdir = function(_path, callback) {
    var self = this;

    _path = path.normalize(_path)

    return Promise.all([
        self.riak.get({
            bucket: self.filesBucket,
            key: _path
        }).then(self.resolver.resolve(_path)),
        self.riak.getIndexAll({
            bucket: self.filesBucket,
            index: self.directoryIndex,
            qtype: 0,
            max_results: 1,
            key: _path
        })
    ])
    .spread(function(reply, search) {
        if(!reply || !reply.content){
            throw new RiakFsError('ENOENT', 'No such file or directory:' + _path)
        }
        if(!reply.content[0].value.isDirectory){
            throw new RiakFsError('ENOTDIR', 'Not a directory: ' + _path)
        }
        if(search && search.keys.length){
            throw new RiakFsError('ENOTEMPTY', 'Directory not empty: ' + _path)
        }

        return self.riak.del({
            bucket: self.filesBucket,
            key: _path,
            vclock: reply.vclock
        })
    })
    .nodeify(callback)
};

RiakFs.prototype.open = function(filename, flags, mode, callback) {
    var self = this, d, p = Promise.resolve({isDirectory: true});

    filename = path.normalize(filename)
    d = path.dirname(filename)

    if(typeof mode === 'function'){
        callback = mode;
    }

    if(!/^(r|r\+|w|wx|w\+|wx\+|a|ax|a\+|ax\+)$/.test(flags)){
        return Promise.reject(new RiakFsError('EINVAL', 'Invalid flags given: ' + flags)).nodeify(callback)
    }

    if (d !== '/') {
        p = self.riak.get({
            bucket: self.filesBucket,
            key: d,
        })
        .then(self.resolver.resolve(d))
        .then(function(reply) {
            if(reply && reply.content){
                return reply.content[0].value
            }
        })
    }

    return Promise.all([
        self.riak.get({
            bucket: self.filesBucket,
            key: filename,
            deletedvclock: true
        }).then(self.resolver.resolve(filename)), p
    ])
    .spread(function(reply, parent) {
        var file = null;
        if(reply && reply.content){
            file = reply.content[0].value
        }
        if(file && file.isDirectory){
            throw new RiakFsError('EISDIR', 'File is a directory: ' + filename)
        }
        if(!parent){
            throw new RiakFsError('ENOENT', 'No such file or directory: ' + d)
        }
        if(!parent.isDirectory){
            throw new RiakFsError('ENOTDIR', 'Not a directory: ' + d)
        }
        if(file && /^(wx|wx\+|ax|ax\+)$/.test(flags)){
            throw new RiakFsError('EEXIST', 'File already exists: ' + filename)
        }
        if(!file && /^(r|r\+)$/.test(flags)){
            throw new RiakFsError('ENOENT', 'No such file or directory: ' + filename)
        }

        if(/^(w|w\+|a|a\+|wx|wx\+|ax|ax\+)$/.test(flags)){
            if(file && /^(w|w\+)$/.test(flags)){
                // truncate file, remove all existing chunks
                return self._removeChunks(file).then(function() {
                    file.size = 0;
                    file.contentType = undefined;
                    return {
                        flags: flags,
                        position: 0,
                        file: file,
                        filename: filename
                    }
                })
            }
            if(file && /^(a|a\+)$/.test(flags)){
                var chunk = new Chunk(file, Math.floor(file.size/Chunk.CHUNK_SIZE), self)
                return chunk.load().then(function() {
                    return {
                        flags: flags,
                        position: file.size,
                        chunk: chunk,
                        file: file,
                        filename: filename
                    }
                })
            }
            if(!file){ // create new file
                file = {
                    id: uuid.v1(),
                    ctime: new Date(),
                    mtime: new Date(),
                    size: 0
                }
                return self.riak.put({
                    bucket: self.filesBucket,
                    key: filename,
                    vclock: reply.vclock, // tombstone vclock
                    content: {
                        value: JSON.stringify(file),
                        content_type: 'application/json',
                        indexes: [{
                            key: self.directoryIndex,
                            value: d
                        }]
                    },
                    // return_head: true
                }).then(function() {
                    return {
                        flags: flags,
                        position: 0,
                        filename: filename,
                        file: file
                    }
                })
            }
        }

        // flags = r or r+
        return {
            flags: flags,
            file: file,
            position: 0,
            filename: filename
        }
    }).nodeify(callback)
};

RiakFs.prototype.close = function(fd, callback) {
    var self = this;

    if(!fd || !fd.file || fd.closed){
        return Promise.reject(new RiakFsError('EBADF', 'Invalid file descriptor')).nodeify(callback)
    }

    var p = Promise.resolve()

    fd.closed = true;

    if(fd.chunk && fd.chunk.modified){
        p = fd.chunk.save()
    }

    if (fd.modified) {
        p = p.then(function() {
            return self.riak.get({
                bucket: self.filesBucket,
                key: fd.filename,
                head: true
            })
        })
        .then(function(reply) {
            fd.file.mtime = new Date()
            fd.file.contentType = fd.file.contentType || 'binary/octet-stream'

            return self.riak.put({
                bucket: self.filesBucket,
                key: fd.filename,
                vclock: reply.vclock,
                content: {
                    value: JSON.stringify(fd.file),
                    content_type: 'application/json',
                    indexes: [{
                        key: self.directoryIndex,
                        value: path.dirname(fd.filename)
                    }]
                }
            })
        })
    }

    return p.nodeify(callback)
};

RiakFs.prototype.read = function(fd, buffer, offset, length, position, callback) {
    var self = this;

    offset = offset | 0;
    length = length | 0;

    if(!fd || fd.closed || !fd.file || !/^(w\+|wx\+|r|r\+|a\+|ax\+)$/.test(fd.flags)){
        return Promise.reject(new RiakFsError('EBADF', 'Invalid file descriptor')).nodeify(callback)
    }

    if(!Buffer.isBuffer(buffer)){
        return Promise.reject(new RiakFsError('EINVAL', 'buffer argument should be Buffer or String')).nodeify(callback)
    }

    if(buffer.length < offset + length){
        return Promise.reject(new RiakFsError('EINVAL', 'buffer is too small')).nodeify(callback)
    }

    if(typeof position === 'undefined'){
        position = null
    }

    if(typeof position === 'function'){
        callback = position
        position = null
    }

    if(position !== null){
        position = position | 0;
        if(position > fd.file.size){
            return Promise.reject(new RiakFsError('EINVAL', 'The specified file offset is invalid')).nodeify(callback)
        }
        fd.position = position;
    }

    if(length > fd.file.size - fd.position){
        length = fd.file.size - fd.position
    }

    var p = Promise.resolve(), n = Math.floor(fd.position/Chunk.CHUNK_SIZE)

    if(!fd.chunk || fd.chunk.n !== n){
        fd.chunk = new Chunk(fd.file, n, self)
        p = fd.chunk.load()
    }

    var _read = function(_buffer, _length) {
        var read = fd.chunk.read(_buffer, fd.position-(fd.chunk.n*Chunk.CHUNK_SIZE), _length)
        fd.position += read;
        _length -= read;

        if(_length > 0){
            fd.chunk = new Chunk(fd.file, Math.floor(fd.position/Chunk.CHUNK_SIZE), self)
            return fd.chunk.load().then(function() {
                return _read(_buffer.slice(read), _length)
            })
        }
        return Promise.resolve();
    }

    return p
        .then(function() {
            return _read(buffer.slice(offset), length)
        })
        .return(length)
        .nodeify(callback)
}

RiakFs.prototype.write = function(fd, buffer, offset, length, position, callback) {
    var self = this;

    offset = offset | 0;
    length = length | 0;

    if(!fd || fd.closed || !fd.file || !/^(w|w\+|wx|wx\+|a|a\+|ax|ax\+)$/.test(fd.flags)){
        return Promise.reject(new RiakFsError('EBADF', 'Invalid file descriptor')).nodeify(callback)
    }

    if(typeof buffer === 'string'){
        buffer = new Buffer(buffer, 'utf8')
    }

    if(!Buffer.isBuffer(buffer)){
        return Promise.reject(new RiakFsError('EINVAL', 'buffer argument should be Buffer or String')).nodeify(callback)
    }

    if(typeof position === 'undefined'){
        position = null
    }

    if(typeof position === 'function'){
        callback = position
        position = null
    }

    if(position !== null){
        position = position | 0;
        if(position > fd.file.size){
            return Promise.reject(new RiakFsError('EINVAL', 'The specified file offset is invalid')).nodeify(callback)
        }
        fd.position = position;
    }

    if(!fd.chunk){
        fd.chunk = new Chunk(fd.file, 0, self)
    }

    fd.modified = true;

    var _write = function(_buffer) {
        var written = fd.chunk.write(_buffer, fd.position - (fd.chunk.n * Chunk.CHUNK_SIZE))
        fd.position += written;

        if(fd.position > fd.file.size){
            fd.file.size = fd.position;
        }

        if(written < _buffer.length){
            return fd.chunk.save().then(function() {
                fd.chunk = new Chunk(fd.file, Math.floor(fd.position/Chunk.CHUNK_SIZE), self)
                return _write(_buffer.slice(written))
            })
        }
        return Promise.resolve();
    }

    return _write(buffer.slice(offset, offset+length))
        .return(length)
        .nodeify(callback)
};

RiakFs.prototype.rename = function(oldName, newName, callback) {
    var self = this;

    newName = path.normalize(newName)
    oldName = path.normalize(oldName)
    var re = new RegExp('^' + XRegExp.escape(oldName))

    if(re.test(newName)){
        return Promise.reject(new RiakFsError('EINVAL', 'old is a parent directory of new')).nodeify(callback)
    }

    function _rename(filename){
        var file, vclock, newFilename = filename.replace(re, newName);

        return Promise.all([
            self.riak.get({
                bucket: self.filesBucket,
                key: filename
            }).then(self.resolver.resolve(filename)),
            self.riak.get({
                bucket: self.filesBucket,
                key: newFilename,
                head: true,
                deletedvclock: true
            })
        ])
        .spread(function(reply, reply2) {
            if(!reply || !reply.content){
                return
            }
            file = reply.content[0].value
            vclock = reply.vclock;

            return self.riak.put({
                bucket: self.filesBucket,
                key: newFilename,
                vclock: reply2.vclock, // for tombstones
                content: {
                    value: JSON.stringify(file),
                    content_type: 'application/json',
                    indexes: [{
                        key: self.directoryIndex,
                        value: path.dirname(newFilename)
                    }]
                }
            })
            .return(file)
        })
        .then(function(file) {
            if(file.isDirectory){
                return self.riak.getIndexAll({
                    bucket: self.filesBucket,
                    index: self.directoryIndex,
                    qtype: 0,
                    key: filename
                }).then(function(search) {
                    if(search){
                        return Promise.map(search.keys, function(key) {
                            return _rename(key)
                        })
                    }
                })
            }
        })
        .then(function() {
            return self.riak.del({
                bucket: self.filesBucket,
                key: filename,
                vclock: vclock
            })
        })
    }

    return Promise.settle([
        self.stat(newName),
        self.stat(path.dirname(newName)),
        self.stat(oldName)
    ])
    .spread(function(newFile, newDir, oldFile) {
        if(oldFile.isRejected() || newDir.isRejected()){
            throw new RiakFsError('ENOENT', 'A component of the old path does not exist, or a path prefix of new does not exist')
        } else {
            oldFile = oldFile.value()
            newDir = newDir.value()
        }

        if(!newDir.isDirectory()){
            throw new RiakFsError('ENOTDIR', 'A component of new path prefix is not a directory')
        }

        if(newFile.isFulfilled()){
            newFile = newFile.value()

            if(oldFile.isDirectory() && !newFile.isDirectory()){
                throw new RiakFsError('ENOTDIR', 'old is a directory, but new is not a directory')
            }
            if(newFile.isDirectory() && !oldFile.isDirectory()){
                throw new RiakFsError('EISDIR', 'new is a directory, but old is not a directory')
            }
            if(newFile.isDirectory()){
                return self.rmdir(newName)
            } else {
                return self.unlink(newName)
            }
        }
    })
    .then(function() {
        return _rename(oldName)
    })
    .nodeify(callback)
};

RiakFs.prototype.stat = function(_path, callback) {
    var self = this;

    _path = path.normalize(_path)

    if(_path === '/'){
        return Promise.resolve(new Stats({
            isDirectory: true,
            ctime: new Date(0),
            mtime: new Date(0)
        })).nodeify(callback)
    }

    return self.riak.get({
        bucket: self.filesBucket,
        key: _path
    })
    .then(self.resolver.resolve(_path))
    .then(function(reply) {
        if(!reply || !reply.content){
            throw new RiakFsError('ENOENT', 'No such file or directory: ' + _path)
        }
        return new Stats(reply.content[0].value)
    })
    .nodeify(callback)
};

RiakFs.prototype.createWriteStream = function(_path, options) {
    return new RiakFsWriteStream(this, _path, options)
};

RiakFs.prototype.writeFile = function(filename, data, options, callback) {
    var self = this;

    if(typeof options === 'function'){
        callback = options;
    }

    options = _.partialRight(_.merge, _.defaults)(options || {}, {
        flags: 'w',
        encoding: 'utf8'
    });

    if(typeof data === 'string'){
        data = new Buffer(data, options.encoding)
    }

    return self.open(filename, options.flags)
        .then(function(fd) {
            return self.write(fd, data, 0, data.length, null)
                .then(function() {
                    return self.close(fd)
                })
        })
        .nodeify(callback)
};

RiakFs.prototype.appendFile = function(filename, data, options, callback) {
    if(typeof options === 'function'){
        callback = options;
    }

    options = _.partialRight(_.merge, _.defaults)(options || {}, {
        flags: 'a',
        encoding: 'utf8'
    });

    return this.writeFile(filename, data, options, callback)
}

RiakFs.prototype.createReadStream = function(_path, options) {
    return new RiakFsReadStream(this, _path, options)
};

RiakFs.prototype.readFile = function(filename, options, callback) {
    var self = this;

    if(typeof options === 'function'){
        callback = options;
    }

    options = _.partialRight(_.merge, _.defaults)(options || {}, {
        flags: 'r',
        encoding: null
    });

    return self.open(filename, options.flags)
        .then(function(fd) {
            var data = new Buffer(fd.file.size)
            return self.read(fd, data, 0, fd.file.size, null)
                .then(function() {
                    return self.close(fd)
                })
                .then(function() {
                    if(!options.encoding){
                        return data
                    }
                    return data.toString(options.encoding)
                })
        })
        .nodeify(callback)
};

RiakFs.prototype.exists = function(_path, callback) {
    var self = this;

    _path = path.normalize(_path)

    return self.riak.get({
        bucket: self.filesBucket,
        key: _path,
        head: true
    })
    .then(self.resolver.resolve(_path))
    .then(function(reply) {
        if(!reply || !reply.content){
            return false
        }
        return true
    })
    .nodeify(callback)
}

RiakFs.prototype.utimes = function(_path, atime, mtime, callback) {
    var self = this;

    return self.open(_path, 'r').then(function(fd) {
        return self.futimes(fd, atime, mtime, callback)
    })
}

RiakFs.prototype.futimes = function(fd, atime, mtime, callback) {
    var self = this;

    if(!fd || fd.closed || !fd.file){
        return Promise.reject(new RiakFsError('EBADF', 'Invalid file descriptor')).nodeify(callback)
    }

    if(typeof mtime === 'number'){
        mtime = new Date(mtime)
    }

    if(!util.isDate(mtime)){
        return Promise.reject(new Error('Invalid time: ' + mtime))
    }

    fd.file.mtime = mtime;

    return self.riak.get({
        bucket: self.filesBucket,
        key: fd.filename,
        head: true
    })
    .then(function(reply) {
        return self.riak.put({
            bucket: self.filesBucket,
            key: fd.filename,
            vclock: reply.vclock,
            content: {
                value: JSON.stringify(fd.file),
                content_type: 'application/json',
                indexes: [{
                    key: self.directoryIndex,
                    value: path.dirname(fd.filename)
                }]
            }
        })
    })
    .nodeify(callback)
}

RiakFs.prototype.makeTree = function(_path, callback) {
    var self = this;

    _path = path.normalize(_path)

    return Promise.reduce(_path.split('/'), function(a, c) {
        a.push(c)
        var p = a.join('/')
        if(p){
            return self.mkdir(p).catch(RiakFsError, function(err) {
                if(err.code !== 'EEXIST'){
                    throw err
                }
            })
            .return(a)
        }
        return a
    }, []).nodeify(callback)
}

RiakFs.prototype.copy = function(from, to, callback) {
    var self = this;

    return new Promise(function(resolve, reject) {
        var readStream = self.createReadStream(from)
        var writeStream = self.createWriteStream(to)
        readStream.on('error', reject)
        writeStream.on('error', reject)
        writeStream.on('close', resolve)
        readStream.pipe(writeStream);
    }).nodeify(callback)
}

RiakFs.prototype.fstat = function(fd, callback) {
    return Promise.resolve(new Stats(fd.file)).nodeify(callback)
}

RiakFs.prototype.truncate = function(_path, len, callback) {
}

RiakFs.prototype.ftruncate = function(fd, len, callback) {
}

