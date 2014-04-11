"use strict";

/* jshint bitwise: false, maxparams: 6 */

// var XRegExp = require('xregexp').XRegExp;
var path    = require('path');
var _       = require('lodash');
var Promise = require('bluebird');
var util    = require('util');
var uid2    = require('uid2');

var RiakFsError            = require('./error')
var RiakFsWriteStream      = require('./writestream')
var RiakFsReadStream       = require('./readstream')
var Stats                  = require('./stats')
var Chunk                  = require('./chunk')
// var RiakFsSiblingsResolver = require('./resolver')

var RiakFs = function(options, riak) {
    var self = this;

    if (!(this instanceof RiakFs)){
        return new RiakFs(options);
    }

    self.options = options;
    self.riak = riak || require('riakpbc-promised').create(options.riak)
    self.filesBucket = options.root + '.files';
    self.chunksBucket = options.root + '.chunks';
    self.directoryIndex = options.root.toLowerCase() + '.files_directory_bin';

    // self.resolver = new RiakFsSiblingsResolver(self)
}

exports.create = function(options, riak, callback) {
    if(typeof riak === 'function'){
        callback = riak
        riak = undefined
    }
    options = _.partialRight(_.merge, _.defaults)(options || {}, {
        root: 'fs',
        cap: {
            n_val: 3, // If you change n_val after keys have been added to the bucket it may result in failed reads
            /* pr, r, w, pw, dw, rw, basic_quorum */
        },
        // meta_backend: '',
        // chunks_backend: '',
        riak: {
            host: '127.0.0.1',
            port: 8087
        },
        events: false,
        shared: {
            fs: false
        },
        trash: false
    });

    var riakfs = new RiakFs(options, riak)

    return riakfs._setup()
        .return(riakfs)
        .nodeify(callback)
};

require('util').inherits(RiakFs, require('events').EventEmitter);

RiakFs.prototype._normalizePath = function(_path) {
    return path.normalize(_path).replace(/(.+)\/$/, '$1')
}

RiakFs.prototype._setup = function() {
    var self = this;

    return Promise.all([
        self.riak.setBucket({
            bucket: self.filesBucket,
            props: _.defaults({
                allow_mult: false,
                last_write_wins: false,
                backend: self.options.meta_backend
            }, self.options.cap)
        }),
        self.riak.setBucket({
            bucket: self.chunksBucket,
            props: _.defaults({
                allow_mult: false,
                last_write_wins: false,
                backend: self.options.chunks_backend
            }, self.options.cap)
        })
    ])
}

RiakFs.prototype._removeChunks = function(file) {
    var self = this;

    return Promise.concurrencyLimit(_.range(Math.ceil(file.size / Chunk.CHUNK_SIZE)), 10, function(n) {
        var key = file.id + ':' + n
        return self.riak.get({
            bucket: self.chunksBucket,
            key: key,
            head: true
        }).then(function(reply) {
            if (reply && reply.content) {
                return self.riak.del({
                    bucket: self.chunksBucket,
                    key: key,
                    vclock: reply.vclock
                })
            }
        })
    })
}

RiakFs.prototype.unlink = function(_filename, callback) {
    var self = this, file;
    return this._checkShared(_filename).spread(function(fs, filename, readOnly) {
        if(readOnly){
            throw new RiakFsError('EACCES', 'Permission denied: ' + _filename)
        }

        return fs.riak.get({
            bucket: fs.filesBucket,
            key: filename
        })
        //.then(fs.resolver.resolve(filename))
        .then(function(reply) {
            if(!reply || !reply.content){
                throw new RiakFsError('ENOENT', 'No such file or directory: ' + filename)
            }

            file = reply.content[0].value;

            if(file.isDirectory){
                throw new RiakFsError('EISDIR', 'File is a directory: ' + filename)
            }

            if(fs.options.trash === true && !/^\/\.Trash(\/|$)/.test(filename)){
                return (function _test(_path, attempt) {
                    attempt = attempt || 0
                    var testPath = _path + (attempt > 0 ? ('.' + attempt) : '')
                    return fs.stat(testPath)
                    .then(function () {
                        return _test(_path, ++attempt)
                    })
                    .catch(function (err) {
                        if(err.code !== 'ENOENT'){
                            throw err
                        }
                        return self._normalizePath(testPath)
                    })
                })('/.Trash/' + filename).then(function (_path) {
                    return fs.makeTree(path.dirname(_path))
                    .then(function () {
                        return fs._rename(filename, _path)
                    })
                })
            } else {
                return fs._removeChunks(file).then(function() {
                    return fs.riak.del({
                        bucket: fs.filesBucket,
                        key: filename,
                        vclock: reply.vclock
                    })
                })
            }
        })
        .then(function() {
            if(fs.options.events){
                fs.emit('delete', filename, file)
            }
        })
    })
    .nodeify(callback)
};

RiakFs.prototype.readdir2 = function(__path, max, marker, callback) {
    var self = this;

    return self._checkShared(__path).spread(function(fs, _path) {

        var searchParams = {
            bucket: fs.filesBucket,
            index: fs.directoryIndex,
            qtype: 0,
            key: _path,
            pagination_sort: true
        }

        if(max){
            searchParams.max_results = max
        }

        if(marker){
            searchParams.continuation = marker
        }

        return Promise.all([
            fs.riak.get({
                bucket: fs.filesBucket,
                key: _path
            })/*.then(fs.resolver.resolve(_path))*/,
            fs.riak.getIndexAll(searchParams)
        ])
        .spread(function(reply, search) {
            if(_path !== '/'){
                if(!reply || !reply.content){
                    throw new RiakFsError('ENOENT', 'No such file or directory:' + __path)
                }
                if(!reply.content[0].value.isDirectory){
                    throw new RiakFsError('ENOTDIR', 'Not a directory: ' + __path)
                }
            }

            if(search && search.keys && search.keys.length){
                search.keys = search.keys.map(function(f) {
                    return path.basename(f)
                })
            }
            return search
        })
    })
    .nodeify(callback)
};


RiakFs.prototype.readdir = function(_path, callback) {
    var self = this;

    return self.readdir2(_path).then(function(search) {
        if(search && search.keys){
            return search.keys
        }
        return []
    }).nodeify(callback)
};

RiakFs.prototype.mkdir = function(__path, mode, callback) {
    if(typeof mode === 'function'){
        callback = mode
    }

    return this._checkShared(__path).spread(function(fs, _path, readOnly) {
        var d = path.dirname(_path), p = Promise.resolve(true);

        if(_path === '/'){
            throw new RiakFsError('EEXIST', 'Path already exists: /')
        }

        if(readOnly){
            throw new RiakFsError('EACCES', 'Readonly share: ' + __path)
        }

        if (d !== '/') {
            p = fs.riak.get({
                bucket: fs.filesBucket,
                key: d
            })
            // .then(fs.resolver.resolve(d))
        }

        return Promise.all([
            fs.riak.get({
                bucket: fs.filesBucket,
                key: _path,
                deletedvclock: true
            }), p
        ]).spread(function(reply, parentReply) {
            if (d !== '/') {
                if(!parentReply || !parentReply.content || !parentReply.content[0].value){
                    throw new RiakFsError('ENOENT', 'No such file or directory: ' + path.dirname(__path))
                }

                if(!parentReply.content[0].value.isDirectory){
                    throw new RiakFsError('ENOTDIR', 'A component of the path prefix is not a directory: ' + __path)
                }
            }

            if(reply && reply.content){
                throw new RiakFsError('EEXIST', 'Path already exists: ' + __path)
            }

            var value = {
                ctime: new Date(),
                mtime: new Date(),
                isDirectory: true
            }

            p = fs.riak.put({
                bucket: fs.filesBucket,
                key: _path,
                vclock: reply.vclock, // tombstone vclock
                content: {
                    value: JSON.stringify(value),
                    content_type: 'application/json',
                    indexes: [{
                        key: fs.directoryIndex,
                        value: d
                    }]
                }
            })

            if(fs.options.events){
                p = p.then(function() {
                    fs.emit('new', _path, value)
                })
            }

            return p
        })
    }).nodeify(callback)
};

RiakFs.prototype.rmdir = function(__path, callback) {
    return this._checkShared(__path).spread(function(fs, _path, readOnly) {
        if(_path === '/'){
            throw new RiakFsError('EACCES', 'Cannot remove /')
        }

        if(readOnly){
            throw new RiakFsError('EACCES', 'Readonly share: ' + __path)
        }

        return Promise.all([
            fs.riak.get({
                bucket: fs.filesBucket,
                key: _path
            })/*.then(fs.resolver.resolve(_path))*/,
            fs.riak.getIndexAll({
                bucket: fs.filesBucket,
                index: fs.directoryIndex,
                qtype: 0,
                max_results: 1,
                key: _path
            })
        ])
        .spread(function(reply, search) {
            var dir;
            if(!reply || !reply.content){
                throw new RiakFsError('ENOENT', 'No such file or directory:' + __path)
            }
            if(!reply.content[0].value.isDirectory){
                throw new RiakFsError('ENOTDIR', 'Not a directory: ' + __path)
            }
            dir = reply.content[0].value
            if(search && search.keys.length){
                throw new RiakFsError('ENOTEMPTY', 'Directory not empty: ' + __path)
            }

            if(dir.share && dir.share.to.length){
                throw new RiakFsError('ESHARED', 'Directory is shared: ' + __path)
            }

            var p = fs.riak.del({
                bucket: fs.filesBucket,
                key: _path,
                vclock: reply.vclock
            })

            if(fs.options.events){
                p = p.then(function() {
                    fs.emit('delete', _path, reply.content[0].value)
                })
            }

            return p
        })
    })
    .nodeify(callback)
};

RiakFs.prototype.open = function(_filename, flags, mode, callback) {
    var self = this, d, p = Promise.resolve({isDirectory: true}), options = {};

    if(typeof _filename === 'object'){
        options = _filename;
        _filename = options.filename;
    }

    if(typeof mode === 'function'){
        callback = mode;
    }

    if(!/^(r|r\+|w|wx|w\+|wx\+|a|ax|a\+|ax\+)$/.test(flags)){
        return Promise.reject(new RiakFsError('EINVAL', 'Invalid flags given: ' + flags)).nodeify(callback)
    }

    return self._checkShared(_filename).spread(function(fs, filename, readOnly) {

        filename = fs._normalizePath(filename)
        d = path.dirname(filename)

        if (d !== '/') {
            p = fs.riak.get({
                bucket: fs.filesBucket,
                key: d,
            })
            // .then(fs.resolver.resolve(d))
            .then(function(reply) {
                if(reply && reply.content){
                    return reply.content[0].value
                }
            })
        }

        return Promise.all([
            fs.riak.get({
                bucket: fs.filesBucket,
                key: filename,
                deletedvclock: true
            })/*.then(fs.resolver.resolve(filename))*/,
            p
        ])
        .spread(function(reply, parent) {
            var file = null;
            if(reply && reply.content){
                file = reply.content[0].value
            }
            if(!file && /^(r|r\+)$/.test(flags)){
                throw new RiakFsError('ENOENT', 'No such file or directory: ' + _filename)
            }
            if(file && file.isDirectory){
                throw new RiakFsError('EISDIR', 'File is a directory: ' + _filename)
            }
            if(!parent){
                throw new RiakFsError('ENOENT', 'No such file or directory: ' + path.dirname(_filename))
            }
            if(!parent.isDirectory){
                throw new RiakFsError('ENOTDIR', 'Not a directory: ' + path.dirname(_filename))
            }
            if(file && /^(wx|wx\+|ax|ax\+)$/.test(flags)){
                throw new RiakFsError('EEXIST', 'File already exists: ' + _filename)
            }

            if(/^(w|w\+|a|a\+|wx|wx\+|ax|ax\+)$/.test(flags)){
                if(readOnly){
                    throw new RiakFsError('EACCES', 'Permission denied: ' + _filename)
                }
                if(file && /^(w|w\+)$/.test(flags)){
                    // truncate file, remove all existing chunks
                    return fs._removeChunks(file).then(function() {
                        file.size = 0;
                        file.contentType = undefined;
                        if(options.meta){
                            file.meta = options.meta
                        }
                        return {
                            flags: flags,
                            position: 0,
                            file: file,
                            fs: fs,
                            vclock: reply.vclock,
                            indexes: reply.content[0].indexes,
                            filename: filename
                        }
                    })
                }
                if(file && /^(a|a\+)$/.test(flags)){
                    return {
                        flags: flags,
                        position: file.size,
                        file: file,
                        fs: fs,
                        vclock: reply.vclock,
                        indexes: reply.content[0].indexes,
                        filename: filename
                    }
                }
                if(!file){ // create new file
                    file = {
                        id: uid2(32),
                        ctime: new Date(),
                        mtime: new Date(),
                        size: 0,
                        contentType: 'binary/octet-stream',
                        version: -1
                    }
                    if(options.meta){
                        file.meta = options.meta
                    }
                    if(options.indexes){
                        options.indexes = options.indexes.map(function(ind) {
                            return {
                                key: fs.options.root + '.files_custom_' + ind.key,
                                value: ind.value
                            }
                        })
                    }
                    return fs.riak.put({
                        bucket: fs.filesBucket,
                        key: filename,
                        vclock: reply.vclock, // tombstone vclock
                        content: {
                            value: JSON.stringify(file),
                            content_type: 'application/json',
                            indexes: [{
                                key: fs.directoryIndex,
                                value: d
                            }].concat(options.indexes)
                        },
                        return_head: true
                    }).then(function(_reply) {
                        return {
                            isNew: true,
                            flags: flags,
                            position: 0,
                            filename: filename,
                            vclock: _reply.vclock,
                            indexes: _reply.content[0].indexes,
                            file: file,
                            fs: fs
                        }
                    })
                }
            }

            // flags = r or r+
            return {
                flags: flags,
                file: file,
                fs: fs,
                position: 0,
                vclock: reply.vclock,
                indexes: reply.content[0].indexes,
                filename: filename
            }
        })
    }).nodeify(callback)
};

RiakFs.prototype.close = function(fd, callback) {
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
            fd.file.mtime = new Date()
            fd.file.version += 1

            return fd.fs.riak.put({
                bucket: fd.fs.filesBucket,
                key: fd.filename,
                vclock: fd.vclock,
                content: {
                    value: JSON.stringify(fd.file),
                    content_type: 'application/json',
                    indexes: fd.indexes
                }
            })
        })
    }

    if(fd.fs.options.events === true && (fd.modified || fd.isNew)){
        p = p.then(function() {
            fd.fs.emit(fd.isNew ? 'new' : 'change', fd.filename, fd.file)
        })
    }

    return p.nodeify(callback)
};

RiakFs.prototype.read = function(fd, buffer, offset, length, position, callback) {
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

    if(fd.file.size === 0 || length === 0){
        return Promise.resolve(0)
    }

    var p = Promise.resolve(), n = Math.floor(fd.position/Chunk.CHUNK_SIZE)

    if(!fd.chunk || fd.chunk.n !== n){
        fd.chunk = new Chunk(fd.file, n, fd.fs)
        p = fd.chunk.load()
    }

    var _read = function(_buffer, _length) {
        var read = fd.chunk.read(_buffer, fd.position-(fd.chunk.n*Chunk.CHUNK_SIZE), _length)
        fd.position += read;
        _length -= read;

        if(_length > 0){
            fd.chunk = new Chunk(fd.file, Math.floor(fd.position/Chunk.CHUNK_SIZE), fd.fs)
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

    var p = Promise.resolve(), n = fd.position === 0 ? 0 : (Math.ceil(fd.position/Chunk.CHUNK_SIZE) - 1)

    if(!fd.chunk || fd.chunk.n !== n){
        fd.chunk = new Chunk(fd.file, n, fd.fs)
        if(n * Chunk.CHUNK_SIZE < fd.file.size){
            p = fd.chunk.load()
        }
    }

    fd.modified = true;

    var _write = function(_buffer) {
        var written = fd.chunk.write(_buffer, fd.position - (fd.chunk.n * Chunk.CHUNK_SIZE))
        fd.position += written;

        if(fd.position > fd.file.size){
            fd.file.size = fd.position;
        }

        if(written < _buffer.length){
            var _ps = fd.chunk.save()
            fd.chunk = new Chunk(fd.file, Math.floor(fd.position/Chunk.CHUNK_SIZE), fd.fs)
            return Promise.all([
                _ps,
                _write(_buffer.slice(written))
            ])
        }
        return Promise.resolve();
    }

    return p
        .then(function() {
            return _write(buffer.slice(offset, offset+length))
        })
        .return(length)
        .nodeify(callback)
};

RiakFs.prototype._rename = function(_old, _new) {
    var self = this, file, vclock;

    return Promise.all([
        self.riak.get({
            bucket: self.filesBucket,
            key: _old
        })/*.then(self.resolver.resolve(_old))*/,
        self.riak.get({
            bucket: self.filesBucket,
            key: _new,
            head: true,
            deletedvclock: true
        })
    ])
    .spread(function(reply, reply2) { // create new file record
        if(!reply || !reply.content){
            throw new RiakFsError('ENOENT', 'No such file or directory: ' + _old)
        }
        file = reply.content[0].value
        vclock = reply.vclock;
        file.mtime = new Date()

        var customIndexes = _.filter(reply.content[0].indexes, function(ind) {
            return ind.key !== self.directoryIndex
        })

        return self.riak.put({
            bucket: self.filesBucket,
            key: _new,
            vclock: reply2.vclock, // for tombstones
            content: {
                value: JSON.stringify(file),
                content_type: 'application/json',
                indexes: [{
                    key: self.directoryIndex,
                    value: path.dirname(_new)
                }].concat(customIndexes)
            }
        })
    })
    .then(function() { // update shares
        if(file && typeof self.options.shared.fs === 'function'){
            if(file.share){ // source share, update destinations
                return Promise.map(file.share.to, function(to) {
                    return self.options.shared.fs(to.root).then(function(_tfs) {
                        var dstPath = '/Shared/' + to.alias
                        return _tfs.riak.get({
                            bucket: _tfs.filesBucket,
                            key: dstPath
                        }).then(function(dreply) {
                            if(!dreply || !dreply.content){
                                return
                            }
                            var tdir = dreply.content[0].value;
                            if(!tdir || !tdir._sharedFrom || !tdir.isDirectory){
                                return
                            }
                            tdir._sharedFrom.path = _new
                            return _tfs.riak.put({
                                bucket: _tfs.filesBucket,
                                key: dstPath,
                                vclock: dreply.vclock,
                                content: {
                                    value: JSON.stringify(tdir),
                                    content_type: 'application/json',
                                    indexes: [{
                                        key: _tfs.directoryIndex,
                                        value: path.dirname(dstPath)
                                    }]
                                }
                            })
                        })
                    })
                })
            } else if(file._sharedFrom){ // destination share, update source
                return self.options.shared.fs(file._sharedFrom.root).then(function(_tfs) {
                    return _tfs.riak.get({
                        bucket: _tfs.filesBucket,
                        key: file._sharedFrom.path
                    }).then(function(dreply) {
                        if(!dreply || !dreply.content){
                            return
                        }
                        var tdir = dreply.content[0].value;
                        if(!tdir || !tdir.share || !tdir.isDirectory){
                            return
                        }
                        var shareInd = _.findIndex(tdir.share.to, { root: self.options.root })
                        if(shareInd === -1){
                            return
                        }
                        tdir.share.to[shareInd].alias = path.basename(_new)
                        return _tfs.riak.put({
                            bucket: _tfs.filesBucket,
                            key: file._sharedFrom.path,
                            vclock: dreply.vclock,
                            content: {
                                value: JSON.stringify(tdir),
                                content_type: 'application/json',
                                indexes: [{
                                    key: _tfs.directoryIndex,
                                    value: path.dirname(file._sharedFrom.path)
                                }]
                            }
                        })
                    })
                })
            }
        }
    })
    .then(function() { // recursively walk directory
        if(file && file.isDirectory){
            return self.riak.getIndexAll({
                bucket: self.filesBucket,
                index: self.directoryIndex,
                qtype: 0,
                key: _old
            }).then(function(search) {
                if(search){
                    return Promise.concurrencyLimit(search.keys, 10, function(key) {
                        return self._rename(key, key.replace(_old, _new))
                    })
                }
            })
        }
    })
    .then(function() { // delete old file record
        return self.riak.del({
            bucket: self.filesBucket,
            key: _old,
            vclock: vclock
        })
    })
}

// cross filesystem
function _move(ofs, from, nfs, to){
    return ofs.stat(from).then(function(stats) {
        if(stats.isFile()){
            return nfs.makeTree(path.dirname(to)).then(function() {
                return new Promise(function(resolve, reject) {
                    var readStream = ofs.createReadStream(from)
                    var writeStream = nfs.createWriteStream(to)
                    readStream.on('error', reject)
                    writeStream.on('error', reject)
                    writeStream.on('close', resolve)
                    readStream.pipe(writeStream);
                })
            }).then(function() {
                return ofs.unlink(from)
            })
        } else {
            return ofs.readdir(from).then(function(list) {
                return Promise.reduce(list, function(a, f) {
                    return _move(ofs, from + '/' + f, nfs, to + '/' + f)
                }, 0)
            }).then(function() {
                return ofs.rmdir(from)
            })
        }
    })
}

RiakFs.prototype.rename = function(_oldName, _newName, callback) {
    var self = this;
    return Promise.all([
        this._checkShared(_oldName),
        this._checkShared(_newName)
    ]).spread(function(_old, _new) {
        var ofs = _old[0], nfs = _new[0], oldFile, oldName = _old[1], newName = _new[1];

        if(_old[2] || _new[2]){
            throw new RiakFsError('EACCES', 'Permission denied')
        }

        if(path.dirname(newName) === oldName){
            throw new RiakFsError('EINVAL', 'old is a parent directory of new')
        }

        if(oldName === '/' || oldName === '/Shared' || oldName === '/.Trash'){
            throw new RiakFsError('EINVAL', 'Cannot rename / or /Shared or /.Trash')
        }

        return Promise.settle([
            nfs.stat(newName),
            nfs.stat(path.dirname(newName)),
            ofs.stat(oldName)
        ])
        .spread(function(newFile, newDir, _oldFile) {
            if(_oldFile.isRejected() || newDir.isRejected()){
                throw new RiakFsError('ENOENT', 'A component of the old path does not exist, or a path prefix of new does not exist')
            } else {
                oldFile = _oldFile.value()
                newDir = newDir.value()
            }

            if(!newDir.isDirectory()){
                throw new RiakFsError('ENOTDIR', 'A component of new path prefix is not a directory')
            }

            if(oldFile.file.share && (path.dirname(newName) !== '/Shared' || nfs.options.root !== self.options.root)){
                throw new RiakFsError('EINVAL', 'Cannot move shared directories')
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
                    return nfs.rmdir(newName)
                } else {
                    return nfs.unlink(newName)
                }
            }
        })
        .then(function() {
            var p;

            if(oldFile.file.share){ // rename the destination share dir (not move)
                ofs = nfs;
                oldName = _oldName;
            }

            if(nfs.options.root === ofs.options.root){
                p = nfs._rename(oldName, newName)

                if(nfs.options.events === true){
                    p = p.then(function() {
                        if(/^\/\.Trash(\/|$)/.test(newName)){
                            nfs.emit('delete', oldName, oldFile.file)
                        } else {
                            nfs.emit('rename', oldName, newName, oldFile.file)
                        }
                    })
                }
            } else {
                p = _move(ofs, oldName, nfs, newName)
            }

            return p
        })
    })
    .nodeify(callback)
};

RiakFs.prototype.stat = function(__path, callback) {
    return this._checkShared(__path).spread(function(fs, _path, readOnly) {

        if(_path === '/'){
            return new Stats({
                isDirectory: true,
                ctime: new Date(0),
                mtime: new Date(0)
            })
        }

        return fs.riak.get({
            bucket: fs.filesBucket,
            key: _path
        })
        // .then(fs.resolver.resolve(_path))
        .then(function(reply) {
            if(!reply || !reply.content){
                throw new RiakFsError('ENOENT', 'No such file or directory: ' + __path)
            }
            return new Stats(reply.content[0].value, readOnly)
        })
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
    return this._checkShared(_path).spread(function(fs, _path) {
        if(_path === '/'){
            return true
        }

        return fs.riak.get({
            bucket: fs.filesBucket,
            key: _path,
            head: true
        })
        // .then(fs.resolver.resolve(_path))
        .then(function(reply) {
            if(!reply || !reply.content){
                return false
            }
            return true
        })
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
    if(!fd || fd.closed || !fd.file){
        return Promise.reject(new RiakFsError('EBADF', 'Invalid file descriptor')).nodeify(callback)
    }

    if(atime !== null){
        if(typeof atime === 'number'){
            atime = new Date(atime)
        }

        if(!util.isDate(atime)){
            return Promise.reject(new Error('Invalid atime: ' + atime))
        }
        fd.file.atime = atime;
    }

    if(mtime !== null){
        if(typeof mtime === 'number'){
            mtime = new Date(mtime)
        }

        if(!util.isDate(mtime)){
            return Promise.reject(new Error('Invalid mtime: ' + mtime))
        }

        fd.file.mtime = mtime;
    }

    if(atime === null && mtime === null){
        return Promise.resolve().nodeify(callback)
    }

    return fd.fs.riak.put({
        bucket: fd.fs.filesBucket,
        key: fd.filename,
        vclock: fd.vclock,
        content: {
            value: JSON.stringify(fd.file),
            content_type: 'application/json',
            indexes: [{
                key: fd.fs.directoryIndex,
                value: path.dirname(fd.filename)
            }]
        }
    })
    .nodeify(callback)
}

RiakFs.prototype.makeTree = function(_path, callback) {
    var self = this;

    _path = self._normalizePath(_path)

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

RiakFs.prototype.findAll = function(search) {
    var self = this;

    return self.riak.getIndexAll(_.partialRight(_.merge, _.defaults)({
        bucket: self.filesBucket,
        index: self.options.root + '.files_custom_' + search.index,
        qtype: search.qtype || 0
    }, search)).then(function(search) {
        return search
    })
}

RiakFs.prototype._updateMeta = function(_filename, data, mergeFn) {
    return this._checkShared(_filename).spread(function(fs, filename, readOnly) {
        if(readOnly){
            throw new RiakFsError('EACCES', 'Permission denied: ' + _filename)
        }

        return fs.riak.get({
            bucket: fs.filesBucket,
            key: filename
        })
        // .then(fs.resolver.resolve(filename))
        .then(function(reply) {
            var file;
            if(!reply || !reply.content){
                throw new RiakFsError('ENOENT', 'No such file or directory: ' + _filename)
            }
            file = reply.content[0].value
            if(file.isDirectory){
                throw new RiakFsError('EISDIR', 'File is a directory: ' + _filename)
            }

            file.meta = mergeFn(data.meta, file.meta);

            if(data.indexes){
                data.indexes = data.indexes.map(function(ind) {
                    return {
                        key: fs.options.root + '.files_custom_' + ind.key,
                        value: ind.value
                    }
                })
            }

            return fs.riak.put({
                bucket: fs.filesBucket,
                key: filename,
                vclock: reply.vclock,
                content: {
                    value: JSON.stringify(file),
                    content_type: 'application/json',
                    indexes: [{
                        key: fs.directoryIndex,
                        value: path.dirname(filename)
                    }].concat(data.indexes)
                }
            })
        })
    })
}

RiakFs.prototype.updateMeta = function(filename, data, callback) {
    return this._updateMeta(filename, data, function(dm) {
        return dm
    }).nodeify(callback)
}

RiakFs.prototype.setMeta = function(filename, data, callback) {
    return this._updateMeta(filename, data, function(dm, fm) {
        return _.partialRight(_.merge, _.defaults)(dm || {}, fm || {})
    }).nodeify(callback)
}

RiakFs.prototype.fstat = function(fd, callback) {
    return Promise.resolve(new Stats(fd.file)).nodeify(callback)
}

RiakFs.prototype._checkShared = function(_path) {
    var self = this, readOnly = false;

    _path = self._normalizePath(_path)

    return new Promise(function(resolve) {
        var p;
        if(typeof self.options.shared.fs === 'function' && /^\/Shared\//.test(_path)){
            p = _path.split('/')
            p = '/' + p[1] + '/' + p[2]

            resolve(self.riak.get({
                bucket: self.filesBucket,
                key: p
            })
            // .then(self.resolver.resolve(_path))
            .then(function(reply) {
                if(!reply || !reply.content){
                    return self
                }
                var d = reply.content[0].value
                if(d._sharedFrom){
                    readOnly = d._sharedFrom.readOnly || false
                    _path = self._normalizePath(_path.replace(p, d._sharedFrom.path));
                    return self.options.shared.fs(d._sharedFrom.root)
                } else {
                    return self
                }
            }))

        } else {
            resolve(self)
        }
    }).then(function(_fs) {
        return [_fs, _path, readOnly]
    })
}

RiakFs.prototype.share = function(_path, root, alias, readOnly, callback) {
    var self = this, dstPath = '/Shared/' + alias;

    _path = self._normalizePath(_path)

    if(typeof self.options.shared.fs !== 'function'){
        return Promise.reject(new RiakFsError('EINVAL', 'options.shared.fs is not a function')).nodeify(callback)
    }

    if(root === this.options.root){
        return Promise.reject(new RiakFsError('EINVAL', 'Cannot share with itself')).nodeify(callback)
    }

    if(_path === '/'){
        return Promise.reject(new RiakFsError('EINVAL', 'Cannot share /')).nodeify(callback)
    }

    if(/^\/Shared\/?/.test(_path)){
        return Promise.reject(new RiakFsError('EINVAL', 'Cannot share from /Shared')).nodeify(callback)
    }

    if(/^\/\.Trash(\/|$)/.test(_path)){
        return Promise.reject(new RiakFsError('EINVAL', 'Cannot share from /.Trash')).nodeify(callback)
    }

    if(/\//.test(alias)){
        return Promise.reject(new RiakFsError('EINVAL', 'Alias cannot be a path')).nodeify(callback)
    }

    return Promise.all([
        self.riak.get({
            bucket: self.filesBucket,
            key: _path
        })/*.then(self.resolver.resolve(_path))*/,
        self.options.shared.fs(root)
    ])
    .spread(function(sreply, _tfs) {
        var dir
        if(!sreply || !sreply.content){
            throw new RiakFsError('ENOENT', 'No such file or directory: ' + _path)
        }
        dir = sreply.content[0].value
        if(!dir.isDirectory){
            throw new RiakFsError('ENOTDIR', 'Not a directory: ' + _path)
        }
        if(dir._sharedFrom){
            throw new RiakFsError('EACCES', 'Permission denied: ' + _path)
        }
        if(!_tfs instanceof RiakFs){
            throw new RiakFsError('EINVAL', 'options.shared.fs should return RiakFs object')
        }
        if(dir.share && dir.share.to.length){
            if(_.find(dir.share.to, { root: root })){
                throw new RiakFsError('ESHARED', 'Already shared')
            }
        }

        return _tfs.makeTree(dstPath).then(function() {
            return Promise.all([
                _tfs.riak.get({
                    bucket: _tfs.filesBucket,
                    key: dstPath
                }),
                _tfs.riak.getIndexAll({
                    bucket: _tfs.filesBucket,
                    index: _tfs.directoryIndex,
                    qtype: 0,
                    max_results: 1,
                    key: dstPath
                })
            ])
            .spread(function(dreply, search) {
                if(!dreply || !dreply.content){
                    throw new RiakFsError('ENOENT', 'No such file or directory: ' + dstPath)
                }
                if(search && search.keys.length){
                    throw new RiakFsError('ENOTEMPTY', 'Directory not empty: ' + dstPath)
                }
                var tdir = dreply.content[0].value
                if(!tdir.isDirectory){
                    throw new RiakFsError('ENOTDIR', 'Not a directory: ' + dstPath)
                }

                tdir._sharedFrom = {
                    root: self.options.root,
                    path: _path,
                    readOnly: readOnly || false
                }

                return _tfs.riak.put({
                    bucket: _tfs.filesBucket,
                    key: dstPath,
                    vclock: dreply.vclock,
                    content: {
                        value: JSON.stringify(tdir),
                        content_type: 'application/json',
                        indexes: [{
                            key: _tfs.directoryIndex,
                            value: path.dirname(dstPath)
                        }]
                    }
                })
            })
        })
        .then(function() {
            if(!dir.share){
                dir.share = {
                    to: [],
                    owner: {
                        root: self.options.root,
                        path: _path
                    }
                }
            }

            dir.share.to.push({
                root: root,
                alias: alias,
                readOnly: readOnly || false
            })

            return self.riak.put({
                bucket: self.filesBucket,
                key: _path,
                vclock: sreply.vclock,
                content: {
                    value: JSON.stringify(dir),
                    content_type: 'application/json',
                    indexes: [{
                        key: self.directoryIndex,
                        value: path.dirname(_path)
                    }]
                }
            })
        })
    })
    .nodeify(callback)
}

RiakFs.prototype.unshare = function(__path, root, callback) {
    if(typeof root === 'function'){
        callback = root
    }
    if(typeof root !== 'string' || !root){
        root = this.options.root
    }

    return this._checkShared(__path).spread(function(fs, _path) {
        if(typeof fs.options.shared.fs !== 'function'){
            throw new RiakFsError('EINVAL', 'options.shared.fs is not a function')
        }

        if(_path === '/'){
            throw new RiakFsError('EINVAL', 'Cannot opearate on root')
        }

        return fs.riak.get({
            bucket: fs.filesBucket,
            key: _path
        })
        // .then(fs.resolver.resolve(_path))
        .then(function(reply) {
            var dir, shareInd, dstPath
            if(!reply || !reply.content){
                throw new RiakFsError('ENOENT', 'No such file or directory: ' + _path)
            }
            dir = reply.content[0].value
            if(!dir.isDirectory){
                throw new RiakFsError('ENOTDIR', 'Not a directory: ' + _path)
            }
            if(dir.share && dir.share.to.length){
                shareInd = _.findIndex(dir.share.to, { root: root })
                if(shareInd === -1){
                    return
                    // throw new RiakFsError('ENOTSHARED', _path + ' is not shared with ' + root)
                }
                dstPath = '/Shared/' + dir.share.to[shareInd].alias
                dir.share.to.splice(shareInd, 1)
                if(dir.share.to.length === 0){
                    delete dir.share
                }
                return fs.riak.put({
                    bucket: fs.filesBucket,
                    key: _path,
                    vclock: reply.vclock,
                    content: {
                        value: JSON.stringify(dir),
                        content_type: 'application/json',
                        indexes: [{
                            key: fs.directoryIndex,
                            value: path.dirname(_path)
                        }]
                    }
                }).then(function() {
                    return fs.options.shared.fs(root)
                }).then(function(_tfs) {
                    return _tfs.riak.get({
                        bucket: _tfs.filesBucket,
                        key: dstPath
                    }).then(function(dreply) {
                        if(!dreply || !dreply.content){
                            throw new RiakFsError('ENOENT', 'No such file or directory: ' + dstPath)
                        }
                        if(!dreply.content[0].value.isDirectory){
                            throw new RiakFsError('ENOTDIR', 'Not a directory: ' + dstPath)
                        }
                        return _tfs.riak.del({
                            bucket: _tfs.filesBucket,
                            key: dstPath,
                            vclock: dreply.vclock
                        })
                    })
                })
            }
        })
    }).nodeify(callback)
}

/*RiakFs.prototype.truncate = function(_path, len, callback) {
}

RiakFs.prototype.ftruncate = function(fd, len, callback) {
}*/

// helper
Promise.concurrencyLimit = function(arr, limit, f) {
    var _arr = [], start = 0, len = arr.length;

    if(arr.length === 1){
        return f(arr[0])
    }

    while (start < len){
        _arr.push(arr.slice(start, start + limit))
        start += limit
    }

    return Promise.reduce(_arr, function(a, o) {
        return Promise.all(o.map(f))
    }, 0)
};



