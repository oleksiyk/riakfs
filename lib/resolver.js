"use strict";

/* jshint bitwise: false */

var Promise = require('bluebird');
var _       = require('lodash');
var Chunk   = require('./chunk')

var RiakFsSiblingsResolver = function(riakfs) {

    if (!(this instanceof RiakFsSiblingsResolver)){
        return new RiakFsSiblingsResolver(riakfs);
    }

    this.riakfs = riakfs
    this.riak = riakfs.riak;
}

module.exports = RiakFsSiblingsResolver;

RiakFsSiblingsResolver.prototype._compareFiles = function(a, b, reverse) {
    var self = this;

    // delete file wins
    if(a.deleted && !b.deleted){
        return -1
    }

    // file with matching content wins
    if(a._chunksLength === a.value.size && b._chunksLength !== b.value.size){
        return -1
    }

    // not empty file wins
    if(a._chunksLength === a.value.size && b._chunksLength === b.value.size && b._chunksLength === 0){
        return -1
    }

    if(!reverse){
        return -1 * self._compareFiles(b, a, true)
    }

    if(a.last_mod >= b.last_mod){
        return -1
    }

    return 1
}

RiakFsSiblingsResolver.prototype._compareFileAndDirectory = function(file, dir) {

    // dir wins over broken file
    if(file._chunksLength !== file.value.size){
        return 1
    }

    // not empty dir wins
    if(dir._directoryNotEmpty){
        return 1
    }

    // any dir wins over deleted file
    if(file.deleted){
        return 1
    }

    return -1
}

RiakFsSiblingsResolver.prototype._compareDirectories = function(a, b) {

    // not empty dir wins
    if(a._directoryNotEmpty && !b._directoryNotEmpty){
        return -1
    } else if(b._directoryNotEmpty){
        return 1
    }

    if(a.last_mod >= b.last_mod){
        return -1
    }

    return 1
}

RiakFsSiblingsResolver.prototype.resolve = function(key) {
    var self = this;

    return function(reply) {
        if (!reply || !reply.content || !reply.vclock) {
            return reply;
        }

        if (reply.content.length > 1) {

            return Promise.map(reply.content, function(sibling) {
                if(sibling.value){
                    if(sibling.value.isDirectory){
                        return self.riak.getIndexAll({
                            bucket: self.riakfs.filesBucket,
                            index: self.riakfs.directoryIndex,
                            qtype: 0,
                            max_results: 1,
                            key: key
                        }).then(function(search) {
                            if(search && search.keys.length){
                                sibling._directoryNotEmpty = true
                            }
                            return sibling
                        })
                    } else {
                        return Promise.reduce(_.range(Math.ceil(sibling.value.size / Chunk.CHUNK_SIZE)), function(total, n) {
                            var key = sibling.value.id + ':' + n
                            return self.riak.get({
                                bucket: self.riakfs.chunksBucket,
                                key: key,
                                head: true
                            }).then(function(_reply) {
                                if (_reply && _reply.content && _reply.content[0].usermeta) {
                                    return total + (_.find(_reply.content[0].usermeta, {
                                        key: 'length'
                                    }).value >> 0)
                                }
                            })
                        }, 0).then(function(size) {
                            sibling._chunksLength = size
                            return sibling
                        })
                    }
                }
                return sibling;
            }).then(function(siblings) {

                // console.log(require('util').inspect(siblings, true, 10, true))

                siblings.sort(function(a, b) {
                    if(!a.value.isDirectory && !b.value.isDirectory){
                        return self._compareFiles(a, b)
                    }

                    if(!a.value.isDirectory && b.value.isDirectory){
                        return self._compareFileAndDirectory(a, b)
                    }

                    if(a.value.isDirectory && !b.value.isDirectory){
                        return -1 * self._compareFileAndDirectory(a, b)
                    }

                    if(a.value.isDirectory && b.value.isDirectory){
                        return self._compareDirectories(a, b)
                    }
                })

                //TODO: is it safe (in getting a race condition) to immediately write resolved value back?
                if(siblings[0].deleted){
                    return self.riak.del({
                        bucket: self.riakfs.filesBucket,
                        key: key,
                        vclock: reply.vclock
                    })
                } else {
                    siblings[0].value = JSON.stringify(siblings[0].value)
                    return self.riak.put({
                        bucket: self.riakfs.filesBucket,
                        key: key,
                        vclock: reply.vclock,
                        content: siblings[0],
                        return_body: true
                    })
                }
            })
        }

        return reply;
    }
}
