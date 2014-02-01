"use strict";

/* jshint bitwise: false */

var Promise = require('bluebird');
var _       = require('lodash');

var RiakFsSiblingsResolver = function(riakfs) {

    if (!(this instanceof RiakFsSiblingsResolver)){
        return new RiakFsSiblingsResolver(riakfs);
    }

    this.riakfs = riakfs
    this.riak = riakfs.riak;
}

module.exports = RiakFsSiblingsResolver;

RiakFsSiblingsResolver.prototype._compareFiles = function(a, b, last) {
    var self = this;
    if(a._chunksLength === a.value.length && b._chunksLength !== b.value.length){
        return -1
    }

    if(a.deleted && !b.deleted){
        return -1
    }

    if(!last){
        return -1 * self._compareFiles(b, a, true)
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
                                sibling.directoryNotEmty = true
                            }
                            return sibling
                        })
                    } else {
                        return self.riak.getIndexAll({
                            bucket: self.riakfs.chunksBucket,
                            index: self.riakfs.chunksIndex,
                            qtype: 0,
                            key: sibling.value.id
                        }).then(function(search) {
                            if(!search){
                                return 0
                            }
                            return Promise.reduce(search.keys, function(total, key) {
                                return self.riak.get({
                                    bucket: self.riakfs.chunksBucket,
                                    key: key,
                                    head: true
                                }).then(function(_reply) {
                                    if(_reply && _reply.content && _reply.content[0].usermeta){
                                        return total + (_.find(_reply.content[0].usermeta, {
                                            key: 'length'
                                        }).value >> 0)
                                    }
                                })
                            }, 0)
                        }).then(function(size) {
                            sibling._chunksLength = size
                            return sibling
                        })
                    }
                }
                return sibling;
            }).then(function(siblings) {
                siblings.sort(function(a, b) {
                    if(!a.value.isDirectory && !b.value.isDirectory){
                        return self._compareFiles(a, b)
                    }
                })

                console.log(key, siblings, '\n----\n')

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
