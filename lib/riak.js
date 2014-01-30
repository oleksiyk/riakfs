"use strict";

var riakpbc     = require('riakpbc');
var Promise     = require('bluebird');
var genericPool = require('generic-pool');

var RiakClient = function(config) {

    this.pool = genericPool.Pool({
        name     : 'riak',
        create   : function(callback) {
            callback(null, riakpbc.createClient(config));
        },
        max      : 10,
        min      : 5,
        // specifies how long a resource can stay idle in pool before being removed
        idleTimeoutMillis : 30000,
         // if true, logs via console.log - can also be a function
        log : false
    });

}

module.exports = function(_config) {
    return new RiakClient(_config);
};

[
    'getBuckets',
    'getBucket',
    'setBucket',
    'resetBucket',
    'getKeys',
    'put',
    'get',
    'del',
    'mapred',
    'getCounter',
    'updateCounter',
    'getIndex',
    'search',
    'getClientId',
    'setClientId',
    'getServerInfo',
    'ping'
].forEach(function (m) {

        RiakClient.prototype[m] = function () {
            var self = this;
            var args = Array.prototype.slice.call(arguments);

            var p = new Promise(function(resolve, reject) {
                self.pool.acquire(function(err, db) {
                    if(err){
                        return reject(err)
                    }

                    if(m === 'getIndex'){
                        resolve(db[m].apply(db, args.concat([function() {
                            self.pool.release(db)
                        }])))
                    } else {
                        db[m].apply(db, args.concat([function(err, data) {
                            self.pool.release(db)
                            if(err){
                                return reject(err)
                            }

                            resolve(data)
                        }]))
                    }
                })
            })

            return p;
        }

    })

RiakClient.prototype.getIndexAll = function(params) {
    var self = this;
    return self.getIndex(params)
        .then(function(stream) {
            return new Promise(function(resolve, reject) {
                var result = null;
                stream.on('error', function(err) {
                    reject(err)
                })
                stream.on('data', function(data) {
                    if(data && data.keys){
                        if(!result){
                            result = data
                        } else {
                            result.keys = result.keys.concat(data.keys)
                        }
                    }
                })
                stream.on('end', function() {
                    resolve(result)
                })
            })
        })
}

