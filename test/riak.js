"use strict";

/* global describe, it */

describe('Riak client', function() {

    var riakClient = require('../lib/riak')();

    it('#getServerInfo', function() {
        return riakClient.getServerInfo()
            .then(function(info) {
                info.should.have.property('node')
                info.should.have.property('server_version')
            })
    })

})
