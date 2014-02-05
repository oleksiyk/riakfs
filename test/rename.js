"use strict";

/* global before, describe, it, sinon, connect */

var Promise = require('bluebird')

describe('#rename', function() {

    var riakfs;

    before(function() {
        return connect().then(function(_riakfs) {
            riakfs = _riakfs
        })
    })

    function createTestHierachy(root) {
        return riakfs.mkdir(root).then(function() {
            return Promise.all([
                riakfs.mkdir(root + '/dir1'),
                riakfs.mkdir(root + '/dir2'),
            ]).then(function() {
                return Promise.all([
                    riakfs.mkdir(root + '/dir1/dir2'),

                    riakfs.open(root + '/dir1/file1', 'w').then(function(fd) {
                        return riakfs.close(fd)
                    }),

                    riakfs.open(root + '/dir1/file2', 'w').then(function(fd) {
                        return riakfs.close(fd)
                    })
                ]).then(function() {
                    return Promise.all([
                        riakfs.mkdir(root + '/dir1/dir2/dir3'),

                        riakfs.open(root + '/dir1/dir2/file1', 'w').then(function(fd) {
                            return riakfs.close(fd)
                        })
                    ])
                })
            })
        })
    }

    it('should fail if old is parent for new', function() {
        var cb = sinon.spy(function() {})
        return createTestHierachy('/t1').then(function() {
            return riakfs.rename('/t1/dir1', '/t1/dir1/xxxx', cb).should.be.rejected.and.eventually.have.property('code', 'EINVAL')
                .then(function() {
                    cb.getCall(0).args[0].should.be.instanceOf(Error).and.have.property('code', 'EINVAL')
                })
        })
    })

    it('should fail if path prefix for new doesn\'t exist', function() {
        var cb = sinon.spy(function() {})
        return createTestHierachy('/t2').then(function() {
            return riakfs.rename('/t2/dir1/dir2', '/t2/xxx/dir2', cb).should.be.rejected.and.eventually.have.property('code', 'ENOENT')
                .then(function() {
                    cb.getCall(0).args[0].should.be.instanceOf(Error).and.have.property('code', 'ENOENT')
                })
        })
    })

    it('should fail if old doesn\'t exist', function() {
        var cb = sinon.spy(function() {})
        return createTestHierachy('/t3').then(function() {
            return riakfs.rename('/t3/abracadabra', '/t3/dir1/abracadabra', cb).should.be.rejected.and.eventually.have.property('code', 'ENOENT')
                .then(function() {
                    cb.getCall(0).args[0].should.be.instanceOf(Error).and.have.property('code', 'ENOENT')
                })
        })
    })

    it('should fail if old is dir and new is a file', function() {
        var cb = sinon.spy(function() {})
        return createTestHierachy('/t4').then(function() {
            return riakfs.rename('/t4/dir1/dir2', '/t4/dir1/file1', cb).should.be.rejected.and.eventually.have.property('code', 'ENOTDIR')
                .then(function() {
                    cb.getCall(0).args[0].should.be.instanceOf(Error).and.have.property('code', 'ENOTDIR')
                })
        })
    })

    it('should fail if old is file and new is a dir', function() {
        var cb = sinon.spy(function() {})
        return createTestHierachy('/t5').then(function() {
            return riakfs.rename('/t5/dir1/file1', '/t5/dir1/dir2', cb).should.be.rejected.and.eventually.have.property('code', 'EISDIR')
                .then(function() {
                    cb.getCall(0).args[0].should.be.instanceOf(Error).and.have.property('code', 'EISDIR')
                })
        })
    })

    it('should fail if new is a directory and is not empty', function() {
        var cb = sinon.spy(function() {})
        return createTestHierachy('/t6').then(function() {
            return riakfs.rename('/t6/dir2', '/t6/dir1/dir2', cb).should.be.rejected.and.eventually.have.property('code', 'ENOTEMPTY')
                .then(function() {
                    cb.getCall(0).args[0].should.be.instanceOf(Error).and.have.property('code', 'ENOTEMPTY')
                })
        })
    })

    it('should rename single empty directory to new name', function() {
        var cb = sinon.spy(function() {})
        return createTestHierachy('/t7').then(function() {
            return riakfs.rename('/t7/dir2', '/t7/dir1/dir3', cb)
                .then(function() {
                    cb.should.have.been.calledWith(null)

                    return riakfs.stat('/t7/dir1/dir3').should.eventually.be.an('object')
                })
        })
    })

    it('should rename single file to new name', function() {
        var cb = sinon.spy(function() {})
        return createTestHierachy('/t8').then(function() {
            return riakfs.stat('/t8/dir1/file1').then(function(stats1) {
                return riakfs.rename('/t8/dir1/file1', '/t8/dir1/file3', cb)
                    .then(function() {
                        cb.should.have.been.calledWith(null)
                        return riakfs.stat('/t8/dir1/file3').then(function(stats2) {
                            stats2.should.be.an('object')
                            stats2.file.id.should.be.eql(stats1.file.id)
                        })
                    })
            })
        })
    })

    it('should rename (move) single file removing existing file', function() {
        var cb = sinon.spy(function() {})
        return createTestHierachy('/t9').then(function() {
            return Promise.all([
                riakfs.stat('/t9/dir1/file1'),
                riakfs.stat('/t9/dir1/file2')
            ]).spread(function(stats1, stats2) {
                return riakfs.rename('/t9/dir1/file1', '/t9/dir1/file2', cb)
                    .then(function() {
                        cb.should.have.been.calledWith(null)
                        return riakfs.stat('/t9/dir1/file2').then(function(stats3) {
                            stats3.should.be.an('object')
                            stats3.file.id.should.be.eql(stats1.file.id)
                            stats3.file.id.should.not.be.eql(stats2.file.id)
                        })
                    })
            })
        })
    })

    it('should rename (move) directory recursively', function() {
        var cb = sinon.spy(function() {})

        return createTestHierachy('/t10').then(function() {
            return riakfs.rename('/t10/dir1', '/t10/zzz', cb).then(function() {
                cb.should.have.been.calledWith(null)

                return riakfs.readdir('/t10').then(function(list) {
                    list.should.be.an('array').and.have.length(2)
                    list.should.contain('zzz')
                    list.should.contain('dir2')
                    list.should.not.contain('dir1')
                })
                    .then(function() {
                        return riakfs.readdir('/t10/zzz').then(function(list) {
                            list.should.be.an('array').and.have.length(3)
                            list.should.include('dir2')
                                .and.include('file2')
                                .and.include('file1')
                        })
                    })
                    .then(function() {
                        return riakfs.readdir('/t10/zzz/dir2').then(function(list) {
                            list.should.be.an('array').and.have.length(2)
                            list.should.include('dir3')
                            list.should.include('file1')
                        })
                    })
            })
        })
    })

    it('should rename (move) directory recursively to existing empty dir', function() {
        var cb = sinon.spy(function() {})

        return createTestHierachy('/t11').then(function() {
            return riakfs.rename('/t11/dir1', '/t11/dir2', cb).then(function() {
                cb.should.have.been.calledWith(null)

                return riakfs.readdir('/t11').then(function(list) {
                    list.should.be.an('array').and.have.length(1)
                    list.should.contain('dir2')
                    list.should.not.contain('dir1')
                })
                    .then(function() {
                        return riakfs.readdir('/t11/dir2').then(function(list) {
                            list.should.be.an('array').and.have.length(3)
                            list.should.include('dir2')
                                .and.include('file2')
                                .and.include('file1')
                        })
                    })
                    .then(function() {
                        return riakfs.readdir('/t11/dir2/dir2').then(function(list) {
                            list.should.be.an('array').and.have.length(2)
                            list.should.include('dir3')
                            list.should.include('file1')
                        })
                    })
            })
        })
    })

})
