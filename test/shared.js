"use strict";

/* global describe, it, connect, before */

var Promise = require('bluebird');
var _       = require('lodash');

describe('#shared directory', function() {

    var riakfs1, riakfs2, riakfs3;

    var sharedFs = function(root) {
        switch(root){
            case riakfs1.options.root:
                return Promise.resolve(riakfs1);
            case riakfs2.options.root:
                return Promise.resolve(riakfs2);
            case riakfs3.options.root:
                return Promise.resolve(riakfs3);
            default:
                return Promise.reject('unknown root: ' + root);
        }
    }

    before(function() {
        return Promise.all([
            connect({shared: {fs: sharedFs}}),
            connect({shared: {fs: sharedFs}}),
            connect({shared: {fs: sharedFs}})
        ]).spread(function(_fs1, _fs2, _fs3) {
            riakfs1 = _fs1
            riakfs2 = _fs2
            riakfs3 = _fs3
        })
        .then(function() {
            return Promise.all([
                riakfs1.mkdir('/dir1'),
                riakfs1.mkdir('/dir2'),
            ]).then(function() {
                return Promise.all([
                    riakfs1.mkdir('/dir1/dir2'),
                    riakfs1.writeFile('/dir1/file1', 'hello')
                ])
            }).then(function() {
                return riakfs1.writeFile('/dir1/dir2/file1', 'hello')
            })
        })
    })

    it('should fail for missing directory', function() {
        return riakfs1.share('/abracadabra', riakfs2.options.root, 'fs1-dir1').should.be.rejected.and.eventually.have.property('code', 'ENOENT')
    })

    it('should fail for file (not a directory)', function() {
        return riakfs1.share('/dir1/file1', riakfs2.options.root, 'fs1-dir1').should.be.rejected.and.eventually.have.property('code', 'ENOTDIR')
    })

    it('should fail for wrong alias', function() {
        return riakfs1.share('/dir1', riakfs2.options.root, 'fs1/dir1').should.be.rejected.and.eventually.have.property('code', 'EINVAL')
    })

    it('should fail for /', function() {
        return riakfs1.share('/', riakfs2.options.root, 'fs1-dir1').should.be.rejected.and.eventually.have.property('code', 'EINVAL')
    })

    it('should fail for /Shared', function() {
        return riakfs1.share('/Shared', riakfs2.options.root, 'fs1-dir1').should.be.rejected.and.eventually.have.property('code', 'EINVAL')
    })

    it('should fail for /.Trash', function() {
        return riakfs1.share('/.Trash', riakfs2.options.root, 'fs1-dir1').should.be.rejected.and.eventually.have.property('code', 'EINVAL')
    })

    it('should not allow sharing when target path is a file', function() {
        return riakfs2.mkdir('/Shared').then(function() {
            return riakfs2.writeFile('/Shared/file1', 'hello').then(function() {
                return riakfs1.share('/dir1', riakfs2.options.root, 'file1').should.be.rejected.and.eventually.have.property('code', 'ENOTDIR')
            })
        })
    })

    it('should not allow sharing when target dir is not empty', function() {
        return riakfs2.mkdir('/Shared/dir1').then(function() {
            return riakfs2.writeFile('/Shared/dir1/file1', 'hello').then(function() {
                return riakfs1.share('/dir1', riakfs2.options.root, 'dir1').should.be.rejected.and.eventually.have.property('code', 'ENOTEMPTY')
            })
        })
    })

    it('should create target shared directory', function() {
        return riakfs1.share('/dir1', riakfs2.options.root, 'fs1-dir1').then(function() {
            return riakfs2.stat('/Shared/fs1-dir1').then(function(stats) {
                stats.should.be.an('object')
                stats.file.should.have.property('share').that.is.an('object')
                stats.file.share.should.have.property('owner').that.is.an('object')
                stats.file.share.owner.root.should.be.eql(riakfs1.options.root)
                stats.file.share.owner.path.should.be.eql('/dir1')
                stats.file.share.should.have.property('to').that.is.an('array').and.have.length(1)
                stats.file.share.to[0].root.should.be.eql(riakfs2.options.root)
                stats.file.share.to[0].alias.should.be.eql('fs1-dir1')
                stats.file.share.to[0].readOnly.should.be.eql(false)
                stats.isDirectory().should.eql(true)
                stats.isFile().should.eql(false)
            })
        })
    })

    it('should not allow re-sharing', function() {
        return riakfs2.share('/Shared/fs1-dir1', riakfs3.options.root, 'fs2-dir1').should.be.rejected.and.eventually.have.property('code', 'ESHARED')
    })

    it('should not allow deep re-sharing', function() {
        return riakfs2.share('/Shared/fs1-dir1/dir2',
            riakfs3.options.root, 'fs2-dir1-dir2').should.be.rejected.and.eventually.have.property('code', 'ESHARED')
    })

    it('should not allow duplicate sharing', function() {
        return riakfs1.share('/dir1', riakfs2.options.root, 'fs1-dir1-duplicate').should.be.rejected.and.eventually.have.property('code', 'ESHARED')
    })

    it('should create readonly target shared directory', function() {
        return riakfs1.share('/dir1', riakfs3.options.root, 'fs1-dir1', true).then(function() {
            return riakfs3.stat('/Shared/fs1-dir1').then(function(stats) {
                stats.should.be.an('object')
                stats.file.should.have.property('share').that.is.an('object')
                stats.file.share.should.have.property('owner').that.is.an('object')
                stats.file.share.owner.root.should.be.eql(riakfs1.options.root)
                stats.file.share.owner.path.should.be.eql('/dir1')
                stats.file.share.should.have.property('to').that.is.an('array').and.have.length(2)
                stats.file.share.to[1].root.should.be.eql(riakfs3.options.root)
                stats.file.share.to[1].alias.should.be.eql('fs1-dir1')
                stats.file.share.to[1].readOnly.should.be.eql(true)
                stats.isDirectory().should.eql(true)
                stats.isFile().should.eql(false)
            })
        })
    })

    it('#readdir', function() {
        return riakfs2.readdir('/Shared/fs1-dir1').then(function(list) {
            list.should.contain('dir2').and.contain('file1')
        })
    })

    it('#readdir - readonly', function() {
        return riakfs3.readdir('/Shared/fs1-dir1').then(function(list) {
            list.should.contain('dir2').and.contain('file1')
        })
    })

    it('#stat - shared dir', function() {
        return riakfs2.stat('/Shared/fs1-dir1').then(function(stats) {
            stats.should.be.an('object')
            stats.isDirectory().should.eql(true)
            stats.file.should.have.property('share').that.is.an('object')
            var s = _.find(stats.file.share.to, { root: riakfs2.options.root })
            s.alias.should.be.eql('fs1-dir1')
        })
    })

    it('#stat', function() {
        return riakfs2.stat('/Shared/fs1-dir1/file1').then(function(stats) {
            stats.should.be.an('object')
            stats.isFile().should.eql(true)
            stats.size.should.be.eql(5)
        })
    })

    it('#stat - readonly', function() {
        return riakfs3.stat('/Shared/fs1-dir1/file1').then(function(stats) {
            stats.should.be.an('object')
            stats.isFile().should.eql(true)
            stats.size.should.be.eql(5)
            stats.mode.should.be.eql(33060) // 0100000 | 0444
        })
    })

    it('#exists', function() {
        return riakfs2.exists('/Shared/fs1-dir1/file1').then(function(exists) {
            return exists.should.be.true
        })
    })

    it('#exists - readonly', function() {
        return riakfs3.exists('/Shared/fs1-dir1/file1').then(function(exists) {
            return exists.should.be.true
        })
    })

    it('#readFile', function() {
        return riakfs2.readFile('/Shared/fs1-dir1/file1', {encoding: 'utf8'}).then(function(data) {
            data.length.should.be.eql(5)
            data.should.be.a('string').and.eql('hello')
        })
    })

    it('#readFile - readonly', function() {
        return riakfs3.readFile('/Shared/fs1-dir1/file1', {encoding: 'utf8'}).then(function(data) {
            data.length.should.be.eql(5)
            data.should.be.a('string').and.eql('hello')
        })
    })

    it('#writeFile', function() {
        return riakfs2.writeFile('/Shared/fs1-dir1/file2', 'hello2').then(function() {
            return riakfs1.stat('/dir1/file2').then(function(stats) {
                stats.should.be.an('object')
                stats.isFile().should.eql(true)
                stats.size.should.be.eql(6)
            })
        }).then(function() {
            return riakfs1.readFile('/dir1/file2', {encoding: 'utf8'}).then(function(data) {
                data.length.should.be.eql(6)
                data.should.be.a('string').and.eql('hello2')
            })
        })
    })

    it('#writeFile - readonly', function() {
        return riakfs3.writeFile('/Shared/fs1-dir1/file2', 'hello2').should.be.rejected.and.eventually.have.property('code', 'EACCES')
    })

    it('#updateMeta', function() {
        var file = {
            filename: '/Shared/fs1-dir1/testFile',
            meta: {
                someKey: 'someValue'
            }
        }

        return riakfs2.writeFile(file.filename, 'test')
        .then(function() {
            return riakfs1.stat('/dir1/testFile').then(function(stats) {
                stats.should.be.an('object')
                stats.size.should.eql(4)
                stats.file.should.not.have.property('meta')
            })
        })
        .then(function() {
            return riakfs2.updateMeta(file.filename, file)
        })
        .then(function() {
            return riakfs1.stat('/dir1/testFile').then(function(stats) {
                stats.should.be.an('object')
                stats.file.meta.should.be.an('object').and.have.property('someKey', 'someValue')
            })
        })
    })

    it('#updateMeta - readonly', function() {
        var file = {
            filename: '/Shared/fs1-dir1/testFile',
            meta: {
                someKey: 'someValue'
            }
        }
        return riakfs3.updateMeta(file.filename, file).should.be.rejected.and.eventually.have.property('code', 'EACCES')
    })

    it('#unlink', function() {
        return riakfs2.unlink('/Shared/fs1-dir1/file2').then(function() {
            return riakfs1.stat('/dir1/file2').should.be.rejected.and.eventually.have.property('code', 'ENOENT')
        })
    })

    it('#unlink - readonly', function() {
        return riakfs3.unlink('/Shared/fs1-dir1/file1').should.be.rejected.and.eventually.have.property('code', 'EACCES')
    })

    it('#mkdir', function() {
        return riakfs2.mkdir('/Shared/fs1-dir1/dir3').then(function() {
            return riakfs1.stat('/dir1/dir3').then(function(stats) {
                stats.should.be.an('object')
                stats.isDirectory().should.eql(true)
            })
        })
    })

    it('#mkdir - readonly', function() {
        return riakfs3.mkdir('/Shared/fs1-dir1/dir4').should.be.rejected.and.eventually.have.property('code', 'EACCES')
    })

    it('#rmdir', function() {
        return riakfs2.rmdir('/Shared/fs1-dir1/dir3').then(function() {
            return riakfs1.stat('/dir1/dir3').should.be.rejected.and.eventually.have.property('code', 'ENOENT')
        })
    })

    it('#rmdir - readonly', function() {
        return riakfs3.rmdir('/Shared/fs1-dir1/dir1').should.be.rejected.and.eventually.have.property('code', 'EACCES')
    })

    it('#copy', function() {
        return riakfs2.writeFile('/testFile', 'hello').then(function() {
            return riakfs2.copy('/testFile', '/Shared/fs1-dir1/testFileCopy')
        })
        .then(function() {
            return riakfs1.stat('/dir1/testFileCopy').then(function(stats) {
                stats.should.be.an('object')
                stats.isFile().should.eql(true)
                stats.size.should.be.eql(5)
            })
        })
    })

    it('#copy - readonly', function() {
        return riakfs3.writeFile('/testFile', 'hello').then(function() {
            return riakfs3.copy('/testFile', '/Shared/fs1-dir1/testFileCopy').should.be.rejected.and.eventually.have.property('code', 'EACCES')
        })
    })

    it('#copy - readonly - reverse', function() {
        return riakfs3.copy('/Shared/fs1-dir1/file1', '/testFile1Copy').then(function() {
            return riakfs3.stat('/testFile1Copy').then(function(stats) {
                stats.should.be.an('object')
                stats.isFile().should.eql(true)
                stats.size.should.be.eql(5)
            })
        })
    })

    it('#makeTree', function() {
        return riakfs2.makeTree('/Shared/fs1-dir1/aa/bb/cc').then(function() {
            return riakfs1.stat('/dir1/aa/bb/cc').then(function(stats) {
                stats.should.be.an('object')
                stats.isDirectory().should.eql(true)
            })
        })
    })

    it('#makeTree - readonly', function() {
        return riakfs3.makeTree('/Shared/fs1-dir1/aa1/bb1/cc1').should.be.rejected.and.eventually.have.property('code', 'EACCES')
    })

    it('#unshare - source', function() {
        return riakfs1.unshare('/dir1', riakfs3.options.root).then(function() {
            return riakfs3.stat('/Shared/fs1-dir1').should.be.rejected.and.eventually.have.property('code', 'ENOENT')
        })
    })

    it('#unshare - destination', function() {
        return riakfs2.unshare('/Shared/fs1-dir1').then(function() {
            return Promise.all([
                riakfs2.stat('/Shared/fs1-dir1').should.be.rejected.and.eventually.have.property('code', 'ENOENT'),
                riakfs1.stat('/dir1').then(function(stats) {
                    stats.should.be.an('object')
                    stats.file.should.not.have.property('share')
                    stats.isDirectory().should.eql(true)
                })
            ])
        })
    })

    it('#rmdir - shared folder', function() {
        return riakfs1.share('/dir2', riakfs2.options.root, 'fs1-dir2').then(function() {
            return Promise.all([
                riakfs2.rmdir('/Shared/fs1-dir2').should.be.rejected.and.eventually.have.property('code', 'ESHARED'),
                riakfs1.rmdir('/dir2').should.be.rejected.and.eventually.have.property('code', 'ESHARED'),
            ])
        })
    })

    it('#rename - should handle source dir renaming', function() {
        return riakfs1.share('/dir1/dir2', riakfs2.options.root, 'fs1-dir1-dir2').then(function() {
            return riakfs1.share('/dir1/dir2', riakfs3.options.root, 'fs1-dir1-dir2')
        })
        .then(function() {
            return riakfs1.rename('/dir1', '/dir1-renamed')
        })
        .then(function() {
            return Promise.all([
                riakfs2.readdir('/Shared/fs1-dir1-dir2').then(function(list) {
                    list.should.contain('file1')
                }),
                riakfs3.readdir('/Shared/fs1-dir1-dir2').then(function(list) {
                    list.should.contain('file1')
                }),
                riakfs1.readdir('/dir1-renamed/dir2').then(function(list) {
                    list.should.contain('file1')
                })
            ])
        })
    })

    it('#rename - should handle rename operation inside destination shares', function() {
        return riakfs2.mkdir('/Shared/fs1-dir1-dir2/dir3')
        .then(function() {
            return riakfs3.writeFile('/Shared/fs1-dir1-dir2/dir3/testFile', 'hello')
        })
        .then(function() {
            return riakfs2.rename('/Shared/fs1-dir1-dir2/dir3', '/Shared/fs1-dir1-dir2/dir3-renamed')
        })
        .then(function() {
            return Promise.all([
                riakfs1.readdir('/dir1-renamed/dir2/dir3-renamed').then(function(list) {
                    list.should.contain('testFile')
                }),
                riakfs3.readdir('/Shared/fs1-dir1-dir2/dir3-renamed').then(function(list) {
                    list.should.contain('testFile')
                }),
                riakfs2.readdir('/Shared/fs1-dir1-dir2/dir3-renamed').then(function(list) {
                    list.should.contain('testFile')
                })
            ])
        })
    })

    it('#rename - inside destination shares - readonly', function() {
        return riakfs1.mkdir('/dir3')
        .then(function() {
            return riakfs1.writeFile('/dir3/testFile', 'hello')
        })
        .then(function() {
            return riakfs1.share('/dir3', riakfs3.options.root, 'fs1-dir3', true)
        })
        .then(function() {
            return riakfs3.rename('/Shared/fs1-dir3/testFile',
                '/Shared/fs1-dir3/testFile-renamed').should.be.rejected.and.eventually.have.property('code', 'EACCES')
        })
    })

    it('#rename - move destination share (to itself)', function() {
        return riakfs2.rename('/Shared/fs1-dir1-dir2', '/fs1-dir1-dir2').should.be.rejected.and.eventually.have.property('code', 'EINVAL')
    })

    it('#rename - move destination share (to other share)', function() {
        return riakfs1.share('/dir1-renamed', riakfs2.options.root, 'fs1-dir1')
        .then(function() {
            return riakfs2.rename('/Shared/fs1-dir1-dir2', '/Shared/fs1-dir1').should.be.rejected.and.eventually.have.property('code', 'EINVAL')
        })
    })

    it('#rename - /Shared', function() {
        return riakfs2.rename('/Shared', '/Shared-renamed').should.be.rejected.and.eventually.have.property('code', 'EINVAL')
    })

    it('#rename - rename destination share', function() {
        return riakfs2.rename('/Shared/fs1-dir1', '/Shared/fs1-dir1-renamed')
        .then(function() {
            return riakfs2.readdir('/Shared/fs1-dir1-renamed').then(function(list) {
                list.should.contain('dir2').and.contain('file1')
                return riakfs1.stat('/dir1-renamed').then(function(stats) {
                    stats.should.be.an('object')
                    stats.file.should.have.property('share')
                    var s = _.find(stats.file.share.to, { root: riakfs2.options.root })
                    s.alias.should.be.eql('fs1-dir1-renamed')
                })
            })
        })
    })

    it('#rename - move between filesystems', function() {
        return riakfs1.makeTree('/dir4/dir2/dir3')
        .then(function() {
            return Promise.all([
                riakfs1.writeFile('/dir4/file1', 'hello'),
                riakfs1.writeFile('/dir4/dir2/file2', 'hello'),
                riakfs1.writeFile('/dir4/dir2/dir3/file3', 'hello!')
            ])
        })
        .then(function() {
            return riakfs1.share('/dir4', riakfs2.options.root, 'fs1-dir4')
        })
        .then(function() {
            return riakfs2.rename('/Shared/fs1-dir4/dir2', '/new')
        })
        .then(function() {
            return riakfs2.stat('/new/file2').then(function(stats) {
                stats.should.be.an('object')
                stats.isFile().should.eql(true)
                stats.size.should.be.eql(5)
            })
        })
        .then(function() {
            return riakfs2.stat('/new/dir3/file3').then(function(stats) {
                stats.should.be.an('object')
                stats.isFile().should.eql(true)
                stats.size.should.be.eql(6)
            })
        })
        .then(function() {
            return riakfs1.readdir('/dir4').then(function(list) {
                list.should.contain('file1').and.not.contain('dir2')
            })
        })
    })

})
