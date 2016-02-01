'use strict';

/* global describe, it, connect, before */

var Promise = require('bluebird');

describe('#meta', function () {
    var riakfs;

    before(function () {
        return connect().then(function (_riakfs) {
            riakfs = _riakfs;
        });
    });

    it('#open should save meta information with file', function () {
        var file = {
            filename: '/testFile',
            meta: {
                someKey: 'someValue'
            }
        };

        return riakfs.open(file, 'w').then(function (fd) {
            return riakfs.close(fd);
        })
        .then(function () {
            return riakfs.stat(file.filename).then(function (stats) {
                stats.should.be.an('object');
                stats.file.meta.should.be.an('object').and.have.property('someKey', 'someValue');
            });
        });
    });

    it('#writeFile should save meta information with file', function () {
        var file = {
            filename: '/testFile2',
            meta: {
                someKey: 'someValue2'
            }
        };

        return riakfs.writeFile(file, 'test')
        .then(function () {
            return riakfs.stat(file.filename).then(function (stats) {
                stats.should.be.an('object');
                stats.size.should.eql(4);
                stats.file.meta.should.be.an('object').and.have.property('someKey', 'someValue2');
            });
        });
    });

    it('#createWriteStream should save meta information with file', function () {
        var file = {
            filename: '/testFile3',
            meta: {
                someKey: 'someValue3'
            }
        };

        return new Promise(function (resolve, reject) {
            var stream = riakfs.createWriteStream(file);
            stream.on('error', reject);
            stream.on('close', resolve);

            stream.end('test');
        })
        .then(function () {
            return riakfs.stat(file.filename).then(function (stats) {
                stats.should.be.an('object');
                stats.size.should.eql(4);
                stats.file.meta.should.be.an('object').and.have.property('someKey', 'someValue3');
            });
        });
    });

    it('#updateMeta should fully update meta information', function () {
        var file = {
            filename: '/testFile5',
            meta: {
                someKey: 'someValue5'
            }
        };

        return riakfs.writeFile(file, 'test')
        .then(function () {
            return riakfs.stat(file.filename).then(function (stats) {
                stats.should.be.an('object');
                stats.size.should.eql(4);
                stats.file.meta.should.be.an('object').and.have.property('someKey', 'someValue5');
            });
        })
        .then(function () {
            file.meta = {
                someNewKey: 'someNewValue'
            };
            return riakfs.updateMeta(file.filename, file.meta);
        })
        .then(function () {
            return riakfs.stat(file.filename).then(function (stats) {
                stats.should.be.an('object');
                stats.file.meta.should.be.an('object').and.have.property('someNewKey', 'someNewValue');
                stats.file.meta.should.not.have.property('someKey');
            });
        });
    });

    it('#updateMeta should save new meta information', function () {
        var file = {
            filename: '/testFile7',
            meta: {
                someKey: 'someValue'
            }
        };

        return riakfs.writeFile(file.filename, 'test')
        .then(function () {
            return riakfs.stat(file.filename).then(function (stats) {
                stats.should.be.an('object');
                stats.size.should.eql(4);
                stats.file.should.not.have.property('meta');
            });
        })
        .then(function () {
            return riakfs.updateMeta(file.filename, file.meta);
        })
        .then(function () {
            return riakfs.stat(file.filename).then(function (stats) {
                stats.should.be.an('object');
                stats.file.meta.should.be.an('object').and.have.property('someKey', 'someValue');
            });
        });
    });

    it('#setMeta should save merged meta information with file', function () {
        var file = {
            filename: '/testFile6',
            meta: {
                someKey: 'someValue6'
            }
        };

        return riakfs.writeFile(file, 'test')
        .then(function () {
            return riakfs.stat(file.filename).then(function (stats) {
                stats.should.be.an('object');
                stats.size.should.eql(4);
                stats.file.meta.should.be.an('object').and.have.property('someKey', 'someValue6');
            });
        })
        .then(function () {
            file.meta = {
                someNewKey: 'someNewValue'
            };
            return riakfs.setMeta(file.filename, file.meta);
        })
        .then(function () {
            return riakfs.stat(file.filename).then(function (stats) {
                stats.should.be.an('object');
                stats.file.meta.should.be.an('object').and.have.property('someNewKey', 'someNewValue');
                stats.file.meta.should.have.property('someKey', 'someValue6');
            });
        });
    });

    it('#setMeta should set meta information', function () {
        var file = {
            filename: '/testFile8',
            meta: {
                someKey: 'someValue'
            }
        };

        return riakfs.writeFile(file.filename, 'test')
        .then(function () {
            return riakfs.stat(file.filename).then(function (stats) {
                stats.should.be.an('object');
                stats.size.should.eql(4);
                stats.file.should.not.have.property('meta');
            });
        })
        .then(function () {
            return riakfs.setMeta(file.filename, file.meta);
        })
        .then(function () {
            return riakfs.stat(file.filename).then(function (stats) {
                stats.should.be.an('object');
                stats.file.meta.should.be.an('object').and.have.property('someKey', 'someValue');
            });
        });
    });
});
