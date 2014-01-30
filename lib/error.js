"use strict";

var RiakFsError = function (code, message) {

    Error.call(this);
    Error.captureStackTrace(this, this.constructor);

    this.name = 'RiakFsError';
    this.code = code;

    this.message = message || 'Error';
}

RiakFsError.prototype = Object.create(Error.prototype);
RiakFsError.prototype.constructor = RiakFsError;

RiakFsError.prototype.toJSON = function () {
    return {
        name: this.name,
        message: this.message
    }
}

module.exports = RiakFsError;
