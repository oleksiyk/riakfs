"use strict";

var Stats = function(file) {
    this.file = file;

    this.atime = file.atime?new Date(file.atime):undefined;
    this.mtime = new Date(file.mtime);
    this.ctime = new Date(file.ctime);

    if(!file.isDirectory){
        this.size = file.size;
        this.contentType = file.contentType;
    }
}

Stats.prototype.isFile = function() {
    return !this.file.isDirectory;
}
Stats.prototype.isDirectory = function() {
    return !!this.file.isDirectory;
}
Stats.prototype.isBlockDevice = function() { return false; }
Stats.prototype.isCharacterDevice = function() { return false; }
Stats.prototype.isSymbolicLink = function() { return false; }
Stats.prototype.isFIFO = function() { return false; }
Stats.prototype.isSocket = function() { return false; }


module.exports = Stats;
