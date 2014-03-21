"use strict";

/* jshint bitwise: false */

var gid = process.getgid(), uid = process.getuid()

var Stats = function(file, readOnly) {
    this.file = file;

    readOnly = readOnly || false;

    this.atime = file.atime?new Date(file.atime):undefined;
    this.mtime = new Date(file.mtime);
    this.ctime = new Date(file.ctime);
    this.gid = gid;
    this.uid = uid;
    this.size = 0;

    if(!file.isDirectory){
        this.size = file.size;
        this.mode = readOnly? 33060 : 33188; // 0100000 | 0644 (regular file + 0644 or 0444)
        this.contentType = file.contentType;
    } else {
        this.mode = readOnly? 16749 : 16877; // 0040000 | 0755 (directory + 0755 or 0555)
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
