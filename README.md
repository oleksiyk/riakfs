[![Build Status](https://travis-ci.org/oleksiyk/riakfs.png)](https://travis-ci.org/oleksiyk/riakfs)

# RiakFS

RiakFS is an implementation of filesystem in [Riak](http://basho.com/riak/) that emulates node.js `fs` module:

*  `open`
*  `close`
*  `read`
*  `write`
*  `readdir`
*  `mkdir`
*  `rmdir`
*  `rename`
*  `createReadStream`
*  `createWriteStream`
*  `unlink`
*  `stat`
*  `fstat`
*  `utime`
*  `futimes`
*  `appendFile`
*  `exists`

It also adds some convenient methods like:

*  `makeTree`
*  `copy`

All methods will return a promise as well as call a usual callback

RiakFS makes use of Riak 2i (secondary indexes) so it requires [LevelDB](http://docs.basho.com/riak/latest/ops/advanced/backends/leveldb/) backend for both of its two buckets.

## Siblings resolution

RiakFS uses `allow_mult=true` for its files (file meta information) bucket and tries to resolve possible [siblings](http://docs.basho.com/riak/latest/theory/concepts/Vector-Clocks/#Siblings) during read operations. It will also handle tombstones conflicts (for example when doing mkdir immediately after rmdir).

Chunks bucket uses `allow_mult=false`. This can be changed later.

## Example

```javascript
require('riakfs')({
    root: 'test-fs' // root is a bucket name prefix
}).then(function(riakfs){
    return riakfs.open('/testFile', 'w').then(function(fd){
        return riakfs.write(fd, 'test', 0, 4, null).then(function() {
            return riakfs.close(fd)
        })
    })
})
```

You can also save some custom meta information with files:

```javascript
var file = {
    filename: '/testFile',
    meta: {
        someKey: 'someValue',
        otherKey: {
            subKey: 'subValue'
        }
    }
}
return riakfs.open(file, 'w').then(function(fd){
    ...
})
```

See tests for more.

## Authors

* Oleksiy Krivoshey [https://github.com/oleksiyk](https://github.com/oleksiyk)

# License (MIT)

Copyright (c) 2014
 Oleksiy Krivoshey.

Permission is hereby granted, free of charge, to any person
obtaining a copy of this software and associated documentation
files (the "Software"), to deal in the Software without
restriction, including without limitation the rights to use,
copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following
conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.

