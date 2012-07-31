databank-memcached
------------------

This is the memcached driver for Databank. It should probably work for
Couchbase, too.

License
=======

Copyright 2011, 2012, StatusNet Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

> http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Usage
=====

To create a memcached databank, use the `Databank.get()` method:

    var Databank = require('databank').Databank;
    
    var db = Databank.get('memcached', {});
    
The driver takes the following parameters:

* `schema`: the database schema, as described in the Databank README.
* `serverLocations`: array of server locations in the form `host:port`,
  or a string in the same form. Default is `128.0.0.1:11211` (localhost on default port).
* `options`: options passed through to the `memcached` driver, q.v.
* `expire`: Expiry for values stored, in seconds. Default is `259200` (30 days).

Database structures
===================

Keys are mapped as `type:id`. So a `person` with id `evanp` is at
`person:evanp`. If a key is too long or has chars that can't be used
in a memcached key, it is hashed. So the `album` with the title `The
Joshua Tree` (with spaces) has the key `album:hash:<some long hash>`.

Most values are stored JSON-encoded.

Arrays are stored with JSON-encoded values separated by ASCII 0x1F
(Unit Separator). This makes atomic `prepend()` and `append()`
possible. However, it makes using binary stuff in arrays a little
dicey.

Integers are stored as themselves, which means that atomic `incr()`
and `decr()` work.

Each type has a single array of all keys in that type at
`_databank_keys:typename`. This is kind of blecherous and will
probably crap out when you get a million keys or so.

If the schema for a type includes `indices`, these are mapped to
arrays, too, of the form `_databank_index:property:value`. So, the
keys of all albums with artist `Supertramp` are stored in
`_databank_index:artist:Supertramp`.

This makes exact-match search non-ridiculous, although it slows down
writes a bit.

TODO
----

See https://github.com/evanp/databank-memcached/issues

