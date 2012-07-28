// memcacheddatabank.js
//
// implementation of Databank interface using memcached
//
// Copyright 2011,2012 StatusNet Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

var databank = require('databank'),
    memcached = require('memcached'),
    crypto = require('crypto');

var Databank = databank.Databank;
var DatabankError = databank.DatabankError;
var AlreadyExistsError = databank.AlreadyExistsError;
var NoSuchThingError = databank.NoSuchThingError;
var NotConnectedError = databank.NotConnectedError;
var AlreadyConnectedError = databank.AlreadyConnectedError;

function MemcachedDatabank(params) {
    this.serverLocations = params.serverLocations || '127.0.0.1:11211';
    this.options = params.options || {};
    this.schema = params.schema || {};
    this.expire = params.expire || 2592000;
    this.client = null;
}

MemcachedDatabank.prototype = new Databank();

MemcachedDatabank.prototype.toKey = function(type, id) {
    if (this.isLegalForKeys(id) && (type.length + id.length + 1) <= 250) {
        return type + ':' + id;
    } else {
        return type + ':hash:' + this.hash(id);
    }
};

MemcachedDatabank.prototype.isLegalForKeys = function(id) {
    return !/[^!-~]/.test(id);
};

MemcachedDatabank.prototype.hash = function(id) {

    var hash = crypto.createHash('md5'),
        str, data;

    data = "" + id;

    hash.update(data);
    str = hash.digest('base64');

    // Make it a little more FS-safe

    str = str.replace(/\+/g, '-');
    str = str.replace(/\//g, '_');
    str = str.replace(/=/g, '');

    return str;
};

MemcachedDatabank.prototype.connect = function(params, onCompletion) {
    if (this.client) {
        onCompletion(new AlreadyConnectedError());
        return;
    }
    // FIXME: accept serverLocations or options in params...?
    this.client = new memcached(this.serverLocations, this.options);
    onCompletion(null);
};

MemcachedDatabank.prototype.disconnect = function(onCompletion) {
    if (!this.client) {
        onCompletion(new NotConnectedError());
        return;
    }
    this.client = null;
    onCompletion(null);
};

MemcachedDatabank.prototype.create = function(type, id, value, onCompletion) {
    var key = this.toKey(type, id);
    if (!this.client) {
        onCompletion(new NotConnectedError());
        return;
    }
    this.client.add(key, JSON.stringify(value), this.expire, function(err, result) {
        if (err) {
            onCompletion(err, null);
        } else if (!result) { // key clash
            onCompletion(new AlreadyExistsError(type, id), null);
        } else {
            onCompletion(null, value);
        }
    });
};

MemcachedDatabank.prototype.read = function(type, id, onCompletion) {
    var key = this.toKey(type, id);
    if (!this.client) {
        onCompletion(new NotConnectedError());
        return;
    }
    this.client.get(key, function(err, value) {
        if (err) {
            // FIXME: find key-misses and return no-such-thing error
            onCompletion(err, null);
        } else {
            onCompletion(null, JSON.parse(value));
        }
    });
};

MemcachedDatabank.prototype.update = function(type, id, value, onCompletion) {
    var key = this.toKey(type, id);
    if (!this.client) {
        onCompletion(new NotConnectedError());
        return;
    }
    this.client.replace(key, JSON.stringify(value), this.expire, function(err, result) {
        if (err) {
            onCompletion(err, null);
        } else if (!result) { // key miss
            onCompletion(new NoSuchThingError(type, id), null);
        } else {
            onCompletion(null, value);
        }
    });
};

MemcachedDatabank.prototype.del = function(type, id, onCompletion) {
    var key = this.toKey(type, id);
    if (!this.client) {
        onCompletion(new NotConnectedError());
        return;
    }
    this.client.del(key, function(err, value) {
        if (err) {
            onCompletion(err);
        } else if (!value) { // key miss
            onCompletion(new NoSuchThingError(type, id), null);
        } else {
            onCompletion(null);
        }
    });
};

MemcachedDatabank.prototype.save = function(type, id, value, onCompletion) {
    var key = this.toKey(type, id);
    if (!this.client) {
        onCompletion(new NotConnectedError());
        return;
    }
    this.client.set(key, JSON.stringify(value), this.expire, function(err, result) {
        if (err) {
            onCompletion(err, null);
        } else if (!result) { // key miss
            onCompletion(new NoSuchThingError(type, id), null);
        } else {
            onCompletion(null, value);
        }
    });
};

MemcachedDatabank.prototype.readAll = function(type, ids, onCompletion) {
    var keys = [], keyToId = {}, key, i;
    if (!this.client) {
        onCompletion(new NotConnectedError());
        return;
    }
    for (i = 0; i < ids.length; i++) {
        key = this.toKey(type, ids[i]);
        keys.push(key);
        keyToId[key] = ids[i];
    }
    this.client.getMulti(keys, function(err, results) {
        var key, idMap = {}, i, id;
        if (err) {
            onCompletion(err, null);
        } else {
            for (i = 0; i < keys.length; i++) {
                key = keys[i];
                id = keyToId[key];
                if (results.hasOwnProperty(key)) {
                    idMap[id] = JSON.parse(results[key]);
                } else {
                    idMap[id] = null;
                }
            }
            onCompletion(null, idMap);
        }
    });
};

MemcachedDatabank.prototype.incr = function(type, id, onCompletion) {
    var key = this.toKey(type, id);
    this.client.incr(key, 1, function(err, result) {
        if (err) {
            onCompletion(err, null);
        } else {
            onCompletion(null, result);
        }
    });
};

MemcachedDatabank.prototype.decr = function(type, id, onCompletion) {
    var key = this.toKey(type, id);
    this.client.decr(key, 1, function(err, result) {
        if (err) {
            onCompletion(err, null);
        } else {
            onCompletion(null, result);
        }
    });
};

module.exports = MemcachedDatabank;
