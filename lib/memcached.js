// memcached.js
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
    Step = require('step'),
    memcached = require('memcached'),
    crypto = require('crypto');

var Databank = databank.Databank;
var DatabankError = databank.DatabankError;
var AlreadyExistsError = databank.AlreadyExistsError;
var NoSuchThingError = databank.NoSuchThingError;
var NotConnectedError = databank.NotConnectedError;
var AlreadyConnectedError = databank.AlreadyConnectedError;

var SEP = "\x1F";

function MemcachedDatabank(params) {
    this.serverLocations = params.serverLocations || '127.0.0.1:11211';
    this.options = params.options || {};
    this.schema = params.schema || {};
    this.expire = params.expire || 2592000;
    this.client = null;
}

MemcachedDatabank.prototype = new Databank();

MemcachedDatabank.prototype.toKey = function(type, id) {
    var idstr = ""+id;
    if (this.isLegalForKeys(idstr) && (type.length + idstr.length + 1) <= 250) {
        return type + ':' + idstr;
    } else {
        return type + ':hash:' + this.hash(idstr);
    }
};

MemcachedDatabank.prototype.isLegalForKeys = function(idstr) {
    return !/[^!-~]/.test(idstr);
};

MemcachedDatabank.prototype.hash = function(idstr) {

    var hash = crypto.createHash('md5'),
        str, data;

    data = idstr;

    hash.update(data);
    str = hash.digest('base64');

    // Make it a little more FS-safe

    str = str.replace(/\+/g, '-');
    str = str.replace(/\//g, '_');
    str = str.replace(/=/g, '');

    return str;
};

MemcachedDatabank.prototype.freeze = function(value) {

    var bank = this, dup;

    if (Array.isArray(value)) {
        dup = value.map(function(item) {
            return bank.freeze(item);
        });
        return SEP + dup.join(SEP);
    } else {
        return JSON.stringify(value);
    }
};

MemcachedDatabank.prototype.melt = function(raw) {
    var bank = this, dup, i;

    if (!raw || raw.length === 0) {
        return "";
    } else if (raw.substr(0, 1) === SEP) {
        return raw.substr(1).split(SEP).map(function(item) {
            return bank.melt(item);
        });
    } else {
        return JSON.parse(raw);
    }
};

MemcachedDatabank.prototype.connect = function(params, callback) {

    if (this.client) {
        callback(new AlreadyConnectedError());
        return;
    }

    this.client = new memcached(this.serverLocations, this.options);
    callback(null);
};

MemcachedDatabank.prototype.disconnect = function(callback) {

    if (!this.client) {
        callback(new NotConnectedError());
        return;
    }

    this.client = null;
    callback(null);
};

MemcachedDatabank.prototype.create = function(type, id, value, callback) {

    var bank = this,
        key = this.toKey(type, id);

    if (!bank.client) {
        callback(new NotConnectedError());
        return;
    }

    Step(
        function() {
            bank.client.add(key, bank.freeze(value), bank.expire, this);
        },
        function(err, result) {
            if (err) throw err;
            if (!result) throw new AlreadyExistsError(type, id); // Key miss
            bank.enlist(type, id, this);
        },
        function(err) {
            if (err) throw err;
            bank.index(type, id, value, this);
        },
        function(err) {
            if (err) {
                callback(err, null);
            } else {
                callback(null, value);
            }
        }
    );
};

MemcachedDatabank.prototype.read = function(type, id, callback) {

    var bank = this,
        key = bank.toKey(type, id);

    if (!bank.client) {
        callback(new NotConnectedError());
        return;
    }

    bank.client.get(key, function(err, value) {
        if (err) {
            callback(err, null);
        } else if (value === false) {
            callback(new NoSuchThingError(type, id), null);
        } else {
            callback(null, bank.melt(value));
        }
    });
};

MemcachedDatabank.prototype.update = function(type, id, value, callback) {

    var bank = this,
        key = bank.toKey(type, id);

    if (!bank.client) {
        callback(new NotConnectedError());
        return;
    }

    Step(
        function() {
            bank.deindex(type, id, this);
        },
        function(err) {
            if (err) throw err;
            bank.client.replace(key, bank.freeze(value), bank.expire, this);
        },
        function(err, result) {
            if (err) throw err;
            if (!result) throw new NoSuchThingError(type, id);
            bank.index(type, id, value, this);
        },
        function(err) {
            if (err) {
                callback(err, null);
            } else {
                callback(null, value);
            }
        }
    );
};

MemcachedDatabank.prototype.del = function(type, id, callback) {

    var bank = this,
        key = bank.toKey(type, id);

    if (!bank.client) {
        callback(new NotConnectedError());
        return;
    }

    Step(
        function() {
            bank.deindex(type, id, this);
        },
        function(err) {
            if (err) throw err;
            bank.delist(type, id, this);
        },
        function(err) {
            if (err) throw err;
            bank.client.del(key, this);
        },
        function(err, value) {
            if (err) {
                callback(err);
            } else if (!value) { // key miss
                callback(new NoSuchThingError(type, id), null);
            } else {
                callback(null);
            }
        }
    );
};

MemcachedDatabank.prototype.save = function(type, id, value, callback) {

    var bank = this,
        key = bank.toKey(type, id);

    if (!bank.client) {
        callback(new NotConnectedError());
        return;
    }

    Step(
        function() {
            bank.read(type, id, this);
        },
        function(err, old) {
            if (err && err instanceof NoSuchThingError) {
                bank.enlist(type, id, this);
            } else if (err) {
                throw err;
            } else {
                bank.deindex(type, id, this);
            }
        },
        function(err) {
            if (err) throw err;
            bank.client.set(key, bank.freeze(value), bank.expire, this);
        },
        function(err, result) {
            if (err) throw err;
            if (!result) throw new NoSuchThingError(type, id);
            bank.index(type, id, value, this);
        },
        function(err) {
            if (err) {
                callback(err, null);
            } else {
                callback(null, value);
            }
        }
    );
};

MemcachedDatabank.prototype.readAll = function(type, ids, callback) {

    var bank = this,
        keys = [],
        keyToId = {},
        key,
        i;

    if (!bank.client) {
        callback(new NotConnectedError());
        return;
    }

    for (i = 0; i < ids.length; i++) {
        key = bank.toKey(type, ids[i]);
        keys.push(key);
        keyToId[key] = ids[i];
    }

    bank.client.getMulti(keys, function(err, results) {
        var key, idMap = {}, i, id;
        if (err) {
            callback(err, null);
        } else {
            for (i = 0; i < keys.length; i++) {
                key = keys[i];
                id = keyToId[key];
                if (results.hasOwnProperty(key)) {
                    idMap[id] = bank.melt(results[key]);
                } else {
                    idMap[id] = null;
                }
            }
            callback(null, idMap);
        }
    });
};

// FIXME: update indices...?

MemcachedDatabank.prototype.incr = function(type, id, callback) {

    var bank = this,
        key = bank.toKey(type, id);

    bank.client.incr(key, 1, function(err, result) {
        if (err) {
            callback(err, null);
        } else {
            callback(null, result);
        }
    });
};

// FIXME: update indices...?

MemcachedDatabank.prototype.decr = function(type, id, callback) {

    var bank = this,
        key = bank.toKey(type, id);

    bank.client.decr(key, 1, function(err, result) {
        if (err) {
            callback(err, null);
        } else {
            callback(null, result);
        }
    });
};

MemcachedDatabank.prototype.index = function(type, id, value, callback) {

    var bank = this,
        key = bank.toKey(type, id);

    if (type.substr(0, 10) === '_databank_') {
        callback(null);
        return;
    }

    // FIXME: this seems like a terrible way to scan a type;
    // I could use some other ideas

    Step(
        function() {
            var i = 0, group = this.group(), indices, k;
            if (bank.hasOwnProperty('schema') &&
                bank.schema.hasOwnProperty(type) &&
                bank.schema[type].hasOwnProperty('indices')) {
                indices = bank.schema[type].indices;
                for (i = 0; i < indices.length; i++) {
                    k = Databank.deepProperty(value, indices[i]);
                    bank.append('_databank_index', type + ':' + indices[i] + ':' + k, key, group());
                }
            }
        },
        function(err, resultses) {
            if (err) {
                callback(err);
            } else {
                callback(null);
            }
        }
    );
};

MemcachedDatabank.prototype.delist = function(type, id, callback) {
    var bank = this,
        key = bank.toKey(type, id);

    if (type.substr(0, 10) === '_databank_') {
        callback(null);
        return;
    }

    bank.remove('_databank_keys', type, key, callback);
};

MemcachedDatabank.prototype.enlist = function(type, id, callback) {

    var bank = this,
        key = bank.toKey(type, id);

    if (type.substr(0, 10) === '_databank_') {
        callback(null);
        return;
    }

    bank.append('_databank_keys', type, key, function(err, newKeys) {
        if (err) {
            callback(err);
        } else {
            callback(null);
        }
    });
};

MemcachedDatabank.prototype.deindex = function(type, id, callback) {

    var bank = this,
        key = bank.toKey(type, id);

    if (type.substr(0, 10) === '_databank_') {
        callback(null);
        return;
    }

    // FIXME: this seems like a terrible way to scan a type;
    // I could use some other ideas

    Step(
        function() {
            bank.read(type, id, this);
        },
        function(err, value) {
            var i = 0, group = this.group(), indices, k;

            if (err && err instanceof NoSuchThingError) {
                // All good! Must be new.
                callback(null);
                return;
            }

            if (err) throw err;
            if (bank.hasOwnProperty('schema') &&
                bank.schema.hasOwnProperty(type) &&
                bank.schema[type].hasOwnProperty('indices')) {
                indices = bank.schema[type].indices;
                for (i = 0; i < indices.length; i++) {
                    k = Databank.deepProperty(value, indices[i]);
                    bank.remove('_databank_index', type + ':' + indices[i] + ':' + k, key, group());
                }
            }
        },
        function(err, resultses) {
            if (err) {
                callback(err);
            } else {
                callback(null);
            }
        }
    );
};


// Partially cadged from databank-redis

MemcachedDatabank.prototype.search = function(type, criteria, onResult, callback) {

    var bank = this,
        indices = [],
        property,
        indexed = {},
        unindexed = {},
        haveIndexed = false,
        scanKeys = function(keys, callback) {
            Step(
                function() {
                    var i, group = this.group();
                    // FIXME: boxcar this; groups of 128, say?
                    bank.client.getMulti(keys, this);
                },
                function(err, raws) {
                    var i, value;
                    if (err) {
                        callback(err);
                        return;
                    }
                    for (i = 0; i < raws.length; i++) {
                        if (raws[i] === false) {
                            continue;
                        }
                        value = bank.melt(raws[i]);
                        if (bank.matchesCriteria(value, unindexed)) {
                            onResult(value);
                        }
                    }
                    callback(null);
                }
            );
        },
        intersect = function(a, b) {
            var ai=0, bi=0;
            var result = new Array();

            while( ai < a.length && bi < b.length )
            {
                if      (a[ai] < b[bi] ){ ai++; }
                else if (a[ai] > b[bi] ){ bi++; }
                else /* they're equal */
                {
                    result.push(a[ai]);
                    ai++;
                    bi++;
                }
            }

            return result;
        },
        intersectAll = function(arrays) {

            if (arrays.length === 0) {
                return [];
            }

            if (arrays.length === 1) {
                return arrays[0];
            }

            return arrays.reduce(function(acc, cur, i, arr) {
                acc.sort();
                cur.sort();
                return intersect(acc, cur);
            });
        };

    if (!bank.client) {
        callback(new NotConnectedError(), null);
        return;
    }

    // Determine which criteria, if any, are on an indexed property

    if (bank.schema && bank.schema[type] && bank.schema[type].indices) {
        indices = bank.schema[type].indices;
        for (property in criteria) {
            if (indices.indexOf(property) == -1) {
                unindexed[property] = criteria[property];
            } else {
                haveIndexed = true;
                indexed[property] = criteria[property];
            }
        }
    } else {
        unindexed = criteria;
    }

    // If there are any indexed properties, use set intersection to get candidate keys
    if (haveIndexed) {
        Step(
            function() {
                var property, group = this.group();
                for (property in indexed) {
                    bank.read('_databank_index', type + ':' + property + ':' + criteria[property], group());
                }
            },
            function(err, indices) {
                var keys;
                if (err) {
                    if (err instanceof NoSuchThingError) {
                        callback(null);
                        return;
                    } else {
                        throw err;
                    }
                }
                keys = intersectAll(indices);
                if (keys.length === 0) {
                    callback(null);
                    return;
                }
                scanKeys(keys, this);
            },
            callback
        );
    } else {
        // Get every record of a given type
        bank.read('_databank_keys', type, function(err, keys) {
            if (err) {
                callback(err);
            } else {
                scanKeys(keys, callback);
            }
        });
    }
};

MemcachedDatabank.prototype.append = function(type, id, toAppend, callback) {
    var bank = this,
        key = bank.toKey(type, id),
        value = SEP + bank.freeze(toAppend);

    bank.client.append(key, value, function(err, result) {
        if (err) {
            callback(err, null);
        } else if (!result) {
            bank.create(type, id, [toAppend], function(err, result) {
                if (err) {
                    if (err instanceof AlreadyExistsError) {
                        // try again
                        bank.append(type, id, toAppend, callback);
                    } else {
                        callback(err, null);
                    }
                } else {
                    callback(null, result);
                }
            });
        } else {
            bank.read(type, id, callback);
        }
    });
};

MemcachedDatabank.prototype.prepend = function(type, id, toPrepend, callback) {
    var bank = this,
        key = bank.toKey(type, id),
        value = SEP + bank.freeze(toPrepend);

    bank.client.prepend(key, value, function(err, result) {
        if (err) {
            callback(err, null);
        } else if (!result) {
            bank.create(type, id, [toPrepend], function(err, result) {
                if (err) {
                    if (err instanceof AlreadyExistsError) {
                        // try again
                        bank.append(type, id, toPrepend, callback);
                    } else {
                        callback(err, null);
                    }
                } else {
                    callback(null, result);
                }
            });
        } else {
            bank.read(type, id, callback);
        }
    });
};

module.exports = MemcachedDatabank;
