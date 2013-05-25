// memcached.js
//
// implementation of Databank interface using memcached
//
// Copyright 2011,2012 E14N https://e14n.com/
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

var MemcachedDatabank = function(params) {

    var bank = this,
        // PRIVATE ATTRIBUTES
        serverLocations = params.serverLocations || '127.0.0.1:11211',
        options = params.options || {},
        expire = params.expire || 2592000,
        client = null,
        // PRIVATE METHODS
        toKey = function(type, id) {
            var idstr = ""+id;
            if (isLegalForKeys(idstr) && (type.length + idstr.length + 1) <= 250) {
                return type + ':' + idstr;
            } else {
                return type + ':hash:' + hash(idstr);
            }
        },
        isLegalForKeys = function(idstr) {
            return !/[^!-~]/.test(idstr);
        },
        hash = function(idstr) {

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
        },
        freeze = function(value) {
            var dup;

            if (Array.isArray(value)) {
                dup = value.map(function(item) {
                    return freeze(item);
                });
                return SEP + dup.join(SEP);
            } else {
                return JSON.stringify(value);
            }
        },
        melt = function(raw) {
            var dup, i;

            if (!raw || raw.length === 0) {
                return "";
            } else if (raw.substr(0, 1) === SEP) {
                return raw.substr(1).split(SEP).map(function(item) {
                    return melt(item);
                });
            } else {
                return JSON.parse(raw);
            }
        },
        index = function(type, id, value, callback) {

            var key = toKey(type, id);

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
        },
        delist = function(type, id, callback) {
            var key = toKey(type, id);

            if (type.substr(0, 10) === '_databank_') {
                callback(null);
                return;
            }

            bank.remove('_databank_keys', type, key, callback);
        },
        enlist = function(type, id, callback) {

            var key = toKey(type, id);

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
        },
        deindex = function(type, id, callback) {

            var key = toKey(type, id);

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

    // PUBLIC ATTRIBUTES

    bank.schema = params.schema || {},

    // PRIVILEGED METHODS

    bank.connect = function(params, callback) {

        if (client) {
            callback(new AlreadyConnectedError());
            return;
        }

        client = new memcached(serverLocations, options);
        callback(null);
    };

    bank.disconnect = function(callback) {

        if (!client) {
            callback(new NotConnectedError());
            return;
        }

        client = null;
        callback(null);
    };

    bank.create = function(type, id, value, callback) {

        var key = toKey(type, id);

        if (!client) {
            callback(new NotConnectedError());
            return;
        }

        Step(
            function() {
                client.add(key, freeze(value), expire, this);
            },
            function(err, result) {
                if (err) throw err;
                if (!result) throw new AlreadyExistsError(type, id); // Key miss
                enlist(type, id, this);
            },
            function(err) {
                if (err) throw err;
                index(type, id, value, this);
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

    bank.read = function(type, id, callback) {

        var bank = this,
            key = toKey(type, id);

        if (!client) {
            callback(new NotConnectedError());
            return;
        }

        client.get(key, function(err, value) {
            if (err) {
                callback(err, null);
            } else if (value === false) {
                callback(new NoSuchThingError(type, id), null);
            } else {
                callback(null, melt(value));
            }
        });
    };

    bank.update = function(type, id, value, callback) {

        var bank = this,
            key = toKey(type, id);

        if (!client) {
            callback(new NotConnectedError());
            return;
        }

        Step(
            function() {
                deindex(type, id, this);
            },
            function(err) {
                if (err) throw err;
                client.replace(key, freeze(value), expire, this);
            },
            function(err, result) {
                if (err) throw err;
                if (!result) throw new NoSuchThingError(type, id);
                index(type, id, value, this);
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

    bank.del = function(type, id, callback) {

        var bank = this,
            key = toKey(type, id);

        if (!client) {
            callback(new NotConnectedError());
            return;
        }

        Step(
            function() {
                deindex(type, id, this);
            },
            function(err) {
                if (err) throw err;
                delist(type, id, this);
            },
            function(err) {
                if (err) throw err;
                client.del(key, this);
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

    bank.save = function(type, id, value, callback) {

        var bank = this,
            key = toKey(type, id);

        if (!client) {
            callback(new NotConnectedError());
            return;
        }

        Step(
            function() {
                bank.read(type, id, this);
            },
            function(err, old) {
                if (err && err instanceof NoSuchThingError) {
                    enlist(type, id, this);
                } else if (err) {
                    throw err;
                } else {
                    deindex(type, id, this);
                }
            },
            function(err) {
                if (err) throw err;
                client.set(key, freeze(value), expire, this);
            },
            function(err, result) {
                if (err) throw err;
                if (!result) throw new NoSuchThingError(type, id);
                index(type, id, value, this);
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

    bank.readAll = function(type, ids, callback) {

        var bank = this,
            keys = [],
            keyToId = {},
            key,
            i;

        if (!client) {
            callback(new NotConnectedError());
            return;
        }

        for (i = 0; i < ids.length; i++) {
            key = toKey(type, ids[i]);
            keys.push(key);
            keyToId[key] = ids[i];
        }

        client.getMulti(keys, function(err, results) {
            var key, idMap = {}, i, id;
            if (err) {
                callback(err, null);
            } else {
                for (i = 0; i < keys.length; i++) {
                    key = keys[i];
                    id = keyToId[key];
                    if (results.hasOwnProperty(key)) {
                        idMap[id] = melt(results[key]);
                    } else {
                        idMap[id] = null;
                    }
                }
                callback(null, idMap);
            }
        });
    };

    // FIXME: update indices...?

    bank.incr = function(type, id, callback) {

        var bank = this,
            key = toKey(type, id);

        client.incr(key, 1, function(err, result) {
            if (err) {
                callback(err, null);
            } else {
                callback(null, result);
            }
        });
    };

    // FIXME: update indices...?

    bank.decr = function(type, id, callback) {

        var bank = this,
            key = toKey(type, id);

        client.decr(key, 1, function(err, result) {
            if (err) {
                callback(err, null);
            } else {
                callback(null, result);
            }
        });
    };

    // Partially cadged from databank-redis

    bank.search = function(type, criteria, onResult, callback) {

        var bank = this,
            indices = [],
            property,
            indexed = {},
            unindexed = {},
            haveIndexed = false,
            scanKeys = function(keys, callback) {
                Step(
                    function() {
                        // FIXME: boxcar this; groups of 128, say?
                        client.getMulti(keys, this);
                    },
                    function(err, raws) {
                        var p, value;
                        if (err) {
                            callback(err);
                            return;
                        }
                        for (p in raws) {
                            if (raws[p] === false) {
                                continue;
                            }
                            value = melt(raws[p]);
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
                var result = [];

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

        if (!client) {
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

    bank.append = function(type, id, toAppend, callback) {
        var bank = this,
            key = toKey(type, id),
            value = SEP + freeze(toAppend);

        Step(
            function() {
                client.append(key, value, this);
            },
            function(err, result) {
                if (err) {
                    if (err.notStored) {
                        bank.create(type, id, [toAppend], this);
                    } else {
                        throw err;
                    }
                } else {
                    this(null);
                }
            },
            function(err, result) {
                if (err) {
                    if (err instanceof AlreadyExistsError) {
                        // try again
                        bank.append(type, id, toAppend, callback);
                    } else {
                        throw err;
                    }
                } else {
                    callback(null);
                }
            },
            callback
        );
    };

    bank.prepend = function(type, id, toPrepend, callback) {
        var bank = this,
            key = toKey(type, id),
            value = SEP + freeze(toPrepend);

        Step(
            function() {
                client.prepend(key, value, this);
            },
            function(err, result) {
                if (err) {
                    if (err.notStored) {
                        bank.create(type, id, [toPrepend], this);
                    } else {
                        throw err;
                    }
                } else {
                    this(null);
                }
            },
            function(err, result) {
                if (err) {
                    if (err instanceof AlreadyExistsError) {
                        // try again
                        bank.prepend(type, id, toPrepend, callback);
                    } else {
                        throw err;
                    }
                } else {
                    callback(null);
                }
            },
            callback
        );
    };
};

MemcachedDatabank.prototype = new Databank();

module.exports = MemcachedDatabank;
