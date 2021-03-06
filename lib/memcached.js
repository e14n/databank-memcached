// memcached.js
//
// implementation of Databank interface using memcached
//
// Copyright 2011-2013 E14N https://e14n.com/
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

var crypto = require('crypto'),
    _ = require('underscore'),
    Step = require('step'),
    memcached = require('memcached'),
    databank = require('databank');

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

            var h = crypto.createHash('md5'),
                str, data;

            data = idstr;

            h.update(data);
            str = h.digest('base64');

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
        getIndices = function(type) {
            if (_.isObject(bank.schema) && _.isObject(bank.schema[type]) && _.isArray(bank.schema[type].indices)) {
                return bank.schema[type].indices;
            } else {
                return [];
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
                    _.each(getIndices(type), function(prop) {
                        k = Databank.deepProperty(value, prop);
                        bank.append('_databank_index', type + ':' + prop + ':' + k, key, group());
                    });
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

            bank.append('_databank_keys', type, key, callback);
        },
        deindex = function(type, id, callback) {

            var key = toKey(type, id);

            if (type.substr(0, 10) === '_databank_') {
                callback(null);
                return;
            }

            Step(
                function() {
                    bank.read(type, id, this);
                },
                function(err, value) {
                    var i = 0, group, k;

                    if (err && err instanceof NoSuchThingError) {
                        // All good! Must be new.
                        this(null);
                        return;
                    }

                    group = this.group();

                    if (err) throw err;
                    _.each(getIndices(type), function(prop) {
                        k = Databank.deepProperty(value, prop);
                        bank.remove('_databank_index', type + ':' + prop + ':' + k, key, group());
                    });
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
        scanKeys = function(keys, onResult, callback) {

            if (keys.length === 0) {
                callback(null);
                return;
            }

            Step(
                function() {
                    // FIXME: boxcar this; groups of 128, say?
                    client.getMulti(keys, this);
                },
                function(err, raws) {
                    var p, value;
                    if (err) throw err;
                    for (p in raws) {
                        if (raws[p] === false) {
                            continue;
                        }
                        value = melt(raws[p]);
                        onResult(value);
                    }
                    this(null);
                },
                callback
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
                if (err) {
                    if (err.notStored) {
                        throw new AlreadyExistsError(type, id); // Key miss
                    } else {
                        throw err;
                    }
                }
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

        var key = toKey(type, id);

        if (!client) {
            callback(new NotConnectedError());
            return;
        }

        client.get(key, function(err, value) {
            if ((err && err.notStored) || (!err && value === false)) {
                callback(new NoSuchThingError(type, id), null);
            } else if (err) {
                callback(err, null);
            } else {
                callback(null, melt(value));
            }
        });
    };

    bank.update = function(type, id, value, callback) {

        var key = toKey(type, id);

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
                if (err) {
                    if (err.notStored) {
                        throw new NoSuchThingError(type, id);
                    } else {
                        throw err;
                    }
                }
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

        var key = toKey(type, id);

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

        var key = toKey(type, id);

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
                if (err) {
                    if (err.notStored) {
                        throw new NoSuchThingError(type, id);
                    } else {
                        throw err;
                    }
                }
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

        var keys = [],
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

        var key = toKey(type, id);

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

        var key = toKey(type, id);

        client.decr(key, 1, function(err, result) {
            if (err) {
                callback(err, null);
            } else {
                callback(null, result);
            }
        });
    };

    bank.scan = function(type, onResult, callback) {

        Step(
            function() {
                bank.read('_databank_keys', type, this);
            },
            function(err, keys) {
                if (err) {
                    if (err instanceof NoSuchThingError) {
                        this(null);
                    } else {
                        throw err;
                    }
                } else {
                    // In some cases we can get blanks in the keys, so filter them out
                    keys = _.filter(keys, function(key) { return key.length > 0; });
                    scanKeys(keys, onResult, this);
                }
            },
            callback
        );
    };

    // Partially cadged from databank-redis

    bank.search = function(type, criteria, onResult, callback) {

        var indices = [],
            property,
            indexed = {},
            unindexed = {},
            haveIndexed = false,
            onScan = function(value) {
                if (bank.matchesCriteria(value, unindexed)) {
                    onResult(value);
                }
            };

        if (!client) {
            callback(new NotConnectedError(), null);
            return;
        }

        // Determine which criteria, if any, are on an indexed property

        indices = getIndices(type);

        _.each(criteria, function(property) {
            if (_.has(indices, property)) {
                haveIndexed = true;
                indexed[property] = criteria[property];
            } else {
                unindexed[property] = criteria[property];
            }
        });

        // If there are any indexed properties, use set intersection to get candidate keys
        if (haveIndexed) {
            Step(
                function() {
                    var property, group = this.group();
                    for (property in indexed) {
                        bank.read('_databank_index', type + ':' + property + ':' + criteria[property], group());
                    }
                },
                function(err, matches) {
                    var keys;
                    if (err) {
                        if (err instanceof NoSuchThingError) {
                            callback(null);
                            return;
                        } else {
                            throw err;
                        }
                    }

                    keys = _.intersection.apply(_, matches);

                    // Hard to tell empty index from a blank string, so filter out

                    keys = _.filter(keys, function(key) { return key.length > 0; });

                    scanKeys(keys, onScan, this);
                },
                callback
            );
        } else {
            unindexed = criteria;
            // Get every record of a given type
            bank.scan(type, onScan, callback);
        }
    };

    bank.append = function(type, id, toAppend, callback) {

        var key = toKey(type, id),
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
                        bank.append(type, id, toAppend, this);
                    } else {
                        throw err;
                    }
                } else {
                    this(null);
                }
            },
            callback
        );
    };

    bank.prepend = function(type, id, toPrepend, callback) {

        var key = toKey(type, id),
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
                        bank.prepend(type, id, toPrepend, this);
                    } else {
                        throw err;
                    }
                } else {
                    this(null);
                }
            },
            callback
        );
    };
};

MemcachedDatabank.prototype = new Databank();

module.exports = MemcachedDatabank;
