this['$'] = this['$'] || this;
this['$']['$mol'] = this['$'];
//mol.js.map
;
var $;
(function ($) {
    function $mol_merge_dict(target, source) {
        var result = {};
        for (var key in target)
            result[key] = target[key];
        for (var key in source)
            result[key] = source[key];
        return result;
    }
    $.$mol_merge_dict = $mol_merge_dict;
})($ || ($ = {}));
//dict.js.map
;
var $;
(function ($) {
    function $mol_log(path, values) {
        var filter = $mol_log.filter();
        if (filter == null)
            return;
        if (path.indexOf(filter) === -1)
            return;
        var time = new Date().toLocaleTimeString();
        console.log.apply(console, [time, path].concat(values));
    }
    $.$mol_log = $mol_log;
    var $mol_log;
    (function ($mol_log) {
        var _filter;
        function filter() {
            var diff = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                diff[_i - 0] = arguments[_i];
            }
            if (diff[0] !== void 0) {
                if (diff[0] == null) {
                    sessionStorage.removeItem('$mol_log.filter()');
                }
                else {
                    sessionStorage.setItem('$mol_log.filter()', diff[0]);
                }
                _filter = diff[0];
            }
            if (_filter !== void 0)
                return _filter;
            return _filter = sessionStorage.getItem('$mol_log.filter()');
        }
        $mol_log.filter = filter;
    })($mol_log = $.$mol_log || ($.$mol_log = {}));
})($ || ($ = {}));
//log.web.js.map
;
var $;
(function ($) {
    var $mol_object = (function () {
        function $mol_object() {
            this['destroyed()'] = false;
        }
        $mol_object.prototype.Class = function () {
            return this.constructor;
        };
        $mol_object.objectPath = function () {
            var self = this;
            return self['name']
                || self['displayName']
                || (self['displayName'] = Function.prototype.toString.call(self)
                    .match(/^function ([a-z0-9_$]*)/)[1]);
        };
        $mol_object.prototype.objectClassNames = function () {
            if (this.hasOwnProperty('objectClassNames()'))
                return this['objectClassNames()'];
            var names = [];
            var current = this;
            while (typeof current === 'object') {
                if (!current.constructor.objectPath)
                    break;
                var name = current.constructor.objectPath();
                if (!name)
                    continue;
                names.push(name);
                if (current === null)
                    break;
                current = Object.getPrototypeOf(current);
            }
            return this['objectClassNames()'] = names;
        };
        $mol_object.prototype.objectOwner = function (next) {
            if (this['objectOwner()'])
                return this['objectOwner()'];
            return this['objectOwner()'] = next;
        };
        $mol_object.prototype.objectField = function (next) {
            if (this['objectField()'])
                return this['objectField()'] || '';
            return this['objectField()'] = next;
        };
        $mol_object.prototype.objectPath = function (next) {
            var path = '';
            var owner = this.objectOwner();
            if (owner)
                path = owner.objectPath();
            var field = this.objectField();
            if (field)
                path += '.' + field;
            return path;
        };
        $mol_object.prototype.setup = function (script) {
            script(this);
            return this;
        };
        $mol_object.prototype.destroyed = function () {
            var diff = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                diff[_i - 0] = arguments[_i];
            }
            if (diff[0] === void 0)
                return this['destroyed()'];
            this['destroyed()'] = diff[0];
            this.log(['.destroyed()', diff[0]]);
            return diff[0];
        };
        $mol_object.prototype.log = function (values) {
            if ($.$mol_log.filter() == null)
                return;
            $.$mol_log(this.objectPath(), values);
        };
        $mol_object.toString = function () {
            return this.objectPath();
        };
        $mol_object.prototype.toString = function () {
            return this.objectPath();
        };
        return $mol_object;
    }());
    $.$mol_object = $mol_object;
})($ || ($ = {}));
//object.js.map
;
var $;
(function ($) {
    var $mol_set_shim = (function () {
        function $mol_set_shim() {
            this._index = {};
            this.size = 0;
        }
        $mol_set_shim.prototype.add = function (value) {
            var key = String(value);
            var list = this._index[key];
            if (list) {
                if (list.indexOf(value) !== -1)
                    return this;
                list.push(value);
            }
            else {
                list = this._index[key] = [value];
            }
            ++this.size;
            return this;
        };
        $mol_set_shim.prototype.has = function (value) {
            var key = String(value);
            var list = this._index[key];
            if (!list)
                return false;
            return list.indexOf(value) !== -1;
        };
        $mol_set_shim.prototype.delete = function (value) {
            var key = String(value);
            var list = this._index[key];
            if (!list)
                return;
            var index = list.indexOf(value);
            if (index === -1)
                return;
            list.splice(index, 1);
            --this.size;
        };
        $mol_set_shim.prototype.forEach = function (handle) {
            for (var key in this._index) {
                if (!this._index.hasOwnProperty(key))
                    continue;
                this._index[key].forEach(function (val, index) { return handle(val, val); });
            }
        };
        $mol_set_shim.prototype.keys = function () {
            var keys = [];
            this.forEach(function (val, key) {
                keys.push(key);
            });
            return keys;
        };
        $mol_set_shim.prototype.values = function () {
            var values = [];
            this.forEach(function (val, key) {
                values.push(val);
            });
            return values;
        };
        $mol_set_shim.prototype.entries = function () {
            var entries = [];
            this.forEach(function (val, key) {
                entries.push([val, key]);
            });
            return entries;
        };
        $mol_set_shim.prototype.clear = function () {
            this._index = {};
            this.size = 0;
        };
        return $mol_set_shim;
    }());
    $.$mol_set_shim = $mol_set_shim;
})($ || ($ = {}));
//set.js.map
;
$.$mol_set = ( typeof Set === 'function' ) ? Set : $.$mol_set_shim

;
var __extends = (this && this.__extends) || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
};
var $;
(function ($) {
    var $mol_defer = (function (_super) {
        __extends($mol_defer, _super);
        function $mol_defer(run) {
            _super.call(this);
            this.run = run;
            $mol_defer.add(this);
        }
        $mol_defer.prototype.destroyed = function () {
            var diff = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                diff[_i - 0] = arguments[_i];
            }
            if (diff[0])
                $mol_defer.drop(this);
            return _super.prototype.destroyed.apply(this, diff);
        };
        $mol_defer.schedule = function () {
            var _this = this;
            if (this.timer)
                return;
            this.timer = this.scheduleNative(function () {
                _this.timer = 0;
                _this.run();
            });
        };
        $mol_defer.unschedule = function () {
            if (!this.timer)
                return;
            cancelAnimationFrame(this.timer);
            this.timer = 0;
        };
        $mol_defer.add = function (defer) {
            this.all.push(defer);
            this.schedule();
        };
        $mol_defer.drop = function (defer) {
            var index = this.all.indexOf(defer);
            if (index >= 0)
                this.all.splice(index, 1);
        };
        $mol_defer.run = function () {
            if (this.all.length === 0)
                return;
            this.schedule();
            for (var defer; defer = this.all.pop();)
                defer.run();
        };
        $mol_defer.all = [];
        $mol_defer.timer = 0;
        $mol_defer.scheduleNative = (typeof requestAnimationFrame == 'function')
            ? function (handler) { return requestAnimationFrame(handler); }
            : function (handler) { return setTimeout(handler, 16); };
        return $mol_defer;
    }($.$mol_object));
    $.$mol_defer = $mol_defer;
})($ || ($ = {}));
//defer.js.map
;
var $;
(function ($) {
    var $mol_dict_shim = (function () {
        function $mol_dict_shim() {
            this._keys = {};
            this._values = {};
            this.size = 0;
        }
        $mol_dict_shim.prototype.set = function (key, value) {
            var keyStr = String(key);
            var keys = this._keys[keyStr];
            if (keys) {
                var index = keys.indexOf(key);
                if (index === -1) {
                    index = keys.length;
                    keys.push(key);
                    ++this.size;
                }
                this._values[keyStr][index] = value;
            }
            else {
                this._keys[keyStr] = [key];
                this._values[keyStr] = [value];
                ++this.size;
            }
            return this;
        };
        $mol_dict_shim.prototype.get = function (key) {
            var keyStr = String(key);
            var list = this._keys[keyStr];
            if (!list)
                return void 0;
            var index = list.indexOf(key);
            if (index === -1)
                return void 0;
            return this._values[keyStr][index];
        };
        $mol_dict_shim.prototype.has = function (key) {
            var keyStr = String(key);
            var list = this._keys[keyStr];
            if (!list)
                return false;
            return list.indexOf(key) !== -1;
        };
        $mol_dict_shim.prototype.delete = function (key) {
            var keyStr = String(key);
            var keys = this._keys[keyStr];
            if (!keys)
                return;
            var index = keys.indexOf(key);
            if (index === -1)
                return;
            keys.splice(index, 1);
            this._values[keyStr].splice(index, 1);
            --this.size;
        };
        $mol_dict_shim.prototype.forEach = function (handle) {
            for (var keyStr in this._keys) {
                if (!this._keys.hasOwnProperty(keyStr))
                    continue;
                var values = this._values[keyStr];
                this._keys[keyStr].forEach(function (key, index) {
                    handle(values[index], key);
                });
            }
        };
        $mol_dict_shim.prototype.keys = function () {
            var keys = [];
            this.forEach(function (val, key) {
                keys.push(key);
            });
            return keys;
        };
        $mol_dict_shim.prototype.values = function () {
            var values = [];
            this.forEach(function (val, key) {
                values.push(val);
            });
            return values;
        };
        $mol_dict_shim.prototype.entries = function () {
            var entries = [];
            this.forEach(function (val, key) {
                entries.push([key, val]);
            });
            return entries;
        };
        $mol_dict_shim.prototype.clear = function () {
            this._keys = {};
            this._values = {};
            this.size = 0;
        };
        return $mol_dict_shim;
    }());
    $.$mol_dict_shim = $mol_dict_shim;
})($ || ($ = {}));
//dict.js.map
;
$.$mol_dict = ( typeof Map === 'function' ) ? Map : $.$mol_dict_shim

;
var $;
(function ($) {
    $.$mol_state_stack = new $.$mol_dict();
})($ || ($ = {}));
//stack.js.map
;
var __extends = (this && this.__extends) || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
};
var $;
(function ($) {
    (function ($mol_atom_status) {
        $mol_atom_status[$mol_atom_status["obsolete"] = 'obsolete'] = "obsolete";
        $mol_atom_status[$mol_atom_status["checking"] = 'checking'] = "checking";
        $mol_atom_status[$mol_atom_status["actual"] = 'actual'] = "actual";
    })($.$mol_atom_status || ($.$mol_atom_status = {}));
    var $mol_atom_status = $.$mol_atom_status;
    var $mol_atom = (function (_super) {
        __extends($mol_atom, _super);
        function $mol_atom(host, field, handler, fail, key) {
            if (field === void 0) { field = 'value()'; }
            _super.call(this);
            this.host = host;
            this.field = field;
            this.handler = handler;
            this.fail = fail;
            this.key = key;
            this.masters = null;
            this.slaves = null;
            this.status = $mol_atom_status.obsolete;
            this.autoFresh = true;
        }
        $mol_atom.prototype.destroyed = function () {
            var diff = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                diff[_i - 0] = arguments[_i];
            }
            if (diff[0]) {
                this.unlink();
                var host = this.host || this;
                var value = host[this.field];
                if (value instanceof $.$mol_object) {
                    if ((value.objectOwner() === host) && (value.objectField() === this.field)) {
                        value.destroyed(true);
                    }
                }
                if (this.host) {
                    host[this.field] = void 0;
                    host['$mol_atom_state'][this.field] = void 0;
                }
                this['destroyed()'] = true;
                this.log(['.destroyed()', true, 'atom']);
                return true;
            }
            else {
                return this['destroyed()'];
            }
        };
        $mol_atom.prototype.unlink = function () {
            this.disobeyAll();
            this.checkSlaves();
        };
        $mol_atom.prototype.objectPath = function () {
            return this.host ? this.host.objectPath() + '.' + this.field : this.field;
        };
        $mol_atom.prototype.get = function () {
            if ($mol_atom.stack.indexOf(this) !== -1) {
                throw new Error('Recursive dependency! ' + this.objectPath());
            }
            var slave = $mol_atom.stack[$mol_atom.stack.length - 1];
            if (slave)
                this.lead(slave);
            if (slave)
                slave.obey(this);
            this.actualize();
            var value = (this.host || this)[this.field];
            if (value instanceof Error)
                throw value;
            return value;
        };
        $mol_atom.prototype.actualize = function () {
            var _this = this;
            if (this.status === $mol_atom_status.actual)
                return;
            var index = $mol_atom.stack.length;
            $mol_atom.stack.push(this);
            if (this.status === $mol_atom_status.checking) {
                this.masters.forEach(function (master) {
                    if (_this.status !== $mol_atom_status.checking)
                        return;
                    master.actualize();
                });
                if (this.status === $mol_atom_status.checking) {
                    this.status = $mol_atom_status.actual;
                }
            }
            if (this.status !== $mol_atom_status.actual) {
                var oldMasters = this.masters;
                this.masters = null;
                if (oldMasters)
                    oldMasters.forEach(function (master) {
                        master.dislead(_this);
                    });
                var host = this.host || this;
                if (this.key !== void 0) {
                    var next = this.handler.call(host, this.key);
                }
                else {
                    var next = this.handler.call(host);
                }
                if (next === void 0)
                    next = host[this.field];
                this.push(next);
            }
            $mol_atom.stack.length = index;
        };
        $mol_atom.prototype.set = function () {
            var diff = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                diff[_i - 0] = arguments[_i];
            }
            var host = this.host || this;
            if (this.key !== void 0) {
                var next = (_a = this.handler).call.apply(_a, [host, this.key].concat(diff));
            }
            else {
                var next = (_b = this.handler).call.apply(_b, [host].concat(diff));
            }
            if (next === void 0)
                return host[this.field];
            return this.push(next);
            var _a, _b;
        };
        $mol_atom.prototype.push = function (next) {
            var host = this.host || this;
            var prev = host[this.field];
            if (next instanceof Error && this.fail) {
                if (this.key !== void 0) {
                    next = this.fail.call(host, this.key, host, next);
                }
                else {
                    next = this.fail.call(host, host, next);
                }
            }
            comparing: if ((next instanceof Array) && (prev instanceof Array) && (next.length === prev.length)) {
                for (var i = 0; i < next['length']; ++i) {
                    if (next[i] !== prev[i])
                        break comparing;
                }
                next = prev;
            }
            if (prev !== next) {
                if (next instanceof $.$mol_object) {
                    next['objectField'](this.field);
                    next['objectOwner'](host);
                }
                host[this.field] = next;
                this.log(['push', next, prev]);
                if (next instanceof Error) {
                    if (this.slaves)
                        this.slaves.forEach(function (slave) { return slave.push(next); });
                }
                else {
                    this.obsoleteSlaves();
                }
            }
            this.status = $mol_atom_status.actual;
            return next;
        };
        $mol_atom.prototype.obsoleteSlaves = function () {
            if (!this.slaves)
                return;
            this.slaves.forEach(function (slave) { return slave.obsolete(); });
        };
        $mol_atom.prototype.checkSlaves = function () {
            if (this.slaves) {
                this.slaves.forEach(function (slave) { return slave.check(); });
            }
            else {
                if (this.autoFresh)
                    $mol_atom.actualize(this);
            }
        };
        $mol_atom.prototype.check = function () {
            if (this.status === $mol_atom_status.actual) {
                this.status = $mol_atom_status.checking;
                this.checkSlaves();
            }
        };
        $mol_atom.prototype.obsolete = function () {
            if (this.status === $mol_atom_status.obsolete)
                return;
            this.log(['obsolete']);
            this.status = $mol_atom_status.obsolete;
            this.checkSlaves();
            return void 0;
        };
        $mol_atom.prototype.lead = function (slave) {
            if (!this.slaves) {
                this.slaves = new $.$mol_set();
                $mol_atom.unreap(this);
            }
            this.slaves.add(slave);
        };
        $mol_atom.prototype.dislead = function (slave) {
            if (!this.slaves)
                return;
            if (this.slaves.size === 1) {
                this.slaves = null;
                $mol_atom.reap(this);
            }
            else {
                this.slaves.delete(slave);
            }
        };
        $mol_atom.prototype.obey = function (master) {
            if (!this.masters)
                this.masters = new $.$mol_set();
            this.masters.add(master);
        };
        $mol_atom.prototype.disobey = function (master) {
            if (!this.masters)
                return;
            this.masters.delete(master);
        };
        $mol_atom.prototype.disobeyAll = function () {
            var _this = this;
            if (!this.masters)
                return;
            this.masters.forEach(function (master) { return master.dislead(_this); });
            this.masters = null;
        };
        $mol_atom.prototype.value = function () {
            var diff = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                diff[_i - 0] = arguments[_i];
            }
            if (diff[0] === void 0) {
                if (diff.length > 1)
                    return this.push(diff[1]);
                if (diff.length > 0)
                    return this.obsolete();
                return this.get();
            }
            else {
                return this.set.apply(this, diff);
            }
        };
        $mol_atom.actualize = function (atom) {
            $mol_atom.updating.push(atom);
            $mol_atom.schedule();
        };
        $mol_atom.reap = function (atom) {
            $mol_atom.reaping.add(atom);
            $mol_atom.schedule();
        };
        $mol_atom.unreap = function (atom) {
            $mol_atom.reaping.delete(atom);
        };
        $mol_atom.schedule = function () {
            var _this = this;
            if (this.scheduled)
                return;
            new $.$mol_defer(function () {
                if (!_this.scheduled)
                    return;
                _this.scheduled = false;
                _this.sync();
            });
            this.scheduled = true;
        };
        $mol_atom.sync = function () {
            var _this = this;
            $.$mol_log('$mol_atom.sync', []);
            this.schedule();
            while (this.updating.length) {
                var atom = this.updating.shift();
                if (!atom.destroyed())
                    atom.actualize();
            }
            while (this.reaping.size) {
                this.reaping.forEach(function (atom) {
                    _this.reaping.delete(atom);
                    if (!atom.slaves)
                        atom.destroyed(true);
                });
            }
            this.scheduled = false;
        };
        $mol_atom.stack = [];
        $mol_atom.updating = [];
        $mol_atom.reaping = new $.$mol_set();
        $mol_atom.scheduled = false;
        return $mol_atom;
    }($.$mol_object));
    $.$mol_atom = $mol_atom;
    function $mol_atom_restore(error) {
        if ($mol_atom.stack.length) {
            var atom = $mol_atom.stack.pop();
            if (error instanceof Error) {
                error = atom.push(error);
            }
        }
        $mol_atom.stack.splice(0, $mol_atom.stack.length);
    }
    $.$mol_atom_restore = $mol_atom_restore;
    $.$mol_state_stack.set('$mol_atom.stack', $mol_atom.stack);
    var $mol_atom_wait = (function (_super) {
        __extends($mol_atom_wait, _super);
        function $mol_atom_wait(message) {
            if (message === void 0) { message = 'Wait...'; }
            _super.call(this, message);
            this.message = message;
            this.name = '$mol_atom_wait';
        }
        return $mol_atom_wait;
    }(Error));
    $.$mol_atom_wait = $mol_atom_wait;
    function $mol_atom_task(handler, fail) {
        var atom = new $mol_atom(null, 'value()', function () {
            handler();
            atom.destroyed(true);
        }, fail);
        $mol_atom.actualize(atom);
        return atom;
    }
    $.$mol_atom_task = $mol_atom_task;
})($ || ($ = {}));
//atom.js.map
;
var $;
(function ($) {
    window.addEventListener('error', function (event) {
        var error = event.error;
        var stack = $.$mol_atom.stack;
        if (error instanceof $.$mol_atom_wait) {
            event.preventDefault();
            console.debug('', error);
        }
        $.$mol_atom_restore(error);
    });
})($ || ($ = {}));
//atom.web.js.map
;
var $;
(function ($) {
    function $mol_prop(config) {
        return function (obj, name, descr) {
            var value = descr.value;
            if (value.length) {
                descr.value = function (key) {
                    var diff = [];
                    for (var _i = 1; _i < arguments.length; _i++) {
                        diff[_i - 1] = arguments[_i];
                    }
                    var host = this;
                    var field = name + "(" + JSON.stringify(key) + ")";
                    var atoms = host['$mol_atom_state'];
                    if (!atoms)
                        atoms = host['$mol_atom_state'] = {};
                    var info = atoms[field];
                    if (!info) {
                        atoms[field] = info = new $.$mol_atom(host, field, value, config && config.fail, key);
                        if (config)
                            info.autoFresh = !config.lazy;
                    }
                    return info.value.apply(info, diff);
                };
            }
            else {
                descr.value = function () {
                    var diff = [];
                    for (var _i = 0; _i < arguments.length; _i++) {
                        diff[_i - 0] = arguments[_i];
                    }
                    var host = this;
                    var field = name + "()";
                    var atoms = host['$mol_atom_state'];
                    if (!atoms)
                        atoms = host['$mol_atom_state'] = {};
                    var info = atoms[field];
                    if (!info) {
                        atoms[field] = info = new $.$mol_atom(host, field, value, config && config.fail);
                        if (config)
                            info.autoFresh = !config.lazy;
                    }
                    return info.value.apply(info, diff);
                };
            }
            void (descr.value['value'] = value);
        };
    }
    $.$mol_prop = $mol_prop;
})($ || ($ = {}));
//prop.js.map
;
var __extends = (this && this.__extends) || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
};
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var $;
(function ($) {
    var $mol_window = (function (_super) {
        __extends($mol_window, _super);
        function $mol_window() {
            _super.apply(this, arguments);
        }
        $mol_window.size = function () {
            var diff = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                diff[_i - 0] = arguments[_i];
            }
            return [window.innerWidth, window.innerHeight];
        };
        __decorate([
            $.$mol_prop()
        ], $mol_window, "size", null);
        return $mol_window;
    }($.$mol_object));
    $.$mol_window = $mol_window;
    window.addEventListener('resize', function () {
        $mol_window.size(void 0);
    });
})($ || ($ = {}));
//window.web.js.map
;
var __extends = (this && this.__extends) || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
};
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var $;
(function ($) {
    var $mol_http_request = (function (_super) {
        __extends($mol_http_request, _super);
        function $mol_http_request() {
            _super.apply(this, arguments);
        }
        $mol_http_request.prototype.uri = function () { return ''; };
        $mol_http_request.prototype.method = function () { return 'Get'; };
        $mol_http_request.prototype.body = function () { return null; };
        $mol_http_request.prototype.native = function () {
            var _this = this;
            if (this['native()'])
                return this['native()'];
            var next = this['native()'] = $.$mol_http_request_native();
            next.onload = function (event) {
                if (Math.floor(next.status / 100) === 2) {
                    _this.response(void 0, next);
                }
                else {
                    _this.response(void 0, new Error(next.responseText));
                }
            };
            next.onerror = function (event) {
                _this.response(void 0, event.error || new Error('Unknown HTTP error'));
            };
            return next;
        };
        $mol_http_request.prototype.destroyed = function () {
            var diff = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                diff[_i - 0] = arguments[_i];
            }
            if (diff[0]) {
                var native = this['native()'];
                if (native)
                    native.abort();
            }
            return _super.prototype.destroyed.apply(this, diff);
        };
        $mol_http_request.prototype.response = function () {
            var diff = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                diff[_i - 0] = arguments[_i];
            }
            if (diff[0] !== void 0)
                return diff[0];
            var native = this.native();
            native.open(this.method(), this.uri());
            native.send(this.body());
            throw new $.$mol_atom_wait(this.method() + " " + this.uri());
        };
        $mol_http_request.prototype.text = function () {
            var diff = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                diff[_i - 0] = arguments[_i];
            }
            if (diff.length === 1)
                this.response(void 0);
            else
                return this.response().responseText;
        };
        __decorate([
            $.$mol_prop()
        ], $mol_http_request.prototype, "response", null);
        return $mol_http_request;
    }($.$mol_object));
    $.$mol_http_request = $mol_http_request;
})($ || ($ = {}));
//request.js.map
;
var $;
(function ($) {
    $.$mol_http_request_native = function () { return new XMLHttpRequest; };
})($ || ($ = {}));
//request.web.js.map
;
var __extends = (this && this.__extends) || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
};
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var $;
(function ($) {
    var $mol_http_resource = (function (_super) {
        __extends($mol_http_resource, _super);
        function $mol_http_resource() {
            _super.apply(this, arguments);
        }
        $mol_http_resource.item = function (uri) {
            return new $mol_http_resource().setup(function (obj) {
                obj.uri = function () { return uri; };
            });
        };
        $mol_http_resource.prototype.uri = function () { return ''; };
        $mol_http_resource.prototype.request = function (method) {
            var _this = this;
            var request = new $.$mol_http_request();
            request.method = function () { return method; };
            request.uri = function () { return _this.uri(); };
            return request;
        };
        $mol_http_resource.prototype.downloader = function () {
            var diff = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                diff[_i - 0] = arguments[_i];
            }
            this.dataNext(void 0, void 0);
            return this.request('Get');
        };
        $mol_http_resource.prototype.uploader = function () {
            var body = this.dataNext();
            if (body === void 0)
                return null;
            var request = this.request('Put');
            request.body = function () { return body; };
            return request;
        };
        $mol_http_resource.prototype.uploaded = function () {
            var diff = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                diff[_i - 0] = arguments[_i];
            }
            if (!this.uploader())
                return null;
            this.text(void 0, this.uploader().text());
            return true;
        };
        $mol_http_resource.prototype.text = function () {
            var diff = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                diff[_i - 0] = arguments[_i];
            }
            if (diff.length === 0) {
                return this.downloader().text();
            }
            else if (diff[0] === void 0) {
                this.downloader(void 0);
            }
            else {
                this.dataNext(diff[0]);
            }
        };
        $mol_http_resource.prototype.dataNext = function () {
            var diff = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                diff[_i - 0] = arguments[_i];
            }
            return diff[0];
        };
        $mol_http_resource.prototype.refresh = function () {
            this.downloader(void 0);
        };
        __decorate([
            $.$mol_prop()
        ], $mol_http_resource.prototype, "downloader", null);
        __decorate([
            $.$mol_prop()
        ], $mol_http_resource.prototype, "uploader", null);
        __decorate([
            $.$mol_prop()
        ], $mol_http_resource.prototype, "uploaded", null);
        __decorate([
            $.$mol_prop()
        ], $mol_http_resource.prototype, "text", null);
        __decorate([
            $.$mol_prop()
        ], $mol_http_resource.prototype, "dataNext", null);
        __decorate([
            $.$mol_prop()
        ], $mol_http_resource, "item", null);
        return $mol_http_resource;
    }($.$mol_object));
    $.$mol_http_resource = $mol_http_resource;
    var $mol_http_resource_json = (function (_super) {
        __extends($mol_http_resource_json, _super);
        function $mol_http_resource_json() {
            _super.apply(this, arguments);
        }
        $mol_http_resource_json.item = function (uri) {
            return new $mol_http_resource_json().setup(function (obj) {
                obj.uri = function () { return uri; };
            });
        };
        $mol_http_resource_json.prototype.json = function () {
            var diff = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                diff[_i - 0] = arguments[_i];
            }
            if (diff.length === 0) {
                return JSON.parse(this.text());
            }
            else if (diff[0] === void 0) {
                this.text(void 0);
            }
            else {
                this.text.apply(this, diff.map(function (val) { return JSON.stringify(val, null, '\t'); }));
            }
        };
        __decorate([
            $.$mol_prop()
        ], $mol_http_resource_json, "item", null);
        return $mol_http_resource_json;
    }($mol_http_resource));
    $.$mol_http_resource_json = $mol_http_resource_json;
})($ || ($ = {}));
//resource.js.map
;
var __extends = (this && this.__extends) || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
};
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var $;
(function ($) {
    var $mol_locale = (function (_super) {
        __extends($mol_locale, _super);
        function $mol_locale() {
            _super.apply(this, arguments);
        }
        $mol_locale.lang = function () {
            var diff = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                diff[_i - 0] = arguments[_i];
            }
            return diff[0] || 'en';
        };
        $mol_locale.texts = function () {
            var uri = "-/web.locale=" + this.lang() + ".json";
            var resource = $.$mol_http_resource_json.item(uri);
            return resource.json();
        };
        $mol_locale.text = function (context, key) {
            return this.texts()[(context + "_" + key)];
        };
        __decorate([
            $.$mol_prop()
        ], $mol_locale, "lang", null);
        return $mol_locale;
    }($.$mol_object));
    $.$mol_locale = $mol_locale;
})($ || ($ = {}));
//locale.js.map
;
var __extends = (this && this.__extends) || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
};
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var $;
(function ($) {
    $.$mol_viewer_context = {};
    $.$mol_viewer_context.$mol_viewer_heightLimit = function () { return $.$mol_window.size()[1]; };
    var $mol_viewer = (function (_super) {
        __extends($mol_viewer, _super);
        function $mol_viewer() {
            _super.apply(this, arguments);
        }
        $mol_viewer.root = function (id) {
            return new this;
        };
        $mol_viewer.statePrefix = function () {
            return '';
        };
        $mol_viewer.prototype.statePrefix = function () {
            var owner = this.objectOwner();
            return owner ? owner.statePrefix() : '';
        };
        $mol_viewer.prototype.stateKey = function (postfix) {
            return this.statePrefix() + postfix;
        };
        $mol_viewer.prototype.context = function () {
            var diff = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                diff[_i - 0] = arguments[_i];
            }
            return diff[0] || $.$mol_viewer_context;
        };
        $mol_viewer.prototype.contextSub = function () {
            return this.context();
        };
        $mol_viewer.prototype.tagName = function () { return 'div'; };
        $mol_viewer.prototype.nameSpace = function () { return 'http://www.w3.org/1999/xhtml'; };
        $mol_viewer.prototype.childs = function () {
            return null;
        };
        $mol_viewer.prototype.childsVisible = function () {
            var childs = this.childs();
            if (!childs)
                return childs;
            var context = this.contextSub();
            for (var i = 0; i < childs.length; ++i) {
                var child = childs[i];
                if (child instanceof $mol_viewer) {
                    child.context(context);
                }
            }
            return childs;
        };
        $mol_viewer.prototype.heightMinimal = function () {
            return 0;
        };
        $mol_viewer.prototype.DOMNode = function () {
            var diff = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                diff[_i - 0] = arguments[_i];
            }
            var path = this.objectPath();
            var next = diff[0];
            if (!next) {
                next = this['DOMNode()'];
                if (next)
                    return next;
                next = document.getElementById(path);
                if (!next) {
                    next = document.createElementNS(this.nameSpace(), this.tagName());
                }
            }
            next.id = path;
            void (next['$mol_viewer'] = this);
            this['DOMNode()'] = next;
            var ownerProto = this.objectOwner() && Object.getPrototypeOf(this.objectOwner());
            if (ownerProto && ownerProto['objectClassNames']) {
                for (var _a = 0, _b = ownerProto['objectClassNames'](); _a < _b.length; _a++) {
                    var className = _b[_a];
                    var attrName = className.replace(/\$/g, '') + '_' + this.objectField().replace(/\(.*/, '');
                    next.setAttribute(attrName, '');
                    if (className === '$mol_viewer')
                        break;
                }
            }
            var proto = Object.getPrototypeOf(this);
            for (var _c = 0, _d = proto['objectClassNames'](); _c < _d.length; _c++) {
                var className = _d[_c];
                next.setAttribute(className.replace(/\$/g, ''), '');
                if (className === '$mol_viewer')
                    break;
            }
            var events = this.event();
            var _loop_1 = function(name_1) {
                var handle = events[name_1];
                next.addEventListener(name_1, function (event) {
                    handle(event);
                });
            };
            for (var name_1 in events) {
                _loop_1(name_1);
            }
            return next;
        };
        $mol_viewer.prototype.DOMTree = function () {
            var diff = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                diff[_i - 0] = arguments[_i];
            }
            var node = this.DOMNode();
            var childs = this.childsVisible();
            if (childs != null) {
                var nextNode = node.firstChild;
                for (var _a = 0, childs_1 = childs; _a < childs_1.length; _a++) {
                    var view = childs_1[_a];
                    if (view == null) {
                    }
                    else if (typeof view === 'object') {
                        var existsNode = (view instanceof $mol_viewer) ? view.DOMNode() : view;
                        while (true) {
                            if (!nextNode) {
                                node.appendChild(existsNode);
                                break;
                            }
                            if (nextNode == existsNode) {
                                nextNode = nextNode.nextSibling;
                                break;
                            }
                            else {
                                node.insertBefore(existsNode, nextNode);
                                break;
                            }
                        }
                    }
                    else {
                        if (nextNode && nextNode.nodeName === '#text') {
                            nextNode.nodeValue = String(view);
                            nextNode = nextNode.nextSibling;
                        }
                        else {
                            var textNode = document.createTextNode(String(view));
                            node.insertBefore(textNode, nextNode);
                        }
                    }
                }
                while (nextNode) {
                    var currNode = nextNode;
                    nextNode = currNode.nextSibling;
                    node.removeChild(currNode);
                }
                for (var _b = 0, childs_2 = childs; _b < childs_2.length; _b++) {
                    var view = childs_2[_b];
                    if (view instanceof $mol_viewer)
                        view.DOMTree();
                }
            }
            var attrs = this.attr();
            for (var name_2 in attrs) {
                var val_1 = attrs[name_2]();
                if ((val_1 == null) || (val_1 === false)) {
                    node.removeAttribute(name_2);
                }
                else if (val_1 === true) {
                    node.setAttribute(name_2, name_2);
                }
                else {
                    node.setAttribute(name_2, String(val_1));
                }
            }
            var fields = this.field();
            for (var path in fields) {
                var names = path.split('.');
                var obj = node;
                for (var i = 0; i < names.length - 1; ++i) {
                    if (names[i])
                        obj = obj[names[i]];
                }
                var field = names[names.length - 1];
                var val = fields[path]();
                if (obj[field] !== val)
                    obj[field] = val;
            }
            return node;
        };
        $mol_viewer.prototype.attr = function () { return { 'mol_viewer_error': function () { return false; } }; };
        $mol_viewer.prototype.field = function () { return {}; };
        $mol_viewer.prototype.event = function () { return {}; };
        $mol_viewer.prototype.focused = function () {
            return $.$mol_viewer_selection.focused().indexOf(this.DOMNode()) !== -1;
        };
        $mol_viewer.prototype.text = function (postfix) {
            var contexts = Object.getPrototypeOf(this).objectClassNames();
            for (var _i = 0, contexts_1 = contexts; _i < contexts_1.length; _i++) {
                var context = contexts_1[_i];
                var text = $.$mol_locale.text(context, postfix);
                if (text != null)
                    return text;
            }
            throw new Error("Locale text not found: [" + contexts.join('|') + "]_" + postfix);
        };
        __decorate([
            $.$mol_prop()
        ], $mol_viewer.prototype, "context", null);
        __decorate([
            $.$mol_prop({
                fail: function (self, error) {
                    var node = self.DOMNode();
                    if (node && !error['$mol_viewer_catched']) {
                        node.setAttribute('mol_viewer_error', error.name);
                    }
                    error['$mol_viewer_catched'] = true;
                    return error;
                }
            })
        ], $mol_viewer.prototype, "DOMTree", null);
        __decorate([
            $.$mol_prop()
        ], $mol_viewer, "root", null);
        return $mol_viewer;
    }($.$mol_object));
    $.$mol_viewer = $mol_viewer;
})($ || ($ = {}));
//viewer.js.map
;
var $;
(function ($) {
    document.addEventListener('DOMContentLoaded', function (event) {
        var nodes = document.querySelectorAll('[mol_viewer_root]');
        var _loop_1 = function(i) {
            var view = $[nodes.item(i).getAttribute('mol_viewer_root')].root(i);
            view.DOMNode(nodes.item(i));
            new $.$mol_defer(function () { return view.DOMTree(); });
        };
        for (var i = nodes.length - 1; i >= 0; --i) {
            _loop_1(i);
        }
        $.$mol_defer.run();
    });
})($ || ($ = {}));
//viewer.web.js.map
;
var __extends = (this && this.__extends) || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
};
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var $;
(function ($) {
    var $mol_viewer_selection = (function (_super) {
        __extends($mol_viewer_selection, _super);
        function $mol_viewer_selection() {
            _super.apply(this, arguments);
        }
        $mol_viewer_selection.focused = function () {
            var diff = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                diff[_i - 0] = arguments[_i];
            }
            return diff[0] || [];
        };
        $mol_viewer_selection.position = function () {
            var diff = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                diff[_i - 0] = arguments[_i];
            }
            if (diff.length) {
                if (!diff[0])
                    return diff[0];
                var start = diff[0].start;
                var end = diff[0].end;
                if (!(start <= end))
                    throw new Error("Wrong offsets (" + start + "," + end + ")");
                var root = document.getElementById(diff[0].id);
                root.focus();
                var range = new Range;
                var cur = root.firstChild;
                while (cur !== root) {
                    while (cur.firstChild)
                        cur = cur.firstChild;
                    if (cur.nodeValue) {
                        var length = cur.nodeValue.length;
                        if (length >= start)
                            break;
                        start -= length;
                    }
                    while (!cur.nextSibling) {
                        cur = cur.parentNode;
                        if (cur === root) {
                            start = root.childNodes.length;
                            break;
                        }
                    }
                }
                range.setStart(cur, start);
                var cur = root.firstChild;
                while (cur !== root) {
                    while (cur.firstChild)
                        cur = cur.firstChild;
                    if (cur.nodeValue) {
                        var length = cur.nodeValue.length;
                        if (length >= end)
                            break;
                        end -= length;
                    }
                    while (!cur.nextSibling) {
                        cur = cur.parentNode;
                        if (cur === root) {
                            end = root.childNodes.length;
                            break;
                        }
                    }
                }
                range.setEnd(cur, end);
                var sel = document.getSelection();
                sel.removeAllRanges();
                sel.addRange(range);
                return diff[0];
            }
            else {
                var sel = document.getSelection();
                if (sel.rangeCount === 0)
                    return null;
                var range = sel.getRangeAt(0);
                var el = range.commonAncestorContainer;
                while (el && !el.id)
                    el = el.parentElement;
                if (!el)
                    return { id: null, start: 0, end: 0 };
                var meter = new Range;
                meter.selectNodeContents(el);
                meter.setEnd(range.startContainer, range.startOffset);
                var startOffset = meter.toString().length;
                meter.setEnd(range.endContainer, range.endOffset);
                var endOffset = meter.toString().length;
                return { id: el.id, start: startOffset, end: endOffset };
            }
        };
        $mol_viewer_selection.onFocus = function (event) {
            var parents = [];
            var element = event.target;
            while (element) {
                parents.push(element);
                element = element.parentElement;
            }
            $mol_viewer_selection.focused(parents);
        };
        $mol_viewer_selection.onBlur = function (event) {
            $mol_viewer_selection.focused([]);
        };
        __decorate([
            $.$mol_prop()
        ], $mol_viewer_selection, "focused", null);
        __decorate([
            $.$mol_prop()
        ], $mol_viewer_selection, "position", null);
        return $mol_viewer_selection;
    }($.$mol_object));
    $.$mol_viewer_selection = $mol_viewer_selection;
})($ || ($ = {}));
//selection.js.map
;
var $;
(function ($) {
    document.addEventListener('selectionchange', function (event) {
        $.$mol_viewer_selection.position(void 0);
    });
    document.addEventListener('focusin', $.$mol_viewer_selection.onFocus);
    document.addEventListener('focus', $.$mol_viewer_selection.onFocus, true);
    document.addEventListener('focusout', $.$mol_viewer_selection.onBlur);
    document.addEventListener('blur', $.$mol_viewer_selection.onBlur, true);
})($ || ($ = {}));
//selection.web.js.map
;
var __extends = (this && this.__extends) || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
};
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var $;
(function ($) {
    var $mol_stringer = (function (_super) {
        __extends($mol_stringer, _super);
        function $mol_stringer() {
            _super.apply(this, arguments);
        }
        $mol_stringer.prototype.tagName = function () {
            return "input";
        };
        $mol_stringer.prototype.enabled = function () {
            return true;
        };
        $mol_stringer.prototype.hint = function () {
            return "";
        };
        $mol_stringer.prototype.type = function () {
            return "text";
        };
        $mol_stringer.prototype.attr = function () {
            var _this = this;
            return $.$mol_merge_dict(_super.prototype.attr.call(this), {
                "placeholder": function () { return _this.hint(); },
                "type": function () { return _this.type(); },
            });
        };
        $mol_stringer.prototype.disabled = function () {
            return false;
        };
        $mol_stringer.prototype.value = function () {
            var diff = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                diff[_i - 0] = arguments[_i];
            }
            return (diff[0] !== void 0) ? diff[0] : "";
        };
        $mol_stringer.prototype.valueChanged = function () {
            var diff = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                diff[_i - 0] = arguments[_i];
            }
            return this.value.apply(this, diff);
        };
        $mol_stringer.prototype.field = function () {
            var _this = this;
            return $.$mol_merge_dict(_super.prototype.field.call(this), {
                "disabled": function () { return _this.disabled(); },
                "value": function () { return _this.valueChanged(); },
            });
        };
        $mol_stringer.prototype.eventChange = function () {
            var diff = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                diff[_i - 0] = arguments[_i];
            }
            return (diff[0] !== void 0) ? diff[0] : null;
        };
        $mol_stringer.prototype.event = function () {
            var _this = this;
            return $.$mol_merge_dict(_super.prototype.event.call(this), {
                "input": function () {
                    var diff = [];
                    for (var _i = 0; _i < arguments.length; _i++) {
                        diff[_i - 0] = arguments[_i];
                    }
                    return _this.eventChange.apply(_this, diff);
                },
            });
        };
        __decorate([
            $.$mol_prop()
        ], $mol_stringer.prototype, "value", null);
        __decorate([
            $.$mol_prop()
        ], $mol_stringer.prototype, "eventChange", null);
        return $mol_stringer;
    }($.$mol_viewer));
    $.$mol_stringer = $mol_stringer;
})($ || ($ = {}));
//stringer.view.tree.js.map
;
var __extends = (this && this.__extends) || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
};
var $;
(function ($) {
    var $mol;
    (function ($mol) {
        var $mol_stringer = (function (_super) {
            __extends($mol_stringer, _super);
            function $mol_stringer() {
                _super.apply(this, arguments);
            }
            $mol_stringer.prototype.eventChange = function () {
                var diff = [];
                for (var _i = 0; _i < arguments.length; _i++) {
                    diff[_i - 0] = arguments[_i];
                }
                this.value(this.DOMNode().value.trim());
            };
            $mol_stringer.prototype.disabled = function () {
                return !this.enabled();
            };
            return $mol_stringer;
        }($.$mol_stringer));
        $mol.$mol_stringer = $mol_stringer;
    })($mol = $.$mol || ($.$mol = {}));
})($ || ($ = {}));
//stringer.view.js.map
;
var __extends = (this && this.__extends) || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
};
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var $;
(function ($) {
    var $mol_app_hello = (function (_super) {
        __extends($mol_app_hello, _super);
        function $mol_app_hello() {
            _super.apply(this, arguments);
        }
        $mol_app_hello.prototype.name = function () {
            var diff = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                diff[_i - 0] = arguments[_i];
            }
            return (diff[0] !== void 0) ? diff[0] : "";
        };
        $mol_app_hello.prototype.namer = function () {
            var _this = this;
            var diff = [];
            for (var _i = 0; _i < arguments.length; _i++) {
                diff[_i - 0] = arguments[_i];
            }
            return (diff[0] !== void 0) ? diff[0] : new $.$mol_stringer().setup(function (__) {
                __.hint = function () { return _this.text("namer"); };
                __.value = function () {
                    var diff = [];
                    for (var _i = 0; _i < arguments.length; _i++) {
                        diff[_i - 0] = arguments[_i];
                    }
                    return _this.name.apply(_this, diff);
                };
            });
        };
        $mol_app_hello.prototype.message = function () {
            return "";
        };
        $mol_app_hello.prototype.childs = function () {
            return [].concat(this.namer(), this.message());
        };
        __decorate([
            $.$mol_prop()
        ], $mol_app_hello.prototype, "name", null);
        __decorate([
            $.$mol_prop()
        ], $mol_app_hello.prototype, "namer", null);
        return $mol_app_hello;
    }($.$mol_viewer));
    $.$mol_app_hello = $mol_app_hello;
})($ || ($ = {}));
//hello.view.tree.js.map
;
var __extends = (this && this.__extends) || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
};
var $;
(function ($) {
    var $mol;
    (function ($mol) {
        var $mol_app_hello = (function (_super) {
            __extends($mol_app_hello, _super);
            function $mol_app_hello() {
                _super.apply(this, arguments);
            }
            $mol_app_hello.prototype.message = function () {
                var name = this.name();
                return name && "Hello, " + name + "!";
            };
            return $mol_app_hello;
        }($.$mol_app_hello));
        $mol.$mol_app_hello = $mol_app_hello;
    })($mol = $.$mol || ($.$mol = {}));
})($ || ($ = {}));
//hello.view.js.map
//# sourceMappingURL=web.js.map