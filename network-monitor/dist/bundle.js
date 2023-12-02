(() => {
  "use strict";
  var t = {
      251: (t, e, n) => {
        n.r(e),
          n.d(e, {
            Builder: () => b,
            ByteBuffer: () => l,
            Encoding: () => u,
            FILE_IDENTIFIER_LENGTH: () => s,
            SIZEOF_INT: () => r,
            SIZEOF_SHORT: () => i,
            SIZE_PREFIX_LENGTH: () => o,
            float32: () => c,
            float64: () => d,
            int32: () => a,
            isLittleEndian: () => h,
          });
        const i = 2,
          r = 4,
          s = 4,
          o = 4,
          a = new Int32Array(2),
          c = new Float32Array(a.buffer),
          d = new Float64Array(a.buffer),
          h = 1 === new Uint16Array(new Uint8Array([1, 0]).buffer)[0];
        var u;
        !(function (t) {
          (t[(t.UTF8_BYTES = 1)] = "UTF8_BYTES"),
            (t[(t.UTF16_STRING = 2)] = "UTF16_STRING");
        })(u || (u = {}));
        class l {
          constructor(t) {
            (this.bytes_ = t),
              (this.position_ = 0),
              (this.text_decoder_ = new TextDecoder());
          }
          static allocate(t) {
            return new l(new Uint8Array(t));
          }
          clear() {
            this.position_ = 0;
          }
          bytes() {
            return this.bytes_;
          }
          position() {
            return this.position_;
          }
          setPosition(t) {
            this.position_ = t;
          }
          capacity() {
            return this.bytes_.length;
          }
          readInt8(t) {
            return (this.readUint8(t) << 24) >> 24;
          }
          readUint8(t) {
            return this.bytes_[t];
          }
          readInt16(t) {
            return (this.readUint16(t) << 16) >> 16;
          }
          readUint16(t) {
            return this.bytes_[t] | (this.bytes_[t + 1] << 8);
          }
          readInt32(t) {
            return (
              this.bytes_[t] |
              (this.bytes_[t + 1] << 8) |
              (this.bytes_[t + 2] << 16) |
              (this.bytes_[t + 3] << 24)
            );
          }
          readUint32(t) {
            return this.readInt32(t) >>> 0;
          }
          readInt64(t) {
            return BigInt.asIntN(
              64,
              BigInt(this.readUint32(t)) +
                (BigInt(this.readUint32(t + 4)) << BigInt(32))
            );
          }
          readUint64(t) {
            return BigInt.asUintN(
              64,
              BigInt(this.readUint32(t)) +
                (BigInt(this.readUint32(t + 4)) << BigInt(32))
            );
          }
          readFloat32(t) {
            return (a[0] = this.readInt32(t)), c[0];
          }
          readFloat64(t) {
            return (
              (a[h ? 0 : 1] = this.readInt32(t)),
              (a[h ? 1 : 0] = this.readInt32(t + 4)),
              d[0]
            );
          }
          writeInt8(t, e) {
            this.bytes_[t] = e;
          }
          writeUint8(t, e) {
            this.bytes_[t] = e;
          }
          writeInt16(t, e) {
            (this.bytes_[t] = e), (this.bytes_[t + 1] = e >> 8);
          }
          writeUint16(t, e) {
            (this.bytes_[t] = e), (this.bytes_[t + 1] = e >> 8);
          }
          writeInt32(t, e) {
            (this.bytes_[t] = e),
              (this.bytes_[t + 1] = e >> 8),
              (this.bytes_[t + 2] = e >> 16),
              (this.bytes_[t + 3] = e >> 24);
          }
          writeUint32(t, e) {
            (this.bytes_[t] = e),
              (this.bytes_[t + 1] = e >> 8),
              (this.bytes_[t + 2] = e >> 16),
              (this.bytes_[t + 3] = e >> 24);
          }
          writeInt64(t, e) {
            this.writeInt32(t, Number(BigInt.asIntN(32, e))),
              this.writeInt32(
                t + 4,
                Number(BigInt.asIntN(32, e >> BigInt(32)))
              );
          }
          writeUint64(t, e) {
            this.writeUint32(t, Number(BigInt.asUintN(32, e))),
              this.writeUint32(
                t + 4,
                Number(BigInt.asUintN(32, e >> BigInt(32)))
              );
          }
          writeFloat32(t, e) {
            (c[0] = e), this.writeInt32(t, a[0]);
          }
          writeFloat64(t, e) {
            (d[0] = e),
              this.writeInt32(t, a[h ? 0 : 1]),
              this.writeInt32(t + 4, a[h ? 1 : 0]);
          }
          getBufferIdentifier() {
            if (this.bytes_.length < this.position_ + r + s)
              throw new Error(
                "FlatBuffers: ByteBuffer is too short to contain an identifier."
              );
            let t = "";
            for (let e = 0; e < s; e++)
              t += String.fromCharCode(this.readInt8(this.position_ + r + e));
            return t;
          }
          __offset(t, e) {
            const n = t - this.readInt32(t);
            return e < this.readInt16(n) ? this.readInt16(n + e) : 0;
          }
          __union(t, e) {
            return (t.bb_pos = e + this.readInt32(e)), (t.bb = this), t;
          }
          __string(t, e) {
            t += this.readInt32(t);
            const n = this.readInt32(t);
            t += r;
            const i = this.bytes_.subarray(t, t + n);
            return e === u.UTF8_BYTES ? i : this.text_decoder_.decode(i);
          }
          __union_with_string(t, e) {
            return "string" == typeof t ? this.__string(e) : this.__union(t, e);
          }
          __indirect(t) {
            return t + this.readInt32(t);
          }
          __vector(t) {
            return t + this.readInt32(t) + r;
          }
          __vector_len(t) {
            return this.readInt32(t + this.readInt32(t));
          }
          __has_identifier(t) {
            if (t.length != s)
              throw new Error(
                "FlatBuffers: file identifier must be length " + s
              );
            for (let e = 0; e < s; e++)
              if (t.charCodeAt(e) != this.readInt8(this.position() + r + e))
                return !1;
            return !0;
          }
          createScalarList(t, e) {
            const n = [];
            for (let i = 0; i < e; ++i) {
              const e = t(i);
              null !== e && n.push(e);
            }
            return n;
          }
          createObjList(t, e) {
            const n = [];
            for (let i = 0; i < e; ++i) {
              const e = t(i);
              null !== e && n.push(e.unpack());
            }
            return n;
          }
        }
        class b {
          constructor(t) {
            let e;
            (this.minalign = 1),
              (this.vtable = null),
              (this.vtable_in_use = 0),
              (this.isNested = !1),
              (this.object_start = 0),
              (this.vtables = []),
              (this.vector_num_elems = 0),
              (this.force_defaults = !1),
              (this.string_maps = null),
              (this.text_encoder = new TextEncoder()),
              (e = t || 1024),
              (this.bb = l.allocate(e)),
              (this.space = e);
          }
          clear() {
            this.bb.clear(),
              (this.space = this.bb.capacity()),
              (this.minalign = 1),
              (this.vtable = null),
              (this.vtable_in_use = 0),
              (this.isNested = !1),
              (this.object_start = 0),
              (this.vtables = []),
              (this.vector_num_elems = 0),
              (this.force_defaults = !1),
              (this.string_maps = null);
          }
          forceDefaults(t) {
            this.force_defaults = t;
          }
          dataBuffer() {
            return this.bb;
          }
          asUint8Array() {
            return this.bb
              .bytes()
              .subarray(this.bb.position(), this.bb.position() + this.offset());
          }
          prep(t, e) {
            t > this.minalign && (this.minalign = t);
            const n = (1 + ~(this.bb.capacity() - this.space + e)) & (t - 1);
            for (; this.space < n + t + e; ) {
              const t = this.bb.capacity();
              (this.bb = b.growByteBuffer(this.bb)),
                (this.space += this.bb.capacity() - t);
            }
            this.pad(n);
          }
          pad(t) {
            for (let e = 0; e < t; e++) this.bb.writeInt8(--this.space, 0);
          }
          writeInt8(t) {
            this.bb.writeInt8((this.space -= 1), t);
          }
          writeInt16(t) {
            this.bb.writeInt16((this.space -= 2), t);
          }
          writeInt32(t) {
            this.bb.writeInt32((this.space -= 4), t);
          }
          writeInt64(t) {
            this.bb.writeInt64((this.space -= 8), t);
          }
          writeFloat32(t) {
            this.bb.writeFloat32((this.space -= 4), t);
          }
          writeFloat64(t) {
            this.bb.writeFloat64((this.space -= 8), t);
          }
          addInt8(t) {
            this.prep(1, 0), this.writeInt8(t);
          }
          addInt16(t) {
            this.prep(2, 0), this.writeInt16(t);
          }
          addInt32(t) {
            this.prep(4, 0), this.writeInt32(t);
          }
          addInt64(t) {
            this.prep(8, 0), this.writeInt64(t);
          }
          addFloat32(t) {
            this.prep(4, 0), this.writeFloat32(t);
          }
          addFloat64(t) {
            this.prep(8, 0), this.writeFloat64(t);
          }
          addFieldInt8(t, e, n) {
            (this.force_defaults || e != n) && (this.addInt8(e), this.slot(t));
          }
          addFieldInt16(t, e, n) {
            (this.force_defaults || e != n) && (this.addInt16(e), this.slot(t));
          }
          addFieldInt32(t, e, n) {
            (this.force_defaults || e != n) && (this.addInt32(e), this.slot(t));
          }
          addFieldInt64(t, e, n) {
            (this.force_defaults || e !== n) &&
              (this.addInt64(e), this.slot(t));
          }
          addFieldFloat32(t, e, n) {
            (this.force_defaults || e != n) &&
              (this.addFloat32(e), this.slot(t));
          }
          addFieldFloat64(t, e, n) {
            (this.force_defaults || e != n) &&
              (this.addFloat64(e), this.slot(t));
          }
          addFieldOffset(t, e, n) {
            (this.force_defaults || e != n) &&
              (this.addOffset(e), this.slot(t));
          }
          addFieldStruct(t, e, n) {
            e != n && (this.nested(e), this.slot(t));
          }
          nested(t) {
            if (t != this.offset())
              throw new TypeError(
                "FlatBuffers: struct must be serialized inline."
              );
          }
          notNested() {
            if (this.isNested)
              throw new TypeError(
                "FlatBuffers: object serialization must not be nested."
              );
          }
          slot(t) {
            null !== this.vtable && (this.vtable[t] = this.offset());
          }
          offset() {
            return this.bb.capacity() - this.space;
          }
          static growByteBuffer(t) {
            const e = t.capacity();
            if (3221225472 & e)
              throw new Error(
                "FlatBuffers: cannot grow buffer beyond 2 gigabytes."
              );
            const n = e << 1,
              i = l.allocate(n);
            return i.setPosition(n - e), i.bytes().set(t.bytes(), n - e), i;
          }
          addOffset(t) {
            this.prep(r, 0), this.writeInt32(this.offset() - t + r);
          }
          startObject(t) {
            this.notNested(),
              null == this.vtable && (this.vtable = []),
              (this.vtable_in_use = t);
            for (let e = 0; e < t; e++) this.vtable[e] = 0;
            (this.isNested = !0), (this.object_start = this.offset());
          }
          endObject() {
            if (null == this.vtable || !this.isNested)
              throw new Error(
                "FlatBuffers: endObject called without startObject"
              );
            this.addInt32(0);
            const t = this.offset();
            let e = this.vtable_in_use - 1;
            for (; e >= 0 && 0 == this.vtable[e]; e--);
            const n = e + 1;
            for (; e >= 0; e--)
              this.addInt16(0 != this.vtable[e] ? t - this.vtable[e] : 0);
            this.addInt16(t - this.object_start);
            const r = (n + 2) * i;
            this.addInt16(r);
            let s = 0;
            const o = this.space;
            t: for (e = 0; e < this.vtables.length; e++) {
              const t = this.bb.capacity() - this.vtables[e];
              if (r == this.bb.readInt16(t)) {
                for (let e = i; e < r; e += i)
                  if (this.bb.readInt16(o + e) != this.bb.readInt16(t + e))
                    continue t;
                s = this.vtables[e];
                break;
              }
            }
            return (
              s
                ? ((this.space = this.bb.capacity() - t),
                  this.bb.writeInt32(this.space, s - t))
                : (this.vtables.push(this.offset()),
                  this.bb.writeInt32(
                    this.bb.capacity() - t,
                    this.offset() - t
                  )),
              (this.isNested = !1),
              t
            );
          }
          finish(t, e, n) {
            const i = n ? o : 0;
            if (e) {
              const t = e;
              if ((this.prep(this.minalign, r + s + i), t.length != s))
                throw new TypeError(
                  "FlatBuffers: file identifier must be length " + s
                );
              for (let e = s - 1; e >= 0; e--) this.writeInt8(t.charCodeAt(e));
            }
            this.prep(this.minalign, r + i),
              this.addOffset(t),
              i && this.addInt32(this.bb.capacity() - this.space),
              this.bb.setPosition(this.space);
          }
          finishSizePrefixed(t, e) {
            this.finish(t, e, !0);
          }
          requiredField(t, e) {
            const n = this.bb.capacity() - t,
              i = n - this.bb.readInt32(n);
            if (!(e < this.bb.readInt16(i) && 0 != this.bb.readInt16(i + e)))
              throw new TypeError("FlatBuffers: field " + e + " must be set");
          }
          startVector(t, e, n) {
            this.notNested(),
              (this.vector_num_elems = e),
              this.prep(r, t * e),
              this.prep(n, t * e);
          }
          endVector() {
            return this.writeInt32(this.vector_num_elems), this.offset();
          }
          createSharedString(t) {
            if (!t) return 0;
            if (
              (this.string_maps || (this.string_maps = new Map()),
              this.string_maps.has(t))
            )
              return this.string_maps.get(t);
            const e = this.createString(t);
            return this.string_maps.set(t, e), e;
          }
          createString(t) {
            if (null == t) return 0;
            let e;
            (e = t instanceof Uint8Array ? t : this.text_encoder.encode(t)),
              this.addInt8(0),
              this.startVector(1, e.length, 1),
              this.bb.setPosition((this.space -= e.length));
            for (
              let t = 0, n = this.space, i = this.bb.bytes();
              t < e.length;
              t++
            )
              i[n++] = e[t];
            return this.endVector();
          }
          createObjectOffset(t) {
            return null === t
              ? 0
              : "string" == typeof t
              ? this.createString(t)
              : t.pack(this);
          }
          createObjectOffsetList(t) {
            const e = [];
            for (let n = 0; n < t.length; ++n) {
              const i = t[n];
              if (null === i)
                throw new TypeError(
                  "FlatBuffers: Argument for createObjectOffsetList cannot contain null."
                );
              e.push(this.createObjectOffset(i));
            }
            return e;
          }
          createStructOffsetList(t, e) {
            return (
              e(this, t.length),
              this.createObjectOffsetList(t.slice().reverse()),
              this.endVector()
            );
          }
        }
      },
      752: function (t, e, n) {
        var i =
            (this && this.__createBinding) ||
            (Object.create
              ? function (t, e, n, i) {
                  void 0 === i && (i = n);
                  var r = Object.getOwnPropertyDescriptor(e, n);
                  (r &&
                    !("get" in r
                      ? !e.__esModule
                      : r.writable || r.configurable)) ||
                    (r = {
                      enumerable: !0,
                      get: function () {
                        return e[n];
                      },
                    }),
                    Object.defineProperty(t, i, r);
                }
              : function (t, e, n, i) {
                  void 0 === i && (i = n), (t[i] = e[n]);
                }),
          r =
            (this && this.__setModuleDefault) ||
            (Object.create
              ? function (t, e) {
                  Object.defineProperty(t, "default", {
                    enumerable: !0,
                    value: e,
                  });
                }
              : function (t, e) {
                  t.default = e;
                }),
          s =
            (this && this.__importStar) ||
            function (t) {
              if (t && t.__esModule) return t;
              var e = {};
              if (null != t)
                for (var n in t)
                  "default" !== n &&
                    Object.prototype.hasOwnProperty.call(t, n) &&
                    i(e, t, n);
              return r(e, t), e;
            };
        Object.defineProperty(e, "__esModule", { value: !0 });
        const o = s(n(251)),
          a = s(n(210)),
          c = n(960);
        new WebSocket("ws://127.0.0.1:55010/pull-stats/").onmessage = function (
          t
        ) {
          const e = new o.ByteBuffer(new Uint8Array(t.data)),
            n = a.PeerChange.getRootAsPeerChange(e);
          (0, c.handleChange)(n);
        };
      },
      210: (t, e, n) => {
        Object.defineProperty(e, "__esModule", { value: !0 }),
          (e.Response =
            e.RemovedConnectionT =
            e.RemovedConnection =
            e.PeerChangeType =
            e.PeerChangeT =
            e.PeerChange =
            e.OkT =
            e.Ok =
            e.ErrorT =
            e.Error =
            e.ControllerResponseT =
            e.ControllerResponse =
            e.AddedConnectionT =
            e.AddedConnection =
              void 0);
        var i = n(54);
        Object.defineProperty(e, "AddedConnection", {
          enumerable: !0,
          get: function () {
            return i.AddedConnection;
          },
        }),
          Object.defineProperty(e, "AddedConnectionT", {
            enumerable: !0,
            get: function () {
              return i.AddedConnectionT;
            },
          });
        var r = n(997);
        Object.defineProperty(e, "ControllerResponse", {
          enumerable: !0,
          get: function () {
            return r.ControllerResponse;
          },
        }),
          Object.defineProperty(e, "ControllerResponseT", {
            enumerable: !0,
            get: function () {
              return r.ControllerResponseT;
            },
          });
        var s = n(275);
        Object.defineProperty(e, "Error", {
          enumerable: !0,
          get: function () {
            return s.Error;
          },
        }),
          Object.defineProperty(e, "ErrorT", {
            enumerable: !0,
            get: function () {
              return s.ErrorT;
            },
          });
        var o = n(798);
        Object.defineProperty(e, "Ok", {
          enumerable: !0,
          get: function () {
            return o.Ok;
          },
        }),
          Object.defineProperty(e, "OkT", {
            enumerable: !0,
            get: function () {
              return o.OkT;
            },
          });
        var a = n(950);
        Object.defineProperty(e, "PeerChange", {
          enumerable: !0,
          get: function () {
            return a.PeerChange;
          },
        }),
          Object.defineProperty(e, "PeerChangeT", {
            enumerable: !0,
            get: function () {
              return a.PeerChangeT;
            },
          });
        var c = n(934);
        Object.defineProperty(e, "PeerChangeType", {
          enumerable: !0,
          get: function () {
            return c.PeerChangeType;
          },
        });
        var d = n(727);
        Object.defineProperty(e, "RemovedConnection", {
          enumerable: !0,
          get: function () {
            return d.RemovedConnection;
          },
        }),
          Object.defineProperty(e, "RemovedConnectionT", {
            enumerable: !0,
            get: function () {
              return d.RemovedConnectionT;
            },
          });
        var h = n(234);
        Object.defineProperty(e, "Response", {
          enumerable: !0,
          get: function () {
            return h.Response;
          },
        });
      },
      54: function (t, e, n) {
        var i =
            (this && this.__createBinding) ||
            (Object.create
              ? function (t, e, n, i) {
                  void 0 === i && (i = n);
                  var r = Object.getOwnPropertyDescriptor(e, n);
                  (r &&
                    !("get" in r
                      ? !e.__esModule
                      : r.writable || r.configurable)) ||
                    (r = {
                      enumerable: !0,
                      get: function () {
                        return e[n];
                      },
                    }),
                    Object.defineProperty(t, i, r);
                }
              : function (t, e, n, i) {
                  void 0 === i && (i = n), (t[i] = e[n]);
                }),
          r =
            (this && this.__setModuleDefault) ||
            (Object.create
              ? function (t, e) {
                  Object.defineProperty(t, "default", {
                    enumerable: !0,
                    value: e,
                  });
                }
              : function (t, e) {
                  t.default = e;
                }),
          s =
            (this && this.__importStar) ||
            function (t) {
              if (t && t.__esModule) return t;
              var e = {};
              if (null != t)
                for (var n in t)
                  "default" !== n &&
                    Object.prototype.hasOwnProperty.call(t, n) &&
                    i(e, t, n);
              return r(e, t), e;
            };
        Object.defineProperty(e, "__esModule", { value: !0 }),
          (e.AddedConnectionT = e.AddedConnection = void 0);
        const o = s(n(251));
        class a {
          constructor() {
            (this.bb = null), (this.bb_pos = 0);
          }
          __init(t, e) {
            return (this.bb_pos = t), (this.bb = e), this;
          }
          static getRootAsAddedConnection(t, e) {
            return (e || new a()).__init(
              t.readInt32(t.position()) + t.position(),
              t
            );
          }
          static getSizePrefixedRootAsAddedConnection(t, e) {
            return (
              t.setPosition(t.position() + o.SIZE_PREFIX_LENGTH),
              (e || new a()).__init(t.readInt32(t.position()) + t.position(), t)
            );
          }
          from(t) {
            const e = this.bb.__offset(this.bb_pos, 4);
            return e ? this.bb.__string(this.bb_pos + e, t) : null;
          }
          fromLocation() {
            const t = this.bb.__offset(this.bb_pos, 6);
            return t ? this.bb.readFloat64(this.bb_pos + t) : 0;
          }
          to(t) {
            const e = this.bb.__offset(this.bb_pos, 8);
            return e ? this.bb.__string(this.bb_pos + e, t) : null;
          }
          toLocation() {
            const t = this.bb.__offset(this.bb_pos, 10);
            return t ? this.bb.readFloat64(this.bb_pos + t) : 0;
          }
          static startAddedConnection(t) {
            t.startObject(4);
          }
          static addFrom(t, e) {
            t.addFieldOffset(0, e, 0);
          }
          static addFromLocation(t, e) {
            t.addFieldFloat64(1, e, 0);
          }
          static addTo(t, e) {
            t.addFieldOffset(2, e, 0);
          }
          static addToLocation(t, e) {
            t.addFieldFloat64(3, e, 0);
          }
          static endAddedConnection(t) {
            const e = t.endObject();
            return t.requiredField(e, 4), t.requiredField(e, 8), e;
          }
          static createAddedConnection(t, e, n, i, r) {
            return (
              a.startAddedConnection(t),
              a.addFrom(t, e),
              a.addFromLocation(t, n),
              a.addTo(t, i),
              a.addToLocation(t, r),
              a.endAddedConnection(t)
            );
          }
          unpack() {
            return new c(
              this.from(),
              this.fromLocation(),
              this.to(),
              this.toLocation()
            );
          }
          unpackTo(t) {
            (t.from = this.from()),
              (t.fromLocation = this.fromLocation()),
              (t.to = this.to()),
              (t.toLocation = this.toLocation());
          }
        }
        e.AddedConnection = a;
        class c {
          constructor(t = null, e = 0, n = null, i = 0) {
            (this.from = t),
              (this.fromLocation = e),
              (this.to = n),
              (this.toLocation = i);
          }
          pack(t) {
            const e = null !== this.from ? t.createString(this.from) : 0,
              n = null !== this.to ? t.createString(this.to) : 0;
            return a.createAddedConnection(
              t,
              e,
              this.fromLocation,
              n,
              this.toLocation
            );
          }
        }
        e.AddedConnectionT = c;
      },
      997: function (t, e, n) {
        var i =
            (this && this.__createBinding) ||
            (Object.create
              ? function (t, e, n, i) {
                  void 0 === i && (i = n);
                  var r = Object.getOwnPropertyDescriptor(e, n);
                  (r &&
                    !("get" in r
                      ? !e.__esModule
                      : r.writable || r.configurable)) ||
                    (r = {
                      enumerable: !0,
                      get: function () {
                        return e[n];
                      },
                    }),
                    Object.defineProperty(t, i, r);
                }
              : function (t, e, n, i) {
                  void 0 === i && (i = n), (t[i] = e[n]);
                }),
          r =
            (this && this.__setModuleDefault) ||
            (Object.create
              ? function (t, e) {
                  Object.defineProperty(t, "default", {
                    enumerable: !0,
                    value: e,
                  });
                }
              : function (t, e) {
                  t.default = e;
                }),
          s =
            (this && this.__importStar) ||
            function (t) {
              if (t && t.__esModule) return t;
              var e = {};
              if (null != t)
                for (var n in t)
                  "default" !== n &&
                    Object.prototype.hasOwnProperty.call(t, n) &&
                    i(e, t, n);
              return r(e, t), e;
            };
        Object.defineProperty(e, "__esModule", { value: !0 }),
          (e.ControllerResponseT = e.ControllerResponse = void 0);
        const o = s(n(251)),
          a = n(234);
        class c {
          constructor() {
            (this.bb = null), (this.bb_pos = 0);
          }
          __init(t, e) {
            return (this.bb_pos = t), (this.bb = e), this;
          }
          static getRootAsControllerResponse(t, e) {
            return (e || new c()).__init(
              t.readInt32(t.position()) + t.position(),
              t
            );
          }
          static getSizePrefixedRootAsControllerResponse(t, e) {
            return (
              t.setPosition(t.position() + o.SIZE_PREFIX_LENGTH),
              (e || new c()).__init(t.readInt32(t.position()) + t.position(), t)
            );
          }
          responseType() {
            const t = this.bb.__offset(this.bb_pos, 4);
            return t ? this.bb.readUint8(this.bb_pos + t) : a.Response.NONE;
          }
          response(t) {
            const e = this.bb.__offset(this.bb_pos, 6);
            return e ? this.bb.__union(t, this.bb_pos + e) : null;
          }
          static startControllerResponse(t) {
            t.startObject(2);
          }
          static addResponseType(t, e) {
            t.addFieldInt8(0, e, a.Response.NONE);
          }
          static addResponse(t, e) {
            t.addFieldOffset(1, e, 0);
          }
          static endControllerResponse(t) {
            const e = t.endObject();
            return t.requiredField(e, 6), e;
          }
          static createControllerResponse(t, e, n) {
            return (
              c.startControllerResponse(t),
              c.addResponseType(t, e),
              c.addResponse(t, n),
              c.endControllerResponse(t)
            );
          }
          unpack() {
            return new d(
              this.responseType(),
              (() => {
                const t = (0, a.unionToResponse)(
                  this.responseType(),
                  this.response.bind(this)
                );
                return null === t ? null : t.unpack();
              })()
            );
          }
          unpackTo(t) {
            (t.responseType = this.responseType()),
              (t.response = (() => {
                const t = (0, a.unionToResponse)(
                  this.responseType(),
                  this.response.bind(this)
                );
                return null === t ? null : t.unpack();
              })());
          }
        }
        e.ControllerResponse = c;
        class d {
          constructor(t = a.Response.NONE, e = null) {
            (this.responseType = t), (this.response = e);
          }
          pack(t) {
            const e = t.createObjectOffset(this.response);
            return c.createControllerResponse(t, this.responseType, e);
          }
        }
        e.ControllerResponseT = d;
      },
      275: function (t, e, n) {
        var i =
            (this && this.__createBinding) ||
            (Object.create
              ? function (t, e, n, i) {
                  void 0 === i && (i = n);
                  var r = Object.getOwnPropertyDescriptor(e, n);
                  (r &&
                    !("get" in r
                      ? !e.__esModule
                      : r.writable || r.configurable)) ||
                    (r = {
                      enumerable: !0,
                      get: function () {
                        return e[n];
                      },
                    }),
                    Object.defineProperty(t, i, r);
                }
              : function (t, e, n, i) {
                  void 0 === i && (i = n), (t[i] = e[n]);
                }),
          r =
            (this && this.__setModuleDefault) ||
            (Object.create
              ? function (t, e) {
                  Object.defineProperty(t, "default", {
                    enumerable: !0,
                    value: e,
                  });
                }
              : function (t, e) {
                  t.default = e;
                }),
          s =
            (this && this.__importStar) ||
            function (t) {
              if (t && t.__esModule) return t;
              var e = {};
              if (null != t)
                for (var n in t)
                  "default" !== n &&
                    Object.prototype.hasOwnProperty.call(t, n) &&
                    i(e, t, n);
              return r(e, t), e;
            };
        Object.defineProperty(e, "__esModule", { value: !0 }),
          (e.ErrorT = e.Error = void 0);
        const o = s(n(251));
        class a {
          constructor() {
            (this.bb = null), (this.bb_pos = 0);
          }
          __init(t, e) {
            return (this.bb_pos = t), (this.bb = e), this;
          }
          static getRootAsError(t, e) {
            return (e || new a()).__init(
              t.readInt32(t.position()) + t.position(),
              t
            );
          }
          static getSizePrefixedRootAsError(t, e) {
            return (
              t.setPosition(t.position() + o.SIZE_PREFIX_LENGTH),
              (e || new a()).__init(t.readInt32(t.position()) + t.position(), t)
            );
          }
          message(t) {
            const e = this.bb.__offset(this.bb_pos, 4);
            return e ? this.bb.__string(this.bb_pos + e, t) : null;
          }
          static startError(t) {
            t.startObject(1);
          }
          static addMessage(t, e) {
            t.addFieldOffset(0, e, 0);
          }
          static endError(t) {
            const e = t.endObject();
            return t.requiredField(e, 4), e;
          }
          static createError(t, e) {
            return a.startError(t), a.addMessage(t, e), a.endError(t);
          }
          unpack() {
            return new c(this.message());
          }
          unpackTo(t) {
            t.message = this.message();
          }
        }
        e.Error = a;
        class c {
          constructor(t = null) {
            this.message = t;
          }
          pack(t) {
            const e = null !== this.message ? t.createString(this.message) : 0;
            return a.createError(t, e);
          }
        }
        e.ErrorT = c;
      },
      798: function (t, e, n) {
        var i =
            (this && this.__createBinding) ||
            (Object.create
              ? function (t, e, n, i) {
                  void 0 === i && (i = n);
                  var r = Object.getOwnPropertyDescriptor(e, n);
                  (r &&
                    !("get" in r
                      ? !e.__esModule
                      : r.writable || r.configurable)) ||
                    (r = {
                      enumerable: !0,
                      get: function () {
                        return e[n];
                      },
                    }),
                    Object.defineProperty(t, i, r);
                }
              : function (t, e, n, i) {
                  void 0 === i && (i = n), (t[i] = e[n]);
                }),
          r =
            (this && this.__setModuleDefault) ||
            (Object.create
              ? function (t, e) {
                  Object.defineProperty(t, "default", {
                    enumerable: !0,
                    value: e,
                  });
                }
              : function (t, e) {
                  t.default = e;
                }),
          s =
            (this && this.__importStar) ||
            function (t) {
              if (t && t.__esModule) return t;
              var e = {};
              if (null != t)
                for (var n in t)
                  "default" !== n &&
                    Object.prototype.hasOwnProperty.call(t, n) &&
                    i(e, t, n);
              return r(e, t), e;
            };
        Object.defineProperty(e, "__esModule", { value: !0 }),
          (e.OkT = e.Ok = void 0);
        const o = s(n(251));
        class a {
          constructor() {
            (this.bb = null), (this.bb_pos = 0);
          }
          __init(t, e) {
            return (this.bb_pos = t), (this.bb = e), this;
          }
          static getRootAsOk(t, e) {
            return (e || new a()).__init(
              t.readInt32(t.position()) + t.position(),
              t
            );
          }
          static getSizePrefixedRootAsOk(t, e) {
            return (
              t.setPosition(t.position() + o.SIZE_PREFIX_LENGTH),
              (e || new a()).__init(t.readInt32(t.position()) + t.position(), t)
            );
          }
          message(t) {
            const e = this.bb.__offset(this.bb_pos, 4);
            return e ? this.bb.__string(this.bb_pos + e, t) : null;
          }
          static startOk(t) {
            t.startObject(1);
          }
          static addMessage(t, e) {
            t.addFieldOffset(0, e, 0);
          }
          static endOk(t) {
            return t.endObject();
          }
          static createOk(t, e) {
            return a.startOk(t), a.addMessage(t, e), a.endOk(t);
          }
          unpack() {
            return new c(this.message());
          }
          unpackTo(t) {
            t.message = this.message();
          }
        }
        e.Ok = a;
        class c {
          constructor(t = null) {
            this.message = t;
          }
          pack(t) {
            const e = null !== this.message ? t.createString(this.message) : 0;
            return a.createOk(t, e);
          }
        }
        e.OkT = c;
      },
      934: (t, e, n) => {
        Object.defineProperty(e, "__esModule", { value: !0 }),
          (e.unionListToPeerChangeType =
            e.unionToPeerChangeType =
            e.PeerChangeType =
              void 0);
        const i = n(54),
          r = n(275),
          s = n(727);
        var o;
        !(function (t) {
          (t[(t.NONE = 0)] = "NONE"),
            (t[(t.AddedConnection = 1)] = "AddedConnection"),
            (t[(t.RemovedConnection = 2)] = "RemovedConnection"),
            (t[(t.Error = 3)] = "Error");
        })(o || (e.PeerChangeType = o = {})),
          (e.unionToPeerChangeType = function (t, e) {
            switch (o[t]) {
              case "NONE":
              default:
                return null;
              case "AddedConnection":
                return e(new i.AddedConnection());
              case "RemovedConnection":
                return e(new s.RemovedConnection());
              case "Error":
                return e(new r.Error());
            }
          }),
          (e.unionListToPeerChangeType = function (t, e, n) {
            switch (o[t]) {
              case "NONE":
              default:
                return null;
              case "AddedConnection":
                return e(n, new i.AddedConnection());
              case "RemovedConnection":
                return e(n, new s.RemovedConnection());
              case "Error":
                return e(n, new r.Error());
            }
          });
      },
      950: function (t, e, n) {
        var i =
            (this && this.__createBinding) ||
            (Object.create
              ? function (t, e, n, i) {
                  void 0 === i && (i = n);
                  var r = Object.getOwnPropertyDescriptor(e, n);
                  (r &&
                    !("get" in r
                      ? !e.__esModule
                      : r.writable || r.configurable)) ||
                    (r = {
                      enumerable: !0,
                      get: function () {
                        return e[n];
                      },
                    }),
                    Object.defineProperty(t, i, r);
                }
              : function (t, e, n, i) {
                  void 0 === i && (i = n), (t[i] = e[n]);
                }),
          r =
            (this && this.__setModuleDefault) ||
            (Object.create
              ? function (t, e) {
                  Object.defineProperty(t, "default", {
                    enumerable: !0,
                    value: e,
                  });
                }
              : function (t, e) {
                  t.default = e;
                }),
          s =
            (this && this.__importStar) ||
            function (t) {
              if (t && t.__esModule) return t;
              var e = {};
              if (null != t)
                for (var n in t)
                  "default" !== n &&
                    Object.prototype.hasOwnProperty.call(t, n) &&
                    i(e, t, n);
              return r(e, t), e;
            };
        Object.defineProperty(e, "__esModule", { value: !0 }),
          (e.PeerChangeT = e.PeerChange = void 0);
        const o = s(n(251)),
          a = n(54),
          c = n(934);
        class d {
          constructor() {
            (this.bb = null), (this.bb_pos = 0);
          }
          __init(t, e) {
            return (this.bb_pos = t), (this.bb = e), this;
          }
          static getRootAsPeerChange(t, e) {
            return (e || new d()).__init(
              t.readInt32(t.position()) + t.position(),
              t
            );
          }
          static getSizePrefixedRootAsPeerChange(t, e) {
            return (
              t.setPosition(t.position() + o.SIZE_PREFIX_LENGTH),
              (e || new d()).__init(t.readInt32(t.position()) + t.position(), t)
            );
          }
          currentState(t, e) {
            const n = this.bb.__offset(this.bb_pos, 4);
            return n
              ? (e || new a.AddedConnection()).__init(
                  this.bb.__indirect(this.bb.__vector(this.bb_pos + n) + 4 * t),
                  this.bb
                )
              : null;
          }
          currentStateLength() {
            const t = this.bb.__offset(this.bb_pos, 4);
            return t ? this.bb.__vector_len(this.bb_pos + t) : 0;
          }
          changeType() {
            const t = this.bb.__offset(this.bb_pos, 6);
            return t
              ? this.bb.readUint8(this.bb_pos + t)
              : c.PeerChangeType.NONE;
          }
          change(t) {
            const e = this.bb.__offset(this.bb_pos, 8);
            return e ? this.bb.__union(t, this.bb_pos + e) : null;
          }
          static startPeerChange(t) {
            t.startObject(3);
          }
          static addCurrentState(t, e) {
            t.addFieldOffset(0, e, 0);
          }
          static createCurrentStateVector(t, e) {
            t.startVector(4, e.length, 4);
            for (let n = e.length - 1; n >= 0; n--) t.addOffset(e[n]);
            return t.endVector();
          }
          static startCurrentStateVector(t, e) {
            t.startVector(4, e, 4);
          }
          static addChangeType(t, e) {
            t.addFieldInt8(1, e, c.PeerChangeType.NONE);
          }
          static addChange(t, e) {
            t.addFieldOffset(2, e, 0);
          }
          static endPeerChange(t) {
            return t.endObject();
          }
          static createPeerChange(t, e, n, i) {
            return (
              d.startPeerChange(t),
              d.addCurrentState(t, e),
              d.addChangeType(t, n),
              d.addChange(t, i),
              d.endPeerChange(t)
            );
          }
          unpack() {
            return new h(
              this.bb.createObjList(
                this.currentState.bind(this),
                this.currentStateLength()
              ),
              this.changeType(),
              (() => {
                const t = (0, c.unionToPeerChangeType)(
                  this.changeType(),
                  this.change.bind(this)
                );
                return null === t ? null : t.unpack();
              })()
            );
          }
          unpackTo(t) {
            (t.currentState = this.bb.createObjList(
              this.currentState.bind(this),
              this.currentStateLength()
            )),
              (t.changeType = this.changeType()),
              (t.change = (() => {
                const t = (0, c.unionToPeerChangeType)(
                  this.changeType(),
                  this.change.bind(this)
                );
                return null === t ? null : t.unpack();
              })());
          }
        }
        e.PeerChange = d;
        class h {
          constructor(t = [], e = c.PeerChangeType.NONE, n = null) {
            (this.currentState = t), (this.changeType = e), (this.change = n);
          }
          pack(t) {
            const e = d.createCurrentStateVector(
                t,
                t.createObjectOffsetList(this.currentState)
              ),
              n = t.createObjectOffset(this.change);
            return d.createPeerChange(t, e, this.changeType, n);
          }
        }
        e.PeerChangeT = h;
      },
      727: function (t, e, n) {
        var i =
            (this && this.__createBinding) ||
            (Object.create
              ? function (t, e, n, i) {
                  void 0 === i && (i = n);
                  var r = Object.getOwnPropertyDescriptor(e, n);
                  (r &&
                    !("get" in r
                      ? !e.__esModule
                      : r.writable || r.configurable)) ||
                    (r = {
                      enumerable: !0,
                      get: function () {
                        return e[n];
                      },
                    }),
                    Object.defineProperty(t, i, r);
                }
              : function (t, e, n, i) {
                  void 0 === i && (i = n), (t[i] = e[n]);
                }),
          r =
            (this && this.__setModuleDefault) ||
            (Object.create
              ? function (t, e) {
                  Object.defineProperty(t, "default", {
                    enumerable: !0,
                    value: e,
                  });
                }
              : function (t, e) {
                  t.default = e;
                }),
          s =
            (this && this.__importStar) ||
            function (t) {
              if (t && t.__esModule) return t;
              var e = {};
              if (null != t)
                for (var n in t)
                  "default" !== n &&
                    Object.prototype.hasOwnProperty.call(t, n) &&
                    i(e, t, n);
              return r(e, t), e;
            };
        Object.defineProperty(e, "__esModule", { value: !0 }),
          (e.RemovedConnectionT = e.RemovedConnection = void 0);
        const o = s(n(251));
        class a {
          constructor() {
            (this.bb = null), (this.bb_pos = 0);
          }
          __init(t, e) {
            return (this.bb_pos = t), (this.bb = e), this;
          }
          static getRootAsRemovedConnection(t, e) {
            return (e || new a()).__init(
              t.readInt32(t.position()) + t.position(),
              t
            );
          }
          static getSizePrefixedRootAsRemovedConnection(t, e) {
            return (
              t.setPosition(t.position() + o.SIZE_PREFIX_LENGTH),
              (e || new a()).__init(t.readInt32(t.position()) + t.position(), t)
            );
          }
          at(t) {
            const e = this.bb.__offset(this.bb_pos, 4);
            return e ? this.bb.__string(this.bb_pos + e, t) : null;
          }
          from(t) {
            const e = this.bb.__offset(this.bb_pos, 6);
            return e ? this.bb.__string(this.bb_pos + e, t) : null;
          }
          static startRemovedConnection(t) {
            t.startObject(2);
          }
          static addAt(t, e) {
            t.addFieldOffset(0, e, 0);
          }
          static addFrom(t, e) {
            t.addFieldOffset(1, e, 0);
          }
          static endRemovedConnection(t) {
            const e = t.endObject();
            return t.requiredField(e, 4), t.requiredField(e, 6), e;
          }
          static createRemovedConnection(t, e, n) {
            return (
              a.startRemovedConnection(t),
              a.addAt(t, e),
              a.addFrom(t, n),
              a.endRemovedConnection(t)
            );
          }
          unpack() {
            return new c(this.at(), this.from());
          }
          unpackTo(t) {
            (t.at = this.at()), (t.from = this.from());
          }
        }
        e.RemovedConnection = a;
        class c {
          constructor(t = null, e = null) {
            (this.at = t), (this.from = e);
          }
          pack(t) {
            const e = null !== this.at ? t.createString(this.at) : 0,
              n = null !== this.from ? t.createString(this.from) : 0;
            return a.createRemovedConnection(t, e, n);
          }
        }
        e.RemovedConnectionT = c;
      },
      234: (t, e, n) => {
        Object.defineProperty(e, "__esModule", { value: !0 }),
          (e.unionListToResponse = e.unionToResponse = e.Response = void 0);
        const i = n(275),
          r = n(798);
        var s;
        !(function (t) {
          (t[(t.NONE = 0)] = "NONE"),
            (t[(t.Error = 1)] = "Error"),
            (t[(t.Ok = 2)] = "Ok");
        })(s || (e.Response = s = {})),
          (e.unionToResponse = function (t, e) {
            switch (s[t]) {
              case "NONE":
              default:
                return null;
              case "Error":
                return e(new i.Error());
              case "Ok":
                return e(new r.Ok());
            }
          }),
          (e.unionListToResponse = function (t, e, n) {
            switch (s[t]) {
              case "NONE":
              default:
                return null;
              case "Error":
                return e(n, new i.Error());
              case "Ok":
                return e(n, new r.Ok());
            }
          });
      },
      960: function (t, e, n) {
        var i =
            (this && this.__createBinding) ||
            (Object.create
              ? function (t, e, n, i) {
                  void 0 === i && (i = n);
                  var r = Object.getOwnPropertyDescriptor(e, n);
                  (r &&
                    !("get" in r
                      ? !e.__esModule
                      : r.writable || r.configurable)) ||
                    (r = {
                      enumerable: !0,
                      get: function () {
                        return e[n];
                      },
                    }),
                    Object.defineProperty(t, i, r);
                }
              : function (t, e, n, i) {
                  void 0 === i && (i = n), (t[i] = e[n]);
                }),
          r =
            (this && this.__setModuleDefault) ||
            (Object.create
              ? function (t, e) {
                  Object.defineProperty(t, "default", {
                    enumerable: !0,
                    value: e,
                  });
                }
              : function (t, e) {
                  t.default = e;
                }),
          s =
            (this && this.__importStar) ||
            function (t) {
              if (t && t.__esModule) return t;
              var e = {};
              if (null != t)
                for (var n in t)
                  "default" !== n &&
                    Object.prototype.hasOwnProperty.call(t, n) &&
                    i(e, t, n);
              return r(e, t), e;
            };
        Object.defineProperty(e, "__esModule", { value: !0 }),
          (e.showConnections =
            e.handleRemovedConnection =
            e.handleAddedConnection =
            e.handleChange =
            e.peers =
              void 0);
        const o = s(n(210));
        function a(t) {
          const n = t,
            i = n.from.toString(),
            r = n.to.toString(),
            s = { id: i, location: n.fromLocation },
            o = { id: r, location: n.toLocation };
          e.peers[i]
            ? (e.peers[i].currentLocation !== n.fromLocation &&
                (e.peers[i].locationHistory.push({
                  location: e.peers[i].currentLocation,
                  timestamp: Date.now(),
                }),
                (e.peers[i].currentLocation = n.fromLocation)),
              e.peers[r].currentLocation !== n.toLocation &&
                (e.peers[r].locationHistory.push({
                  location: e.peers[r].currentLocation,
                  timestamp: Date.now(),
                }),
                (e.peers[r].currentLocation = n.toLocation)),
              e.peers[i].connections.push(o))
            : (e.peers[i] = {
                id: i,
                currentLocation: n.fromLocation,
                connections: [o],
                history: [],
                locationHistory: [],
              }),
            e.peers[r]
              ? e.peers[r].connections.push(s)
              : (e.peers[r] = {
                  id: r,
                  currentLocation: n.toLocation,
                  connections: [s],
                  history: [],
                  locationHistory: [],
                });
          const a = { type: "Added", from: s, to: o, timestamp: Date.now() };
          e.peers[i].history.push(a), e.peers[r].history.push(a);
        }
        function c(t) {
          const n = t,
            i = n.from.toString(),
            r = n.at.toString(),
            s = e.peers[i].connections.findIndex((t) => t.id === r);
          s > -1 && e.peers[i].connections.splice(s, 1);
          const o = e.peers[r].connections.findIndex((t) => t.id === i);
          o > -1 && e.peers[r].connections.splice(o, 1);
          const a = {
            type: "Removed",
            from: { id: i, location: e.peers[i].currentLocation },
            to: { id: r, location: e.peers[r].currentLocation },
            timestamp: Date.now(),
          };
          e.peers[i].history.push(a), e.peers[r].history.push(a);
        }
        function d(t) {
          var e, n;
          const i = t.id,
            r = null !== (e = t.connections) && void 0 !== e ? e : [],
            s = document.getElementById("overlay-div");
          if (s) {
            s.innerHTML = "";
            const e = document.createElement("section");
            e.classList.add("header-section");
            const o = document.createElement("h2");
            (o.textContent = `Peer ID: ${i}, Location: ${
              null !== (n = t.currentLocation) && void 0 !== n ? n : ""
            }`),
              e.appendChild(o),
              s.appendChild(e);
            const a = document.createElement("section");
            a.classList.add("table-section");
            const c = document.createElement("table");
            c.classList.add("peers-table");
            const d = document.createElement("tr"),
              h = document.createElement("th");
            h.textContent = "Peer ID";
            const u = document.createElement("th");
            (u.textContent = "Location"),
              d.appendChild(h),
              d.appendChild(u),
              c.appendChild(d),
              r.forEach((t) => {
                var e, n;
                const i = document.createElement("tr"),
                  r = document.createElement("td");
                r.textContent =
                  null !== (e = t.id.toString()) && void 0 !== e ? e : "";
                const s = document.createElement("td");
                (s.textContent =
                  null !== (n = t.location.toString()) && void 0 !== n
                    ? n
                    : ""),
                  i.appendChild(r),
                  i.appendChild(s),
                  c.appendChild(i);
              }),
              a.appendChild(c),
              s.appendChild(a);
          }
        }
        function h(t) {
          let e = '<table class="table is-fullwidth is-striped">';
          return (
            (e +=
              "<thead><tr><th>Timestamp</th><th>Type</th><th>From</th><th>To</th></tr></thead><tbody>"),
            (e += t.history
              .map(
                (t) =>
                  `<tr><td>${t.timestamp}</td><td>${t.type}</td><td>${t.from.id}</td><td>${t.to.id}</td></tr>`
              )
              .join("")),
            (e += "</tbody></table>"),
            e
          );
        }
        (e.peers = {}),
          (e.handleChange = function (t) {
            const n = t.unpack();
            try {
              switch (n.changeType) {
                case o.PeerChangeType.AddedConnection:
                  a(n.change);
                  break;
                case o.PeerChangeType.RemovedConnection:
                  c(n.change);
              }
            } catch (t) {
              console.error(t);
            }
            !(function () {
              const t = document.getElementById("peers-table");
              if (t) {
                t.innerHTML = "";
                for (const n in e.peers) {
                  const i = document.createElement("tr");
                  (i.onclick = () => d(e.peers[n])),
                    (i.onclick = () => h(e.peers[n]));
                  const r = document.createElement("td");
                  (r.textContent = n), i.appendChild(r), t.appendChild(i);
                }
              }
            })();
          }),
          (e.handleAddedConnection = a),
          (e.handleRemovedConnection = c),
          (e.showConnections = d);
      },
    },
    e = {};
  function n(i) {
    var r = e[i];
    if (void 0 !== r) return r.exports;
    var s = (e[i] = { exports: {} });
    return t[i].call(s.exports, s, s.exports, n), s.exports;
  }
  (n.d = (t, e) => {
    for (var i in e)
      n.o(e, i) &&
        !n.o(t, i) &&
        Object.defineProperty(t, i, { enumerable: !0, get: e[i] });
  }),
    (n.o = (t, e) => Object.prototype.hasOwnProperty.call(t, e)),
    (n.r = (t) => {
      "undefined" != typeof Symbol &&
        Symbol.toStringTag &&
        Object.defineProperty(t, Symbol.toStringTag, { value: "Module" }),
        Object.defineProperty(t, "__esModule", { value: !0 });
    }),
    n(752);
})();
//# sourceMappingURL=bundle.js.map
