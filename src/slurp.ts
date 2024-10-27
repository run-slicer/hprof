import { type Visitor, valueSize, Type } from "./";

export enum EntryType {
    INSTANCE,
    OBJ_ARRAY,
    PRIM_ARRAY,
}

export interface Entry {
    type: EntryType;
    id: bigint;
    name?: string;

    count: number;
    totalSize: number;
    largestSize: number;
}

export interface ArrayEntry extends Entry {
    type: EntryType.OBJ_ARRAY | EntryType.PRIM_ARRAY;
    elemCount: number;
}

export interface SlurpVisitor extends Visitor {
    idSize?: number;
    timestamp?: Date;
    entries?: Entry[];
}

interface ClassInfo {
    id: bigint;
    instSize: number;
    superClsId: bigint;
}

interface EntryProto {
    type: EntryType;
    id: bigint;

    count: number;
}

interface ArrayEntryProto extends EntryProto {
    type: EntryType.OBJ_ARRAY | EntryType.PRIM_ARRAY;
    elemCount: number;
    maxSize: number;
}

const primDescs = {
    [Type.BOOLEAN]: "Z",
    [Type.CHAR]: "C",
    [Type.FLOAT]: "F",
    [Type.DOUBLE]: "D",
    [Type.BYTE]: "B",
    [Type.SHORT]: "S",
    [Type.INT]: "I",
    [Type.LONG]: "J",
};

export const slurp = (): SlurpVisitor => {
    const strings = new Map<bigint, string>();
    const classNames = new Map<bigint, string>();

    const classes = new Map<bigint, ClassInfo>();
    const instances = new Map<bigint, EntryProto>();
    const objArrays = new Map<bigint, ArrayEntryProto>();
    const primArrays = new Map<number, ArrayEntryProto>();

    return {
        header(_header, idSize, timestamp) {
            this.idSize = idSize;
            this.timestamp = new Date(Number(timestamp));
        },
        record(_tag, _tsDelta, _length) {
            return {
                utf8(id, value) {
                    strings.set(id, value);
                },
                loadClass(_num, objId, _stackNum, nameId) {
                    const value = strings.get(nameId);
                    if (!value) {
                        return; // class doesn't have a valid name constant index?
                    }

                    classNames.set(objId, value);
                },
                heapDump(_segment) {
                    strings.clear();

                    return {
                        gcClassDump(
                            clsObjId,
                            _stackNum,
                            superObjId,
                            _loaderObjId,
                            _signerObjId,
                            _protDomainObjId,
                            _reserved1,
                            _reserved2,
                            instSize,
                            _pool,
                            _staticFields,
                            _instFields
                        ) {
                            classes.set(clsObjId, {
                                id: clsObjId,
                                instSize,
                                superClsId: superObjId,
                            });
                        },
                        gcInstanceDump(_objId, _stackNum, clsObjId, _fieldValues) {
                            let entry = instances.get(clsObjId);
                            if (!entry) {
                                entry = {
                                    type: EntryType.INSTANCE,
                                    id: clsObjId,
                                    count: 0,
                                };
                                instances.set(clsObjId, entry);
                            }

                            entry.count++;
                        },
                        gcObjArrayDump(_arrObjId, _stackNum, arrClsId, elems) {
                            let entry = objArrays.get(arrClsId);
                            if (!entry) {
                                entry = {
                                    type: EntryType.OBJ_ARRAY,
                                    id: arrClsId,
                                    count: 0,
                                    elemCount: 0,
                                    maxSize: 0,
                                };
                                objArrays.set(arrClsId, entry);
                            }

                            entry.count++;
                            entry.elemCount += elems.length;
                        },
                        gcPrimArrayDump(_arrObjId, _stackNum, elemType, elems) {
                            let entry = primArrays.get(elemType);
                            if (!entry) {
                                entry = {
                                    type: EntryType.PRIM_ARRAY,
                                    id: BigInt(elemType),
                                    count: 0,
                                    elemCount: 0,
                                    maxSize: 0,
                                };
                                primArrays.set(elemType, entry);
                            }

                            entry.count++;
                            entry.elemCount += elems.length;
                            entry.maxSize = Math.max(entry.maxSize, elems.length);
                        },
                    };
                },
            };
        },
        end() {
            strings.clear();

            // https://shipilev.net/blog/2014/heapdump-is-a-lie/
            // we can't know the object size for sure, so we at least make an educated estimate
            let objectHeader = this.idSize + 4;
            objectHeader += objectHeader % this.idSize; // alignment

            this.entries = [];
            for (const [id, inst] of instances) {
                const entry: Entry = {
                    ...inst,
                    name: classNames.get(id),
                    totalSize: -1,
                    largestSize: -1,
                };

                const cls = classes.get(id);
                if (cls) {
                    let size = objectHeader + cls.instSize;

                    let superClsId = cls.superClsId;
                    while (superClsId !== 0n) {
                        const superCls = classes.get(superClsId);
                        if (!superCls) {
                            break;
                        }

                        size += superCls.instSize;
                        superClsId = superCls.superClsId;
                    }

                    size += size % this.idSize; // alignment
                    entry.largestSize = size;
                    entry.totalSize = size * entry.count;
                }

                this.entries.push(entry);
            }
            instances.clear();
            classes.clear();

            const arrayHeader = this.idSize + 8; // already aligned for both 32- and 64-bit

            // not including the objects' sizes - only the reference sizes
            for (const [id, inst] of objArrays) {
                const allHeaders = arrayHeader * inst.count;
                const allRefs = this.idSize * inst.elemCount;

                this.entries.push({
                    ...inst,
                    name: classNames.get(id),
                    totalSize: allHeaders + allRefs,
                    largestSize: arrayHeader + this.idSize * inst.maxSize,
                });
            }
            objArrays.clear();
            classNames.clear();

            for (const [id, inst] of primArrays) {
                const value = valueSize(id, this.idSize);

                const allHeaders = arrayHeader * inst.count;
                const allValues = value * inst.elemCount;
                const allPadding = inst.count * 4; /* 4 bytes per array - estimate, this data is lost */

                let largestArr = arrayHeader + value * inst.maxSize;
                largestArr += largestArr % this.idSize; // alignment

                this.entries.push({
                    ...inst,
                    name: `[${primDescs[id]}`,
                    totalSize: allHeaders + allValues + allPadding,
                    largestSize: largestArr,
                });
            }
            primArrays.clear();
        },
    };
};
