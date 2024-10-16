import { type Visitor, valueSize, Type } from "./";

export enum SlurpEntryType {
    INSTANCE,
    OBJ_ARRAY,
    PRIM_ARRAY,
}

export interface SlurpEntry {
    type: SlurpEntryType;
    id: bigint;
    name?: string;

    count: number;
    totalSize: number;
    largestSize: number;
}

export interface SlurpVisitor extends Visitor {
    idSize?: number;
    timestamp?: Date;
    entries?: SlurpEntry[];
}

const update = (entry: SlurpEntry, size: number) => {
    entry.count++;
    entry.totalSize += size;
    entry.largestSize = Math.max(entry.largestSize, size);
};

export const slurp = (): SlurpVisitor => {
    const strings = new Map<bigint, string>();
    const classes = new Map<bigint, string>();
    const sizes = new Map<bigint, number>();

    const objEntries = new Map<bigint, SlurpEntry>();
    const primEntries = new Map<number, SlurpEntry>();

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

                    classes.set(objId, value);
                },
                heapDump(_segment) {
                    strings.clear(); // we don't need them anymore

                    return {
                        gcInstanceDump(objId, _stackNum, classObjId, fieldValues) {
                            sizes.set(objId, fieldValues.byteLength);

                            let entry = objEntries.get(classObjId);
                            if (!entry) {
                                entry = {
                                    type: SlurpEntryType.INSTANCE,
                                    id: classObjId,
                                    count: 0,
                                    largestSize: 0,
                                    totalSize: 0,
                                };
                                objEntries.set(classObjId, entry);
                            }
                            update(entry, fieldValues.byteLength);
                        },
                        gcObjArrayDump(arrObjId, _stackNum, arrClsId, elems) {
                            const size = elems.reduce((acc, v) => acc + (sizes.get(v) || 0), 0);
                            sizes.set(arrObjId, size);

                            let entry = objEntries.get(arrClsId);
                            if (!entry) {
                                entry = {
                                    type: SlurpEntryType.OBJ_ARRAY,
                                    id: arrClsId,
                                    count: 0,
                                    largestSize: 0,
                                    totalSize: 0,
                                };
                                objEntries.set(arrClsId, entry);
                            }
                            update(entry, size);
                        },
                        gcPrimArrayDump(arrObjId, _stackNum, elemType, elems) {
                            const size = valueSize(elemType, this.idSize) * elems.length;
                            sizes.set(arrObjId, size);

                            let entry = primEntries.get(elemType);
                            if (!entry) {
                                entry = {
                                    type: SlurpEntryType.PRIM_ARRAY,
                                    id: BigInt(elemType),
                                    count: 0,
                                    largestSize: 0,
                                    totalSize: 0,
                                };
                                primEntries.set(elemType, entry);
                            }
                            update(entry, size);
                        },
                    };
                },
            };
        },
        end() {
            strings.clear();
            sizes.clear();

            this.entries = [...Array.from(objEntries.values()), ...Array.from(primEntries.values())];
            objEntries.clear();
            primEntries.clear();

            for (const entry of this.entries) {
                entry.name =
                    entry.type === SlurpEntryType.PRIM_ARRAY
                        ? `${Type[entry.id].toLowerCase()}[]`
                        : classes.get(entry.id);
            }

            classes.clear();
        },
    };
};
