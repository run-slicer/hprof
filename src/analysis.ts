import { type Visitor, Type, valueSize } from "./";

export enum SlurpEntryType {
    INSTANCE,
    OBJ_ARRAY,
    PRIM_ARRAY,
}

export interface SlurpEntry {
    type: SlurpEntryType;
    name: string;

    count: number;
    totalSize: number;
    largestSize: number;
}

export interface SlurpVisitor extends Visitor {
    idSize?: number;
    timestamp?: Date;
    entries?: SlurpEntry[];
}

export const slurp = (): SlurpVisitor => {
    const strings = new Map<bigint, string>();
    const classes = new Map<bigint, string>();
    const sizes = new Map<bigint, number>();
    const entries = new Map<string, SlurpEntry>();

    const update = (type: SlurpEntryType, name: string, size: number) => {
        let entry = entries.get(name);
        if (!entry) {
            entry = { type, name, count: 0, totalSize: 0, largestSize: 0 };

            entries.set(name, entry);
        }

        entry.count++;
        entry.totalSize += size;
        entry.largestSize = Math.max(entry.largestSize, size);
    };

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
                            update(
                                SlurpEntryType.INSTANCE,
                                `L${classes.get(classObjId) || "unknown"};`,
                                fieldValues.byteLength
                            );
                        },
                        gcObjArrayDump(arrObjId, _stackNum, arrClsId, elems) {
                            const size = elems.reduce((acc, v) => acc + (sizes.get(v) || 0), 0);

                            sizes.set(arrObjId, size);
                            update(SlurpEntryType.OBJ_ARRAY, classes.get(arrClsId) || "[Lunknown;", size);
                        },
                        gcPrimArrayDump(arrObjId, _stackNum, elemType, elems) {
                            const size = valueSize(elemType, this.idSize) * elems.length;

                            sizes.set(arrObjId, size);
                            update(SlurpEntryType.PRIM_ARRAY, `${Type[elemType].toLowerCase()}[]`, size);
                        },
                    };
                },
            };
        },
        end() {
            strings.clear();
            classes.clear();
            sizes.clear();

            this.entries = Array.from(entries.values());
            entries.clear();
        },
    };
};
