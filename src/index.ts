import { Buffer, EOF, wrap } from "./buffer";

type Awaitable<T> = T | PromiseLike<T>;

// https://github.com/openjdk/jdk/blob/master/src/jdk.hotspot.agent/share/classes/sun/jvm/hotspot/utilities/HeapHprofBinWriter.java

export enum Tag {
    UTF8 = 0x01,
    LOAD_CLASS = 0x02,
    UNLOAD_CLASS = 0x03,
    FRAME = 0x04,
    TRACE = 0x05,
    ALLOC_SITES = 0x06,
    HEAP_SUMMARY = 0x07,
    START_THREAD = 0x0a,
    END_THREAD = 0x0b,
    HEAP_DUMP = 0x0c,
    CPU_SAMPLES = 0x0d,
    CONTROL_SETTINGS = 0x0e,
    HEAP_DUMP_SEGMENT = 0x1c,
    HEAP_DUMP_END = 0x2c,
}

export enum HeapDumpTag {
    GC_ROOT_UNKNOWN = 0xff,
    GC_ROOT_JNI_GLOBAL = 0x01,
    GC_ROOT_JNI_LOCAL = 0x02,
    GC_ROOT_JAVA_FRAME = 0x03,
    GC_ROOT_NATIVE_STACK = 0x04,
    GC_ROOT_STICKY_CLASS = 0x05,
    GC_ROOT_THREAD_BLOCK = 0x06,
    GC_ROOT_MONITOR_USED = 0x07,
    GC_ROOT_THREAD_OBJ = 0x08,
    GC_CLASS_DUMP = 0x20,
    GC_INSTANCE_DUMP = 0x21,
    GC_OBJ_ARRAY_DUMP = 0x22,
    GC_PRIM_ARRAY_DUMP = 0x23,
}

export enum Type {
    ARRAY_OBJECT = 1,
    NORMAL_OBJECT = 2,
    BOOLEAN = 4,
    CHAR = 5,
    FLOAT = 6,
    DOUBLE = 7,
    BYTE = 8,
    SHORT = 9,
    INT = 10,
    LONG = 11,
}

export interface Value<T> {
    type: number | Type;
    value: T;
}

export interface NumberValue extends Value<number> {
    type: Type.BOOLEAN | Type.CHAR | Type.FLOAT | Type.DOUBLE | Type.BYTE | Type.SHORT | Type.INT;
}

export interface LongValue extends Value<bigint> {
    type: Type.NORMAL_OBJECT | Type.LONG;
}

export interface PoolItem {
    index: number;
    value: Value<any>;
}

export type Pool = PoolItem[];

export interface Field {
    name: bigint;
    type: number | Type;
    value?: Value<any>; // static if set
}

export interface HeapDumpRecordVisitor {
    gcRootUnknown?: (objId: bigint) => Awaitable<void>;
    gcRootThreadObj?: (objId: bigint, seq: number, stackSeq: number) => Awaitable<void>;
    gcRootJniGlobal?: (objId: bigint, jniRefId: bigint) => Awaitable<void>;
    gcRootJniLocal?: (objId: bigint, threadNum: number, frameNum: number) => Awaitable<void>;
    gcRootJavaFrame?: (objId: bigint, threadNum: number, frameNum: number) => Awaitable<void>;
    gcRootNativeStack?: (objId: bigint, threadNum: number) => Awaitable<void>;
    gcRootStickyClass?: (objId: bigint) => Awaitable<void>;
    gcRootThreadBlock?: (objId: bigint, threadNum: number) => Awaitable<void>;
    gcRootMonitorUsed?: (objId: bigint) => Awaitable<void>;
    gcClassDump?: (
        clsObjId: bigint,
        stackNum: number,
        superObjId: bigint,
        loaderObjId: bigint,
        signerObjId: bigint,
        protDomainObjId: bigint,
        reserved1: bigint,
        reserved2: bigint,
        instSize: number,
        pool: Pool,
        staticFields: Field[],
        instFields: Field[]
    ) => Awaitable<void>;
    gcInstanceDump?: (objId: bigint, stackNum: number, classObjId: bigint, fieldValues: Uint8Array) => Awaitable<void>;
    gcObjArrayDump?: (arrObjId: bigint, stackNum: number, arrClsId: bigint, elems: bigint[]) => Awaitable<void>;
    gcPrimArrayDump?: (
        arrObjId: bigint,
        stackNum: number,
        elemType: number | Type,
        elems: Value<any>[]
    ) => Awaitable<void>;
}

export interface AllocationSite {
    array: number | Type;
    num: number;
    stackNum: number;
    liveBytes: number;
    liveInsts: number;
    allocBytes: number;
    allocInsts: number;
}

export interface CPUTrace {
    samples: number;
    stackNum: number;
}

export interface RecordVisitor {
    utf8?: (id: bigint, value: string) => Awaitable<void>;
    loadClass?: (num: number, objId: bigint, stackNum: number, nameId: bigint) => Awaitable<void>;
    unloadClass?: (num: number) => Awaitable<void>;
    frame?: (
        stackId: bigint,
        methodNameId: bigint,
        methodSigId: bigint,
        sfNameId: bigint,
        classNum: number,
        lineNum: number
    ) => Awaitable<void>;
    trace?: (stackNum: number, threadNum: number, frameIds: bigint[]) => Awaitable<void>;
    allocSites?: (
        flags: number,
        cutoffRatio: number,
        liveBytes: number,
        liveInsts: number,
        allocBytes: bigint,
        allocInsts: bigint,
        sites: AllocationSite[]
    ) => Awaitable<void>;
    startThread?: (
        num: number,
        objId: bigint,
        stackNum: number,
        nameId: bigint,
        groupNameId: bigint,
        groupParentNameId: bigint
    ) => Awaitable<void>;
    endThread?: (num: number) => Awaitable<void>;
    heapSummary?: (liveBytes: number, liveInsts: number, allocBytes: bigint, allocInsts: bigint) => Awaitable<void>;
    heapDump?: (segment: boolean) => Awaitable<HeapDumpRecordVisitor | null>;
    cpuSamples?: (totalSamples: number, traces: CPUTrace[]) => Awaitable<void>;
    controlSettings?: (flags: number, traceDepth: number) => Awaitable<void>;
    raw?: (data: Uint8Array) => Awaitable<void>;
}

export interface Visitor {
    header?: (header: string, idSize: number, timestamp: bigint) => Awaitable<void>;
    record?: (tag: number, tsDelta: number, length: number) => Awaitable<RecordVisitor | null>;
}

const readId = async (buffer: Buffer, size: number): Promise<bigint> => {
    // realistically speaking, you're only ever going to have 8 (64-bit) and 4 (32-bit)
    // but might as well account for smaller sizes if we can read them
    switch (size) {
        case 8:
            return buffer.getBigInt64();
        case 4:
            return BigInt(await buffer.getInt32());
        case 2:
            return BigInt(await buffer.getInt16());
        case 1:
            return BigInt(await buffer.getInt8());
    }

    throw new Error(`Unsupported identifier size ${size}`);
};

export const valueSize = (type: number | Type, idSize: number = -1): number => {
    switch (type) {
        case Type.ARRAY_OBJECT:
        case Type.NORMAL_OBJECT:
            return idSize;
        case Type.BOOLEAN:
        case Type.BYTE:
            return 1;
        case Type.CHAR:
        case Type.SHORT:
            return 2;
        case Type.FLOAT:
        case Type.INT:
            return 4;
        case Type.DOUBLE:
        case Type.LONG:
            return 8;
    }

    throw new Error(`Unsupported value type ${type}`);
};

const readValue = (buffer: Buffer, type: number | Type, idSize: number): Promise<any> => {
    switch (type) {
        case Type.ARRAY_OBJECT:
        case Type.NORMAL_OBJECT:
            return readId(buffer, idSize);
        case Type.BOOLEAN:
        case Type.BYTE:
            return buffer.getUint8();
        case Type.CHAR:
        case Type.SHORT:
            return buffer.getUint16();
        case Type.FLOAT:
            return buffer.getFloat32();
        case Type.DOUBLE:
            return buffer.getFloat64();
        case Type.INT:
            return buffer.getUint32();
        case Type.LONG:
            return buffer.getBigUint64();
    }

    throw new Error(`Unsupported value type ${type}`);
};

const readHDSubRecord = async (buffer: Buffer, visitor: HeapDumpRecordVisitor, idSize: number): Promise<number> => {
    const tag = await buffer.getUint8();
    switch (tag) {
        case HeapDumpTag.GC_ROOT_UNKNOWN: {
            if (visitor.gcRootUnknown) {
                await visitor.gcRootUnknown(await readId(buffer, idSize));
            } else {
                await buffer.skip(idSize);
            }
            return 1 + idSize;
        }
        case HeapDumpTag.GC_ROOT_THREAD_OBJ: {
            if (visitor.gcRootThreadObj) {
                await visitor.gcRootThreadObj(
                    await readId(buffer, idSize),
                    await buffer.getUint32(),
                    await buffer.getUint32()
                );
            } else {
                await buffer.skip(idSize + 8);
            }
            return 1 + idSize + 8;
        }
        case HeapDumpTag.GC_ROOT_JNI_GLOBAL: {
            if (visitor.gcRootJniGlobal) {
                await visitor.gcRootJniGlobal(await readId(buffer, idSize), await readId(buffer, idSize));
            } else {
                await buffer.skip(idSize * 2);
            }
            return 1 + idSize * 2;
        }
        case HeapDumpTag.GC_ROOT_JNI_LOCAL: {
            if (visitor.gcRootJniLocal) {
                await visitor.gcRootJniLocal(
                    await readId(buffer, idSize),
                    await buffer.getUint32(),
                    await buffer.getUint32()
                );
            } else {
                await buffer.skip(idSize + 8);
            }
            return 1 + idSize + 8;
        }
        case HeapDumpTag.GC_ROOT_JAVA_FRAME: {
            if (visitor.gcRootJavaFrame) {
                await visitor.gcRootJavaFrame(
                    await readId(buffer, idSize),
                    await buffer.getUint32(),
                    await buffer.getUint32()
                );
            } else {
                await buffer.skip(idSize + 8);
            }
            return 1 + idSize + 8;
        }
        case HeapDumpTag.GC_ROOT_NATIVE_STACK: {
            if (visitor.gcRootNativeStack) {
                await visitor.gcRootNativeStack(await readId(buffer, idSize), await buffer.getUint32());
            } else {
                await buffer.skip(idSize + 4);
            }
            return 1 + idSize + 4;
        }
        case HeapDumpTag.GC_ROOT_STICKY_CLASS: {
            if (visitor.gcRootStickyClass) {
                await visitor.gcRootStickyClass(await readId(buffer, idSize));
            } else {
                await buffer.skip(idSize);
            }
            return 1 + idSize;
        }
        case HeapDumpTag.GC_ROOT_THREAD_BLOCK: {
            if (visitor.gcRootThreadBlock) {
                await visitor.gcRootThreadBlock(await readId(buffer, idSize), await buffer.getUint32());
            } else {
                await buffer.skip(idSize + 4);
            }
            return 1 + idSize + 4;
        }
        case HeapDumpTag.GC_ROOT_MONITOR_USED: {
            if (visitor.gcRootMonitorUsed) {
                await visitor.gcRootMonitorUsed(await readId(buffer, idSize));
            } else {
                await buffer.skip(idSize);
            }
            return 1 + idSize;
        }
        case HeapDumpTag.GC_CLASS_DUMP: {
            let length = idSize * 7 + 8;
            if (visitor.gcClassDump) {
                const classObjId = await readId(buffer, idSize);
                const stackNum = await buffer.getUint32();
                const superObjId = await readId(buffer, idSize);
                const loaderObjId = await readId(buffer, idSize);
                const signerObjId = await readId(buffer, idSize);
                const protDomainObjId = await readId(buffer, idSize);
                const reserved1 = await readId(buffer, idSize);
                const reserved2 = await readId(buffer, idSize);
                const instSize = await buffer.getUint32();

                const constPoolSize = await buffer.getUint16();
                length += 2;

                const constPool = new Array<PoolItem>(constPoolSize);
                for (let i = 0; i < constPoolSize; i++) {
                    const index = await buffer.getUint16();
                    const type = await buffer.getUint8();

                    constPool[i] = {
                        index,
                        value: { type, value: await readValue(buffer, type, idSize) },
                    };

                    length += 3 + valueSize(type, idSize);
                }

                const numStaticFields = await buffer.getUint16();
                length += 2;

                const staticFields = new Array<Field>(numStaticFields);
                for (let i = 0; i < numStaticFields; i++) {
                    const name = await readId(buffer, idSize);
                    const type = await buffer.getUint8();

                    staticFields[i] = {
                        name,
                        type,
                        value: { type, value: await readValue(buffer, type, idSize) },
                    };

                    length += idSize + 1 + valueSize(type, idSize);
                }

                const numInstFields = await buffer.getUint16();
                length += 2 + (1 + idSize) * numInstFields;

                const instFields = new Array<Field>(numInstFields);
                for (let i = 0; i < numInstFields; i++) {
                    instFields[i] = {
                        name: await readId(buffer, idSize),
                        type: await buffer.getUint8(),
                    };
                }

                await visitor.gcClassDump(
                    classObjId,
                    stackNum,
                    superObjId,
                    loaderObjId,
                    signerObjId,
                    protDomainObjId,
                    reserved1,
                    reserved2,
                    instSize,
                    constPool,
                    staticFields,
                    instFields
                );
            } else {
                await buffer.skip(length);

                const constPoolSize = await buffer.getUint16();
                length += 2;

                for (let i = 0; i < constPoolSize; i++) {
                    await buffer.skip(2);
                    const size = valueSize(await buffer.getUint8(), idSize);
                    await buffer.skip(size);

                    length += 3 + size;
                }

                const numStaticFields = await buffer.getUint16();
                length += 2;

                for (let i = 0; i < numStaticFields; i++) {
                    await buffer.skip(idSize);
                    const size = valueSize(await buffer.getUint8(), idSize);
                    await buffer.skip(size);

                    length += idSize + 1 + size;
                }

                const numInstFields = await buffer.getUint16();
                const size = (1 + idSize) * numInstFields;
                await buffer.skip(size);

                length += 2 + size;
            }
            return 1 + length;
        }
        case HeapDumpTag.GC_INSTANCE_DUMP: {
            let numBytes: number;
            if (visitor.gcInstanceDump) {
                const objId = await readId(buffer, idSize);
                const stackNum = await buffer.getUint32();
                const classObjId = await readId(buffer, idSize);
                numBytes = await buffer.getUint32();

                await visitor.gcInstanceDump(objId, stackNum, classObjId, await buffer.get(numBytes));
            } else {
                await buffer.skip(idSize * 2 + 4);
                numBytes = await buffer.getUint32();
                await buffer.skip(numBytes);
            }
            return 1 + idSize * 2 + 8 + numBytes;
        }
        case HeapDumpTag.GC_OBJ_ARRAY_DUMP: {
            let numElems: number;
            if (visitor.gcObjArrayDump) {
                const arrObjId = await readId(buffer, idSize);
                const stackNum = await buffer.getUint32();
                numElems = await buffer.getUint32();
                const arrClsId = await readId(buffer, idSize);

                const elems = new Array<bigint>(numElems);
                for (let i = 0; i < numElems; i++) {
                    elems[i] = await readId(buffer, idSize);
                }

                await visitor.gcObjArrayDump(arrObjId, stackNum, arrClsId, elems);
            } else {
                await buffer.skip(idSize + 4);
                numElems = await buffer.getUint32();
                await buffer.skip(idSize * (1 + numElems));
            }
            return 1 + idSize * (2 + numElems) + 8;
        }
        case HeapDumpTag.GC_PRIM_ARRAY_DUMP: {
            let numElems: number, elemType: number;
            if (visitor.gcPrimArrayDump) {
                const arrObjId = await readId(buffer, idSize);
                const stackNum = await buffer.getUint32();
                numElems = await buffer.getUint32();
                elemType = await buffer.getUint8();

                const elems = new Array<Value<any>>(numElems);
                for (let i = 0; i < numElems; i++) {
                    elems[i] = { type: elemType, value: await readValue(buffer, elemType, idSize) };
                }

                await visitor.gcPrimArrayDump(arrObjId, stackNum, elemType, elems);
            } else {
                await buffer.skip(idSize + 4);
                numElems = await buffer.getUint32();
                elemType = await buffer.getUint8();
                await buffer.skip(numElems * valueSize(elemType, idSize));
            }
            return 1 + idSize + 9 + numElems * valueSize(elemType, idSize);
        }
    }

    throw new Error(`Unsupported heap dump sub-record tag ${tag}`);
};

const readRecordRaw = async (buffer: Buffer, visitor: RecordVisitor, length: number) => {
    return visitor.raw ? visitor.raw(await buffer.get(length)) : buffer.skip(length);
};

const decoder = new TextDecoder();

const readRecord = async (buffer: Buffer, visitor: Visitor, idSize: number) => {
    const tag = await buffer.getUint8();
    const tsDelta = await buffer.getUint32();
    const length = await buffer.getUint32();

    const rv = await visitor.record?.(tag, tsDelta, length);
    if (!rv) {
        await buffer.skip(length);
        return;
    }

    // statement of hell
    switch (tag) {
        case Tag.UTF8: {
            if (rv.utf8) {
                await rv.utf8(await readId(buffer, idSize), decoder.decode(await buffer.get(length - idSize)));
            } else {
                await readRecordRaw(buffer, rv, length);
            }
            return;
        }
        case Tag.LOAD_CLASS: {
            if (rv.loadClass) {
                await rv.loadClass(
                    await buffer.getUint32(),
                    await readId(buffer, idSize),
                    await buffer.getUint32(),
                    await readId(buffer, idSize)
                );
            } else {
                await readRecordRaw(buffer, rv, length);
            }
            return;
        }
        case Tag.UNLOAD_CLASS: {
            if (rv.unloadClass) {
                await rv.unloadClass(await buffer.getUint32());
            } else {
                await readRecordRaw(buffer, rv, length);
            }
            return;
        }
        case Tag.FRAME: {
            if (rv.frame) {
                await rv.frame(
                    await readId(buffer, idSize),
                    await readId(buffer, idSize),
                    await readId(buffer, idSize),
                    await readId(buffer, idSize),
                    await buffer.getUint32(),
                    await buffer.getInt32()
                );
            } else {
                await readRecordRaw(buffer, rv, length);
            }
            return;
        }
        case Tag.TRACE: {
            if (rv.trace) {
                const stackNum = await buffer.getUint32();
                const threadNum = await buffer.getUint32();
                const numFrames = await buffer.getUint32();

                const frames = new Array<bigint>(numFrames);
                for (let i = 0; i < numFrames; i++) {
                    frames[i] = await readId(buffer, idSize);
                }

                await rv.trace(stackNum, threadNum, frames);
            } else {
                await readRecordRaw(buffer, rv, length);
            }
            return;
        }
        case Tag.ALLOC_SITES: {
            if (rv.allocSites) {
                const flags = await buffer.getUint16();
                const cutoffRatio = await buffer.getUint32();
                const liveBytes = await buffer.getUint32();
                const liveInsts = await buffer.getUint32();
                const allocBytes = await buffer.getBigUint64();
                const allocInsts = await buffer.getBigUint64();
                const numSites = await buffer.getUint32();

                const sites = new Array<AllocationSite>(numSites);
                for (let i = 0; i < numSites; i++) {
                    sites[i] = {
                        array: await buffer.getUint8(),
                        num: await buffer.getUint32(),
                        stackNum: await buffer.getUint32(),
                        liveBytes: await buffer.getUint32(),
                        liveInsts: await buffer.getUint32(),
                        allocBytes: await buffer.getUint32(),
                        allocInsts: await buffer.getUint32(),
                    };
                }

                await rv.allocSites(flags, cutoffRatio, liveBytes, liveInsts, allocBytes, allocInsts, sites);
            } else {
                await readRecordRaw(buffer, rv, length);
            }
            return;
        }
        case Tag.START_THREAD: {
            if (rv.startThread) {
                await rv.startThread(
                    await buffer.getUint32(),
                    await readId(buffer, idSize),
                    await buffer.getUint32(),
                    await readId(buffer, idSize),
                    await readId(buffer, idSize),
                    await readId(buffer, idSize)
                );
            } else {
                await readRecordRaw(buffer, rv, length);
            }
            return;
        }
        case Tag.END_THREAD: {
            if (rv.endThread) {
                await rv.endThread(await buffer.getUint32());
            } else {
                await readRecordRaw(buffer, rv, length);
            }
            return;
        }
        case Tag.HEAP_SUMMARY: {
            if (rv.heapSummary) {
                await rv.heapSummary(
                    await buffer.getUint32(),
                    await buffer.getUint32(),
                    await buffer.getBigUint64(),
                    await buffer.getBigUint64()
                );
            } else {
                await readRecordRaw(buffer, rv, length);
            }
            return;
        }
        case Tag.HEAP_DUMP:
        case Tag.HEAP_DUMP_SEGMENT: {
            if (rv.heapDump) {
                const hdrv = await rv.heapDump(tag === Tag.HEAP_DUMP_SEGMENT);
                if (hdrv) {
                    let remaining = length;
                    while (remaining > 0) {
                        remaining -= await readHDSubRecord(buffer, hdrv, idSize);
                    }

                    if (remaining !== 0) {
                        throw new Error("Buffer underflow");
                    }
                } else {
                    await buffer.skip(length);
                }
            } else {
                await readRecordRaw(buffer, rv, length);
            }
            return;
        }
        case Tag.CPU_SAMPLES: {
            if (rv.cpuSamples) {
                const totalSamples = await buffer.getUint32();
                const numTraces = await buffer.getUint32();

                const traces = new Array<CPUTrace>(numTraces);
                for (let i = 0; i < numTraces; i++) {
                    traces[i] = { samples: await buffer.getUint32(), stackNum: await buffer.getUint32() };
                }

                await rv.cpuSamples(totalSamples, traces);
            } else {
                await readRecordRaw(buffer, rv, length);
            }
            return;
        }
        case Tag.CONTROL_SETTINGS: {
            if (rv.controlSettings) {
                await rv.controlSettings(await buffer.getUint32(), await buffer.getUint16());
            } else {
                await readRecordRaw(buffer, rv, length);
            }
            return;
        }
    }

    await readRecordRaw(buffer, rv, length);
};

export const read = async (stream: ReadableStream<Uint8Array>, visitor: Visitor) => {
    const buffer = wrap(stream);

    const header = decoder.decode(await buffer.take(0));
    const idSize = await buffer.getUint32();
    const timestamp = await buffer.getBigUint64();

    await visitor.header?.(header, idSize, timestamp);

    try {
        while (true) {
            await readRecord(buffer, visitor, idSize);
        }
    } catch (e) {
        if (e !== EOF) {
            throw e;
        }
    }

    await buffer.reader.cancel();
};
