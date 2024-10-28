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
    value?: T; // undefined with ReaderFlags.SKIP_VALUES
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
        instSize: number,
        pool: Pool,
        staticFields: Field[],
        instFields: Field[]
    ) => Awaitable<void>;
    gcInstanceDump?: (objId: bigint, stackNum: number, clsObjId: bigint, fieldValues: Uint8Array) => Awaitable<void>;
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
    end?: () => Awaitable<void>;
}

const valueSizes: Record<Type, number> = {
    [Type.ARRAY_OBJECT]: -1,
    [Type.NORMAL_OBJECT]: -1,
    [Type.BOOLEAN]: 1,
    [Type.CHAR]: 2,
    [Type.FLOAT]: 4,
    [Type.DOUBLE]: 8,
    [Type.BYTE]: 1,
    [Type.SHORT]: 2,
    [Type.INT]: 4,
    [Type.LONG]: 8,
};

export const valueSize = (type: number | Type, idSize: number = -1): number => {
    let size = valueSizes[type];
    if (!size) {
        throw new Error(`Unsupported value type ${type}`);
    }

    return size === -1 ? idSize : size;
};

type ReaderFunc<T> = () => T | PromiseLike<T>;

interface ReaderContext {
    buffer: Buffer;
    idSize: number;
    flags: number;

    readId: ReaderFunc<bigint>;
}

const idReader = (buffer: Buffer, size: number): ReaderFunc<bigint> => {
    switch (size) {
        case 8:
            return buffer.getBigInt64.bind(buffer);
        case 4:
            return async () => BigInt(await buffer.getInt32());
    }

    throw new Error(`Unsupported identifier size ${size}`);
};

const valueReader = (buffer: Buffer, type: number | Type, readId: ReaderFunc<bigint>): ReaderFunc<any> => {
    switch (type) {
        case Type.BOOLEAN:
        case Type.BYTE:
            return buffer.getUint8.bind(buffer);
        case Type.INT:
            return buffer.getUint32.bind(buffer);
        case Type.LONG:
            return buffer.getBigUint64.bind(buffer);
        case Type.FLOAT:
            return buffer.getFloat32.bind(buffer);
        case Type.DOUBLE:
            return buffer.getFloat64.bind(buffer);
        case Type.CHAR:
        case Type.SHORT:
            return buffer.getUint16.bind(buffer);
        case Type.ARRAY_OBJECT:
        case Type.NORMAL_OBJECT:
            return readId;
    }

    throw new Error(`Unsupported value type ${type}`);
};

const constSubRecordHandlers: Record<number, { handler: string; size?: number; idCount?: number }> = {
    [HeapDumpTag.GC_ROOT_UNKNOWN]: { handler: "gcRootUnknown" },
    [HeapDumpTag.GC_ROOT_THREAD_OBJ]: { handler: "gcRootThreadObj", size: 8 },
    [HeapDumpTag.GC_ROOT_JNI_GLOBAL]: { handler: "gcRootJniGlobal", idCount: 2 },
    [HeapDumpTag.GC_ROOT_JNI_LOCAL]: { handler: "gcRootJniLocal", size: 8 },
    [HeapDumpTag.GC_ROOT_JAVA_FRAME]: { handler: "gcRootJavaFrame", size: 8 },
    [HeapDumpTag.GC_ROOT_NATIVE_STACK]: { handler: "gcRootNativeStack", size: 4 },
    [HeapDumpTag.GC_ROOT_STICKY_CLASS]: { handler: "gcRootStickyClass" },
    [HeapDumpTag.GC_ROOT_THREAD_BLOCK]: { handler: "gcRootThreadBlock", size: 4 },
    [HeapDumpTag.GC_ROOT_MONITOR_USED]: { handler: "gcRootMonitorUsed" },
};

const readSubRecord = async (ctx: ReaderContext, visitor: HeapDumpRecordVisitor): Promise<number> => {
    const { buffer, idSize, readId } = ctx;

    const tag = await buffer.getUint8();

    // fast path for sub-records with constant lengths
    const constRec = constSubRecordHandlers[tag];
    if (constRec && !(constRec.handler in visitor)) {
        const size = (constRec.idCount || 1) * idSize + (constRec.size || 0);

        await buffer.skip(size);
        return 1 + size;
    }

    switch (tag) {
        case HeapDumpTag.GC_ROOT_UNKNOWN: {
            await visitor.gcRootUnknown(await readId());
            return 1 + idSize;
        }
        case HeapDumpTag.GC_ROOT_THREAD_OBJ: {
            await visitor.gcRootThreadObj(await readId(), await buffer.getUint32(), await buffer.getUint32());
            return 1 + idSize + 8;
        }
        case HeapDumpTag.GC_ROOT_JNI_GLOBAL: {
            await visitor.gcRootJniGlobal(await readId(), await readId());
            return 1 + idSize * 2;
        }
        case HeapDumpTag.GC_ROOT_JNI_LOCAL: {
            await visitor.gcRootJniLocal(await readId(), await buffer.getUint32(), await buffer.getUint32());
            return 1 + idSize + 8;
        }
        case HeapDumpTag.GC_ROOT_JAVA_FRAME: {
            await visitor.gcRootJavaFrame(await readId(), await buffer.getUint32(), await buffer.getUint32());
            return 1 + idSize + 8;
        }
        case HeapDumpTag.GC_ROOT_NATIVE_STACK: {
            await visitor.gcRootNativeStack(await readId(), await buffer.getUint32());
            return 1 + idSize + 4;
        }
        case HeapDumpTag.GC_ROOT_STICKY_CLASS: {
            await visitor.gcRootStickyClass(await readId());
            return 1 + idSize;
        }
        case HeapDumpTag.GC_ROOT_THREAD_BLOCK: {
            await visitor.gcRootThreadBlock(await readId(), await buffer.getUint32());
            return 1 + idSize + 4;
        }
        case HeapDumpTag.GC_ROOT_MONITOR_USED: {
            await visitor.gcRootMonitorUsed(await readId());
            return 1 + idSize;
        }
        case HeapDumpTag.GC_CLASS_DUMP: {
            let length = idSize * 7 + 8;
            if (visitor.gcClassDump) {
                const clsObjId = await readId();
                const stackNum = await buffer.getUint32();
                const superObjId = await readId();
                const loaderObjId = await readId();
                const signerObjId = await readId();
                const protDomainObjId = await readId();
                await buffer.skip(idSize * 2); // reserved bytes
                const instSize = await buffer.getUint32();

                const constPoolSize = await buffer.getUint16();
                length += 2;

                const skipValues = (ctx.flags & ReaderFlags.SKIP_VALUES) > 0;

                const constPool = new Array<PoolItem>(constPoolSize);
                for (let i = 0; i < constPoolSize; i++) {
                    const index = await buffer.getUint16();
                    const type = await buffer.getUint8();

                    const size = valueSize(type, idSize);
                    constPool[i] = {
                        index,
                        value: {
                            type,
                            value: skipValues ? await buffer.skip(size) : await valueReader(buffer, type, readId)(),
                        },
                    };

                    length += 3 + size;
                }

                const numStaticFields = await buffer.getUint16();
                length += 2;

                const staticFields = new Array<Field>(numStaticFields);
                for (let i = 0; i < numStaticFields; i++) {
                    const name = await readId();
                    const type = await buffer.getUint8();

                    const size = valueSize(type, idSize);
                    staticFields[i] = {
                        name,
                        type,
                        value: {
                            type,
                            value: skipValues ? await buffer.skip(size) : await valueReader(buffer, type, readId)(),
                        },
                    };

                    length += idSize + 1 + size;
                }

                const numInstFields = await buffer.getUint16();
                length += 2 + (1 + idSize) * numInstFields;

                const instFields = new Array<Field>(numInstFields);
                for (let i = 0; i < numInstFields; i++) {
                    instFields[i] = {
                        name: await readId(),
                        type: await buffer.getUint8(),
                    };
                }

                await visitor.gcClassDump(
                    clsObjId,
                    stackNum,
                    superObjId,
                    loaderObjId,
                    signerObjId,
                    protDomainObjId,
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
                const objId = await readId();
                const stackNum = await buffer.getUint32();
                const clsObjId = await readId();
                numBytes = await buffer.getUint32();

                await visitor.gcInstanceDump(objId, stackNum, clsObjId, await buffer.get(numBytes));
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
                const arrObjId = await readId();
                const stackNum = await buffer.getUint32();
                numElems = await buffer.getUint32();
                const arrClsId = await readId();

                const elems = new Array<bigint>(numElems);
                if ((ctx.flags & ReaderFlags.SKIP_VALUES) > 0) {
                    await buffer.skip(numElems * idSize);

                    elems.fill(0n);
                } else {
                    for (let i = 0; i < numElems; i++) {
                        elems[i] = await readId();
                    }
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
            let numElems: number, size: number;
            if (visitor.gcPrimArrayDump) {
                const arrObjId = await readId();
                const stackNum = await buffer.getUint32();
                numElems = await buffer.getUint32();
                const elemType = await buffer.getUint8();
                size = valueSize(elemType, idSize);

                const elems = new Array<Value<any>>(numElems);
                if ((ctx.flags & ReaderFlags.SKIP_VALUES) > 0) {
                    await buffer.skip(numElems * size);

                    elems.fill({ type: elemType, value: undefined });
                } else {
                    const readValue = valueReader(buffer, elemType, readId);

                    for (let i = 0; i < numElems; i++) {
                        elems[i] = { type: elemType, value: await readValue() };
                    }
                }

                await visitor.gcPrimArrayDump(arrObjId, stackNum, elemType, elems);
            } else {
                await buffer.skip(idSize + 4);
                numElems = await buffer.getUint32();
                const elemType = await buffer.getUint8();
                size = valueSize(elemType, idSize);
                await buffer.skip(numElems * size);
            }
            return 1 + idSize + 9 + numElems * size;
        }
    }

    throw new Error(`Unsupported heap dump sub-record tag ${tag}`);
};

const decoder = new TextDecoder();

const recordHandlers: Record<number, string> = {
    [Tag.UTF8]: "utf8",
    [Tag.LOAD_CLASS]: "loadClass",
    [Tag.UNLOAD_CLASS]: "unloadClass",
    [Tag.FRAME]: "frame",
    [Tag.TRACE]: "trace",
    [Tag.ALLOC_SITES]: "allocSites",
    [Tag.HEAP_SUMMARY]: "heapSummary",
    [Tag.START_THREAD]: "startThread",
    [Tag.END_THREAD]: "endThread",
    [Tag.HEAP_DUMP]: "heapDump",
    [Tag.CPU_SAMPLES]: "cpuSamples",
    [Tag.CONTROL_SETTINGS]: "controlSettings",
    [Tag.HEAP_DUMP_SEGMENT]: "heapDump",
};

const readRecord = async (ctx: ReaderContext, visitor: Visitor) => {
    const { buffer, idSize, readId } = ctx;

    const tag = await buffer.getUint8();
    const tsDelta = await buffer.getUint32();
    const length = await buffer.getUint32();

    const rv = await visitor.record?.(tag, tsDelta, length);
    if (!rv) {
        await buffer.skip(length);
        return;
    }

    if (recordHandlers[tag] in rv) {
        // fast path escape
        switch (tag) {
            case Tag.UTF8: {
                await rv.utf8?.(await readId(), decoder.decode(await buffer.get(length - idSize)));
                return;
            }
            case Tag.LOAD_CLASS: {
                await rv.loadClass?.(
                    await buffer.getUint32(),
                    await readId(),
                    await buffer.getUint32(),
                    await readId()
                );
                return;
            }
            case Tag.UNLOAD_CLASS: {
                await rv.unloadClass?.(await buffer.getUint32());
                return;
            }
            case Tag.FRAME: {
                await rv.frame?.(
                    await readId(),
                    await readId(),
                    await readId(),
                    await readId(),
                    await buffer.getUint32(),
                    await buffer.getInt32()
                );
                return;
            }
            case Tag.TRACE: {
                const stackNum = await buffer.getUint32();
                const threadNum = await buffer.getUint32();
                const numFrames = await buffer.getUint32();

                const frames = new Array<bigint>(numFrames);
                for (let i = 0; i < numFrames; i++) {
                    frames[i] = await readId();
                }

                await rv.trace?.(stackNum, threadNum, frames);
                return;
            }
            case Tag.ALLOC_SITES: {
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

                await rv.allocSites?.(flags, cutoffRatio, liveBytes, liveInsts, allocBytes, allocInsts, sites);
                return;
            }
            case Tag.START_THREAD: {
                await rv.startThread?.(
                    await buffer.getUint32(),
                    await readId(),
                    await buffer.getUint32(),
                    await readId(),
                    await readId(),
                    await readId()
                );
                return;
            }
            case Tag.END_THREAD: {
                await rv.endThread?.(await buffer.getUint32());
                return;
            }
            case Tag.HEAP_SUMMARY: {
                await rv.heapSummary?.(
                    await buffer.getUint32(),
                    await buffer.getUint32(),
                    await buffer.getBigUint64(),
                    await buffer.getBigUint64()
                );
                return;
            }
            case Tag.HEAP_DUMP:
            case Tag.HEAP_DUMP_SEGMENT: {
                const hdrv = await rv.heapDump?.(tag === Tag.HEAP_DUMP_SEGMENT);
                if (hdrv) {
                    let remaining = length;
                    while (remaining > 0) {
                        remaining -= await readSubRecord(ctx, hdrv);
                    }

                    if (remaining !== 0) {
                        throw new Error("Buffer underflow");
                    }
                } else {
                    await buffer.skip(length);
                }
                return;
            }
            case Tag.CPU_SAMPLES: {
                const totalSamples = await buffer.getUint32();
                const numTraces = await buffer.getUint32();

                const traces = new Array<CPUTrace>(numTraces);
                for (let i = 0; i < numTraces; i++) {
                    traces[i] = { samples: await buffer.getUint32(), stackNum: await buffer.getUint32() };
                }

                await rv.cpuSamples?.(totalSamples, traces);
                return;
            }
            case Tag.CONTROL_SETTINGS: {
                await rv.controlSettings?.(await buffer.getUint32(), await buffer.getUint16());
                return;
            }
        }
    } else if (rv.raw) {
        await rv.raw(await buffer.get(length));
    } else {
        await buffer.skip(length);
    }
};

export enum ReaderFlags {
    SKIP_VALUES = 1 << 0,
}

export const read = async (stream: ReadableStream<Uint8Array>, visitor: Visitor, flags: number = 0) => {
    const buffer = wrap(stream);

    const header = decoder.decode(await buffer.take(0));
    const idSize = await buffer.getUint32();
    const timestamp = await buffer.getBigUint64();

    const ctx: ReaderContext = { buffer, idSize, flags, readId: idReader(buffer, idSize) };

    await visitor.header?.(header, idSize, timestamp);

    try {
        while (true) {
            await readRecord(ctx, visitor);
        }
    } catch (e) {
        if (e !== EOF) {
            throw e;
        }
    }

    await visitor.end?.();
    await buffer.reader.cancel();
};
