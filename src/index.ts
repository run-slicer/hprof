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
    cpuSamples?: (totalSamples: number, traces: CPUTrace[]) => Awaitable<void>;
    controlSettings?: (flag: number, traceDepth: number) => Awaitable<void>;
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

const decoder = new TextDecoder();

const readRecordRaw = async (buffer: Buffer, visitor: RecordVisitor, length: number) => {
    if (visitor.raw) {
        await visitor.raw(await buffer.get(length));
    } else {
        await buffer.skip(length);
    }
};

const readRecord = async (buffer: Buffer, visitor: Visitor, idSize: number) => {
    const tag = await buffer.getUint8();
    const tsDelta = await buffer.getUint32();
    const length = await buffer.getUint32();

    const rv = await visitor.record?.(tag, tsDelta, length);
    if (rv) {
        switch (tag) {
            case Tag.UTF8: {
                if (rv.utf8) {
                    await rv.utf8(await readId(buffer, idSize), decoder.decode(await buffer.get(length - idSize)));
                } else {
                    await readRecordRaw(buffer, rv, length);
                }
                break;
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
                break;
            }
            case Tag.UNLOAD_CLASS: {
                if (rv.unloadClass) {
                    await rv.unloadClass(await buffer.getUint32());
                } else {
                    await readRecordRaw(buffer, rv, length);
                }
                break;
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
                break;
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
                break;
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
                break;
            }
            case Tag.END_THREAD: {
                if (rv.endThread) {
                    await rv.endThread(await buffer.getUint32());
                } else {
                    await readRecordRaw(buffer, rv, length);
                }
                break;
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
                break;
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
                break;
            }
            case Tag.CONTROL_SETTINGS: {
                if (rv.controlSettings) {
                    await rv.controlSettings(await buffer.getUint32(), await buffer.getUint16());
                } else {
                    await readRecordRaw(buffer, rv, length);
                }
                break;
            }
            default:
                await readRecordRaw(buffer, rv, length);
        }
    } else {
        await buffer.skip(length);
    }
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
