import { EOF, wrap } from "./buffer";

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

export interface Visitor {
    header?: (header: string, idSize: number, timestamp: bigint) => void | Promise<void>;
    record?: (tag: number, timestampDelta: number, body: Uint8Array) => void | Promise<void>;
}

const decoder = new TextDecoder();
export const read = async (stream: ReadableStream<Uint8Array>, visitor: Visitor) => {
    const buffer = wrap(stream);

    await visitor.header?.(decoder.decode(await buffer.take(0)), await buffer.getUint32(), await buffer.getBigUint64());

    try {
        while (true) {
            await visitor.record?.(
                await buffer.getUint8(),
                await buffer.getUint32(),
                await buffer.get(await buffer.getUint32())
            );
        }
    } catch (e) {
        if (e !== EOF) {
            throw e;
        }
    }

    await buffer.reader.cancel();
};
