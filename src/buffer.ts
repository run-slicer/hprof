// A streaming DataView abstraction. (c) 2024 zlataovce (github.com/zlataovce)
// License: Public domain (or MIT if needed). Attribution appreciated.
// https://gist.github.com/zlataovce/7db8bc7cfe8b7897816495bf2ec3858d
const DEFAULT_LITTLE_ENDIAN = false;
const MIN_READ = 1024 * 1024 * 20; // 20 MiB

export const EOF = new Error("End of stream");

export interface Buffer {
    reader: ReadableStreamDefaultReader<Uint8Array>;
    littleEndian?: boolean;

    view: DataView;
    offset: number;

    get(length: number): Promise<Uint8Array>;
    skip(length: number): Promise<void>;
    take(termValue: number): Promise<Uint8Array>; // exclusive
    getFloat32(): Promise<number>;
    getFloat64(): Promise<number>;
    getInt8(): Promise<number>;
    getInt16(): Promise<number>;
    getInt32(): Promise<number>;
    getUint8(): Promise<number>;
    getUint16(): Promise<number>;
    getUint32(): Promise<number>;
    getBigInt64(): Promise<bigint>;
    getBigUint64(): Promise<bigint>;
}

const arrayToView = (arr: Uint8Array): DataView => {
    return new DataView(arr.buffer, arr.byteOffset, arr.byteLength);
};

const ensure = async (buffer: Buffer, length: number) => {
    if (!(buffer.offset + length > buffer.view.byteLength)) return;

    const chunks: Uint8Array[] = [];

    let chunksLength = 0;
    if (buffer.offset < buffer.view.byteLength) {
        // unread data
        chunksLength = buffer.view.byteLength - buffer.offset;
        chunks.push(new Uint8Array(buffer.view.buffer, buffer.view.byteOffset + buffer.offset, chunksLength));
    }

    const toRead = Math.max(length, MIN_READ);
    while (toRead > chunksLength) {
        const { done, value } = await buffer.reader.read();
        if (done) {
            throw EOF;
        }

        chunksLength += value.byteLength;
        chunks.push(value);
    }

    const combined = new Uint8Array(chunksLength);

    let offset = 0;
    for (const chunk of chunks) {
        combined.set(chunk, offset);
        offset += chunk.byteLength;
    }

    buffer.offset = 0;
    buffer.view = arrayToView(combined);
};

export const wrap = (stream: ReadableStream<Uint8Array>, littleEndian: boolean = DEFAULT_LITTLE_ENDIAN): Buffer => {
    return {
        reader: stream.getReader(),
        littleEndian,
        view: new DataView(new ArrayBuffer(0)),
        offset: 0,

        async get(length: number, copy: boolean = false): Promise<Uint8Array> {
            await ensure(this, length);

            const offset = this.view.byteOffset + this.offset;
            const value = copy
                ? new Uint8Array(this.view.buffer.slice(offset, offset + length))
                : new Uint8Array(this.view.buffer, offset, length);

            this.offset += length;
            return value;
        },

        async skip(length: number): Promise<void> {
            while (length > 0) {
                const available = this.view.byteLength - this.offset;
                if (available >= length) {
                    this.offset += length;
                    return;
                }

                length -= available;

                const { done, value } = await this.reader.read();
                if (done) {
                    throw EOF;
                }

                this.view = arrayToView(value);
                this.offset = 0;
            }
        },

        async getBigInt64(): Promise<bigint> {
            await ensure(this, 8);

            const value = this.view.getBigInt64(this.offset, this.littleEndian);
            this.offset += 8;
            return value;
        },

        async getBigUint64(): Promise<bigint> {
            await ensure(this, 8);

            const value = this.view.getBigUint64(this.offset, this.littleEndian);
            this.offset += 8;
            return value;
        },

        async getFloat32(): Promise<number> {
            await ensure(this, 4);

            const value = this.view.getFloat32(this.offset, this.littleEndian);
            this.offset += 4;
            return value;
        },

        async getFloat64(): Promise<number> {
            await ensure(this, 8);

            const value = this.view.getFloat64(this.offset, this.littleEndian);
            this.offset += 8;
            return value;
        },

        async getInt16(): Promise<number> {
            await ensure(this, 2);

            const value = this.view.getInt16(this.offset, this.littleEndian);
            this.offset += 2;
            return value;
        },

        async getInt32(): Promise<number> {
            await ensure(this, 4);

            const value = this.view.getInt32(this.offset, this.littleEndian);
            this.offset += 4;
            return value;
        },

        async getInt8(): Promise<number> {
            await ensure(this, 1);

            const value = this.view.getInt8(this.offset);
            this.offset += 1;
            return value;
        },

        async getUint16(): Promise<number> {
            await ensure(this, 2);

            const value = this.view.getUint16(this.offset, littleEndian);
            this.offset += 2;
            return value;
        },

        async getUint32(): Promise<number> {
            await ensure(this, 4);

            const value = this.view.getUint32(this.offset, littleEndian);
            this.offset += 4;
            return value;
        },

        async getUint8(): Promise<number> {
            await ensure(this, 1);

            const value = this.view.getUint8(this.offset);
            this.offset += 1;
            return value;
        },

        async take(termValue: number): Promise<Uint8Array> {
            const result: number[] = [];
            while (true) {
                const byte = await this.getUint8();
                if (byte === termValue) {
                    break;
                }

                result.push(byte);
            }

            return new Uint8Array(result);
        },
    };
};
