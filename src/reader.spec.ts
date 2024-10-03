import { createReadStream } from "node:fs";
import { Readable } from "node:stream";
import { read, Tag } from "./";

describe("reader", () => {
    it("read samples/Main_14891_02_10_2024_16_01_21.hprof", async () => {
        const stream = createReadStream("samples/Main_14891_02_10_2024_16_01_21.hprof");

        let count = 0;
        await read(Readable.toWeb(stream), {
            header(header, idSize, timestamp) {
                console.log(`header: ${header}`);
                console.log(`identifier size: ${idSize}`);
                console.log(`timestamp: ${new Date(Number(timestamp))}`);
            },
            record(tag, timestampDelta, body) {
                if (count === 0) {
                    console.log(`tag: ${tag} (${Tag[tag]})`);
                    console.log(`timestamp delta: ${timestampDelta}`);
                    console.log(`body length: ${body.length}`);
                }

                count++;
            },
        });

        console.log(`record count: ${count}`);
    }).timeout(5000);
});
