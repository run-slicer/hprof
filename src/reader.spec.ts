import { createReadStream } from "node:fs";
import { Readable } from "node:stream";
import { expect } from "chai";
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
                console.log();
            },
            record(tag, tsDelta, length) {
                if (count < 5) {
                    console.log(`tag: ${tag} (${Tag[tag]})`);
                    console.log(`timestamp delta: ${tsDelta}`);
                    console.log(`body length: ${length}`);
                    console.log();
                }

                count++;
                return {
                    utf8() {},
                    loadClass() {},
                    unloadClass() {},
                    frame() {},
                    trace() {},
                    allocSites() {},
                    startThread() {},
                    endThread() {},
                    heapSummary() {},
                    heapDump() {
                        return {
                            gcRootUnknown() {},
                            gcRootThreadObj() {},
                            gcRootJniGlobal() {},
                            gcRootJniLocal() {},
                            gcRootJavaFrame() {},
                            gcRootNativeStack() {},
                            gcRootStickyClass() {},
                            gcRootThreadBlock() {},
                            gcRootMonitorUsed() {},
                            gcClassDump() {},
                            gcInstanceDump() {},
                            gcObjArrayDump() {},
                            gcPrimArrayDump() {},
                        };
                    },
                    cpuSamples() {},
                    controlSettings() {},
                    raw() {},
                } /*{
                    utf8(id, value) {
                        // console.log(`${id}: ${value}`);
                    },
                    heapSummary(liveBytes, liveInsts, allocBytes, allocInsts) {
                        console.log(`live bytes: ${liveBytes}`);
                        console.log(`live instances: ${liveInsts}`);
                        console.log(`allocated bytes: ${allocBytes}`);
                        console.log(`allocated instances: ${allocInsts}`);
                        console.log();
                    },
                }*/;
            },
        });

        console.log(`record count: ${count}`);
        expect(count).to.equal(1041772);
    }).timeout(-1);
});
