import { Fetcher } from "./log";
import CHUNKS from "./fake_fetcher_data";

const SIMULATE_IN_PROGRESS = true;

function nowSec() {
  return Date.now() / 1000;
}

class FakeFetcher implements Fetcher {
  private headIndex = SIMULATE_IN_PROGRESS ? Math.floor((CHUNKS.length * 2) / 3) : CHUNKS.length - 1;
  private tailIndex = this.headIndex - 1;
  private startTimeSeconds = nowSec();
  private startOffsetSeconds = CHUNKS[this.headIndex].modify_time;
  private cachedInitialLines: string[] | null = null;

  initialLines(): string[] {
    if (this.cachedInitialLines !== null) return this.cachedInitialLines;
    return (this.cachedInitialLines = getLines(CHUNKS[this.headIndex++].content));
  }

  hasPreviousChunk(): boolean {
    return this.tailIndex >= 0;
  }

  hasNextChunk(): boolean {
    return this.headIndex < CHUNKS.length;
  }

  async fetchNextChunk(): Promise<string[]> {
    console.log("FakeFetcher: fetching NEXT chunk");
    if (!this.hasNextChunk()) return [];
    const next = CHUNKS[this.headIndex++];
    this.headIndex++;
    const t = nowSec() - this.startTimeSeconds + this.startOffsetSeconds;
    if (t < next.modify_time) return [];
    await delay(25 + Math.random() * 200);
    return getLines(next.content);
  }

  async fetchPreviousChunk(): Promise<string[]> {
    console.log("FakeFetcher: fetching PREV chunk");
    if (this.tailIndex < 0) return [];
    const next = CHUNKS[this.tailIndex--];
    await delay(25 + Math.random() * 200);
    return getLines(next.content);
  }
}

function delay(delayMs: number) {
  return new Promise((resolve) => setTimeout(resolve, delayMs));
}

function getLines(content: string): string[] {
  return window.atob(content).split("\n");
}

export default new FakeFetcher();
