import { describe, expect, it } from "vitest";
import { classifySequence, compareSequences, mergeBySequence } from "./sequence";

describe("decimal event sequences", () => {
  it("orders int8 values without converting them to unsafe JavaScript numbers", () => {
    expect(compareSequences("9007199254740993", "9007199254740992")).toBe(1);
  });

  it("classifies replay duplicates and missing sequence ranges", () => {
    expect(classifySequence("8", "8")).toBe("duplicate");
    expect(classifySequence("8", "7")).toBe("duplicate");
    expect(classifySequence("8", "9")).toBe("next");
    expect(classifySequence("8", "10")).toBe("gap");
  });

  it("deduplicates overlapping pages and keeps timeline order", () => {
    const first = [{ sequence: "10", type: "started" }, { sequence: "11", type: "step" }];
    const replay = [{ sequence: "11", type: "duplicate" }, { sequence: "12", type: "finished" }];

    expect(mergeBySequence(first, replay)).toEqual([
      { sequence: "10", type: "started" },
      { sequence: "11", type: "step" },
      { sequence: "12", type: "finished" },
    ]);
  });
});
