type Sequenced = { sequence: string };

export function compareSequences(left: string, right: string): number {
  const leftValue = BigInt(left);
  const rightValue = BigInt(right);
  return leftValue < rightValue ? -1 : leftValue > rightValue ? 1 : 0;
}

export function classifySequence(
  previous: string,
  next: string,
): "duplicate" | "gap" | "next" {
  const previousValue = BigInt(previous);
  const nextValue = BigInt(next);
  if (nextValue <= previousValue) return "duplicate";
  return nextValue === previousValue + 1n ? "next" : "gap";
}

export function mergeBySequence<T extends Sequenced>(...groups: readonly (readonly T[])[]): T[] {
  const unique = new Map<string, T>();
  for (const group of groups) {
    for (const item of group) {
      if (!unique.has(item.sequence)) unique.set(item.sequence, item);
    }
  }
  return [...unique.values()].sort((left, right) =>
    compareSequences(left.sequence, right.sequence),
  );
}
