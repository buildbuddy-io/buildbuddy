import { NumberTypedArray, TypedArrayBuilder } from "./typed_arrays";

describe("TypedArrayBuilder", () => {
  it("should preserve initial and appended values", () => {
    const builder = TypedArrayBuilder.of(Uint32Array);

    builder.append(1).append(2).append(3).append(4);

    expect(builder.length).toBe(4);
    expectTypedArray(builder.toArray(), new Uint32Array([1, 2, 3, 4]));
  });

  it("should support NumberTypedArray constructors", () => {
    expectTypedArray(TypedArrayBuilder.of(Int8Array).append(-1).append(2).toArray(), new Int8Array([-1, 2]));
    expectTypedArray(TypedArrayBuilder.of(Uint8Array).append(1).append(2).toArray(), new Uint8Array([1, 2]));
    expectTypedArray(
      TypedArrayBuilder.of(Uint8ClampedArray).append(1).append(2).toArray(),
      new Uint8ClampedArray([1, 2])
    );
    expectTypedArray(TypedArrayBuilder.of(Int16Array).append(-1).append(2).toArray(), new Int16Array([-1, 2]));
    expectTypedArray(TypedArrayBuilder.of(Uint16Array).append(1).append(2).toArray(), new Uint16Array([1, 2]));
    expectTypedArray(TypedArrayBuilder.of(Int32Array).append(-1).append(2).toArray(), new Int32Array([-1, 2]));
    expectTypedArray(TypedArrayBuilder.of(Uint32Array).append(1).append(2).toArray(), new Uint32Array([1, 2]));
    expectTypedArray(
      TypedArrayBuilder.of(Float32Array).append(1.5).append(2.5).toArray(),
      new Float32Array([1.5, 2.5])
    );
    expectTypedArray(
      TypedArrayBuilder.of(Float64Array).append(1.5).append(2.5).toArray(),
      new Float64Array([1.5, 2.5])
    );
  });
});

function expectTypedArray<T extends NumberTypedArray>(values: T, expected: T) {
  expect(values.constructor).toBe(expected.constructor);
  expect(Array.from(values)).toEqual(Array.from(expected));
}
