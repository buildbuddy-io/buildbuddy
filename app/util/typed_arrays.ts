/**
 * This module contains utilities for working with `TypedArray` objects.
 * https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/TypedArray
 */

/**
 * Append-only typed array builder. Useful for consuming a stream of numeric
 * data whose final length is not known ahead of time, and iteratively building
 * a typed array from the data.
 *
 * Values are stored in chunks of size 1, 2, 4, ... while building, avoiding
 * repeated full-array copies as the list grows. Calling `toArray` materializes
 * the final contiguous typed array.
 *
 * Example:
 *
 * ```ts
 * const builder = TypedArrayBuilder.of(Uint32Array);
 * builder.append(1).append(2);
 * const values = builder.toArray();
 * ```
 */
export class TypedArrayBuilder<T extends NumberTypedArray> {
  public length = 0;

  private readonly ArrayType: TypedArrayConstructor<T>;
  private readonly chunks: T[] = [];
  private currentChunkLength = 0;

  /** Creates an empty builder that materializes values as the given typed array type. */
  static of<T extends NumberTypedArray>(ArrayType: TypedArrayConstructor<T>): TypedArrayBuilder<T> {
    return new TypedArrayBuilder(ArrayType);
  }

  private constructor(ArrayType: TypedArrayConstructor<T>) {
    this.ArrayType = ArrayType;
  }

  /** Appends a value to the builder. */
  append(value: number): this {
    let chunk = this.chunks[this.chunks.length - 1];
    if (!chunk || this.currentChunkLength === chunk.length) {
      chunk = new this.ArrayType(nextCapacity(chunk?.length || 0));
      this.chunks.push(chunk);
      this.currentChunkLength = 0;
    }
    chunk[this.currentChunkLength++] = value;
    this.length++;
    return this;
  }

  /** Materializes the appended values as one contiguous typed array. */
  toArray(): T {
    const values = new this.ArrayType(this.length);
    let offset = 0;
    for (let i = 0; i < this.chunks.length; i++) {
      const chunk = this.chunks[i];
      const length = i === this.chunks.length - 1 ? this.currentChunkLength : chunk.length;
      values.set(chunk.subarray(0, length), offset);
      offset += length;
    }
    return values;
  }
}

/** Typed array variants whose elements can be assigned from a JavaScript number. */
export type NumberTypedArray =
  | Int8Array
  | Uint8Array
  | Uint8ClampedArray
  | Int16Array
  | Uint16Array
  | Int32Array
  | Uint32Array
  | Float32Array
  | Float64Array;

/**
 * Constructor signature for types that allocate a new instance from an element
 * count, such as `new Uint32Array(length)`.
 */
export type TypedArrayConstructor<T> = {
  new (length: number): T;
};

function nextCapacity(capacity: number): number {
  return capacity ? capacity * 2 : 1;
}
