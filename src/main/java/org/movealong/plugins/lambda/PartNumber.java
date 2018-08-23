package org.movealong.plugins.lambda;

import lombok.AllArgsConstructor;
import lombok.Value;

import static com.jnape.palatable.lambda.adt.Maybe.just;
import static com.jnape.palatable.lambda.adt.Maybe.nothing;
import static com.jnape.palatable.lambda.adt.hlist.HList.tuple;
import static com.jnape.palatable.lambda.functions.builtin.fn2.Map.map;
import static com.jnape.palatable.lambda.functions.builtin.fn2.Unfoldr.unfoldr;
import static lombok.AccessLevel.PRIVATE;

@Value
@AllArgsConstructor(access = PRIVATE)
public class PartNumber {
    public static final long PART_SIZE = 5 * 1024 * 1024; // Set part size to 5 MB.

    int  partNumber;
    long contentLength;

    public long getPartSize() {
        return Math.min(PART_SIZE, contentLength - getStartPosition());
    }

    public long getStartPosition() {
        return (partNumber - 1) * PART_SIZE;
    }

    public static Iterable<PartNumber> partNumbers(long contentLength) {
        return map(partNumber -> new PartNumber(partNumber + 1, contentLength),
                   unfoldr(x -> x * PART_SIZE < contentLength
                                ? just(tuple(x, x + 1))
                                : nothing(),
                           0));
    }
}
