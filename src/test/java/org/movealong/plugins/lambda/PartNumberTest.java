package org.movealong.plugins.lambda;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.google.common.primitives.Ints.asList;
import static com.jnape.palatable.lambda.functions.builtin.fn2.Map.map;
import static com.jnape.palatable.lambda.functions.builtin.fn2.ToCollection.toCollection;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.movealong.plugins.lambda.PartNumber.PART_SIZE;

public class PartNumberTest {

    @Test
    public void partNumbers() {
        assertThat(toCollection(ArrayList::new, PartNumber.partNumbers(0)),
                   equalTo(Collections.<PartNumber>emptyList()));

        assertThat(toCollection(ArrayList::new,
                                map(PartNumber::getPartNumber,
                                    PartNumber.partNumbers(PART_SIZE * 2 - 1))),
                   equalTo(asList(1, 2)));
        assertThat(toCollection(ArrayList::new,
                                map(PartNumber::getPartNumber,
                                    PartNumber.partNumbers(PART_SIZE * 2))),
                   equalTo(asList(1, 2)));
        assertThat(toCollection(ArrayList::new,
                                map(PartNumber::getPartNumber,
                                    PartNumber.partNumbers(PART_SIZE * 2 + 1))),
                   equalTo(asList(1, 2, 3)));
    }

    @Test
    public void partBoundaries() {
        long             contentLength = PART_SIZE * 2 + 3;
        List<PartNumber> partNumbers   = toCollection(ArrayList::new, PartNumber.partNumbers(contentLength));

        PartNumber part0 = partNumbers.get(0);
        assertEquals(1, part0.getPartNumber());
        assertEquals(0L, part0.getStartPosition());
        assertEquals(PART_SIZE, part0.getPartSize());

        PartNumber part1 = partNumbers.get(1);
        assertEquals(2, part1.getPartNumber());
        assertEquals(PART_SIZE, part1.getStartPosition());
        assertEquals(PART_SIZE, part1.getPartSize());

        PartNumber part2 = partNumbers.get(2);
        assertEquals(3, part2.getPartNumber());
        assertEquals(PART_SIZE * 2L, part2.getStartPosition());
        assertEquals(3L, part2.getPartSize());
    }
}