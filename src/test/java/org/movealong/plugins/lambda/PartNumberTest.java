package org.movealong.plugins.lambda;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.google.common.primitives.Ints.asList;
import static com.jnape.palatable.lambda.functions.builtin.fn2.Map.map;
import static com.jnape.palatable.lambda.functions.builtin.fn2.ToCollection.toCollection;
import static org.hamcrest.core.AllOf.allOf;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.movealong.jumpstart.test.HasMatcher.has;
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

        assertThat(partNumbers.get(0),
                   allOf(has(PartNumber::getPartNumber, equalTo(1)),
                         has(PartNumber::getStartPosition, equalTo(0L)),
                         has(PartNumber::getPartSize, equalTo(PART_SIZE))));
        assertThat(partNumbers.get(1),
                   allOf(has(PartNumber::getPartNumber, equalTo(2)),
                         has(PartNumber::getStartPosition, equalTo(PART_SIZE)),
                         has(PartNumber::getPartSize, equalTo(PART_SIZE))));
        assertThat(partNumbers.get(2),
                   allOf(has(PartNumber::getPartNumber, equalTo(3)),
                         has(PartNumber::getStartPosition, equalTo(PART_SIZE * 2)),
                         has(PartNumber::getPartSize, equalTo(3L))));
    }
}