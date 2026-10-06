package parser;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ColumnRangeTests {

    @org.junit.Test
    public void getModus_1() {
        ColumnRange sut = new ColumnRange();
        sut.add(1);
        int result = sut.getModus();
        assertEquals(1, result);
    }

    @org.junit.Test
    public void getModus_2() {
        ColumnRange sut = new ColumnRange();
        sut.add(1);
        sut.add(2);
        sut.add(2);
        int result = sut.getModus();
        assertEquals(2, result);
    }

    @org.junit.Test
    public void getModus_3() {
        ColumnRange sut = new ColumnRange();
        sut.add(1);
        sut.add(3);
        sut.add(2);
        sut.add(3);
        sut.add(2);
        sut.add(3);
        int result = sut.getModus();
        assertEquals(3, result);
    }

    @org.junit.Test
    public void getModus_4() {
        ColumnRange sut = new ColumnRange();
        sut.add(3);
        sut.add(2);
        sut.add(2);
        sut.add(3);
        int result = sut.getModus();
        assertEquals(2, result);
    }

    @org.junit.Test
    public void getModus_5() {
        ColumnRange sut = new ColumnRange();
        int result = sut.getModus();
        assertEquals(0, result);
    }

}
