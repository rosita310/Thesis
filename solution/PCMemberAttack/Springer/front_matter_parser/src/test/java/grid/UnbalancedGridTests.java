package grid;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class UnbalancedGridTests {
    private UnbalancedGrid<Integer> getTestGrid() {
        UnbalancedGrid<Integer> sut = new UnbalancedGrid<>();
        //  1   2   3
        //  4   5   6
        //  7   8   9
        sut.createCell(1, 0, 0);
        sut.createCell(2, 0, 1);
        sut.createCell(3, 0, 2);
        sut.createCell(4, 1, 0);
        sut.createCell(5, 1, 1);
        sut.createCell(6, 1, 2);
        sut.createCell(7, 2, 0);
        sut.createCell(8, 2, 1);
        sut.createCell(9, 2, 2);
        return sut;
    }

    @org.junit.Test
    public void getPosition_1() {
        UnbalancedGrid<Integer> sut = getTestGrid();
        Position<Integer> position = sut.getPosition(0, 0);
        Integer result = position.getElement();
        assertEquals(1, result);
    }

    @org.junit.Test
    public void getPosition_9() {
        UnbalancedGrid<Integer> sut = getTestGrid();
        Position<Integer> position = sut.getPosition(2, 2);
        Integer result = position.getElement();
        assertEquals(9, result);
    }

    @org.junit.Test
    public void getDown() {
        UnbalancedGrid<Integer> sut = getTestGrid();
        Position<Integer> position = sut.getPosition(0, 0);
        Position<Integer> newPosition = sut.getDown(position);
        Integer result = newPosition.getElement();
        assertEquals(4, result);
    }

    @org.junit.Test
    public void getUp() {
        UnbalancedGrid<Integer> sut = getTestGrid();
        Position<Integer> position = sut.getPosition(1, 0);
        Position<Integer> newPosition = sut.getUp(position);
        Integer result = newPosition.getElement();
        assertEquals(1, result);
    }
}
