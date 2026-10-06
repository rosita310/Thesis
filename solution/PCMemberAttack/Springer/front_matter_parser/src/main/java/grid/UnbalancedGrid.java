package grid;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class UnbalancedGrid<E> {

    /**
     * Rows of columns
     */
    private List<List<Cell<E>>> rows = new ArrayList<>();
    private int numberOfCells = 0;

    protected class Cell<E> implements Position<E> {
        private E element;
        private int rowNumber;
        private int colNumber;

        public Cell(E element, int rowNumber, int colNumber) {
            this.element = element;
            this.rowNumber = rowNumber;
            this.colNumber = colNumber;
        }

        public E getElement() throws IllegalStateException {
            return element;
        }

        public String toString() {
            if (element == null) {
                return "<Empty cell>";
            }
            return element.toString();
        }
    }

    public Position<E> getPosition(int row, int column) {
        return rows.get(row).get(column);
    }

    /**
     * @param e
     * @param rowNumber 0-based
     * @param colNumber 0-based
     * @return
     */
    public Position<E> createCell(E e, int rowNumber, int colNumber) {
        Cell cell = new Cell(e, rowNumber, colNumber);

        // Add necessary rows if not exist yet
        for (int i = rows.size(); i <= rowNumber; i++) {
            rows.add(new ArrayList<>());
        }

        List<Cell<E>> column = rows.get(rowNumber);
        for (int i = column.size(); i <= colNumber; i++) {
            column.add(null);
        }
        column.set(colNumber, cell);
        numberOfCells++;
        return cell;
    }

    public int getNumberOfCells() {
        return numberOfCells;
    }

    public Position<E> getDown(Position<E> fromPosition) {
        Cell<E> fromCell = validate(fromPosition);
        if (rows.size() <= fromCell.rowNumber) {
            throw new IllegalStateException("Row under given cell does not exist");
        }
        List<Cell<E>> row = rows.get(fromCell.rowNumber+1);
        if (row.size() <= fromCell.colNumber || row.get(fromCell.colNumber) == null) {
            throw new IllegalStateException("Row underneath does not contain a cell at column position");
        }
        return row.get(fromCell.colNumber);
    }

    public Position<E> getUp(Position<E> fromPosition) {
        Cell<E> fromCell = validate(fromPosition);
        if (rows.size() <= fromCell.rowNumber) {
            throw new IllegalStateException("Row above given cell does not exist");
        }
        List<Cell<E>> row = rows.get(fromCell.rowNumber-1);
        if (row.size() <= fromCell.colNumber || row.get(fromCell.colNumber) == null) {
            throw new IllegalStateException("Row above does not contain a cell at column position");
        }
        return row.get(fromCell.colNumber);
    }

    private Cell<E> validate(Position<E> p) throws IllegalArgumentException {
        if (!(p instanceof Cell)) {
            throw new IllegalArgumentException("Not valid position type");
        }
        Cell<E> cell = (Cell<E>) p;
        return cell;
    }

    public Iterable<Position<E>> leftRightTopBottomElements() {
        List<Position<E>> positions = new ArrayList<>();
        for (List<Cell<E>> row : rows) {
            for (Cell<E> cell : row) {
                positions.add(cell);
            }
        }
        return positions;
    }

    public List<Position<E>> getColumn(int columnIndex) {
        List<Position<E>> positions = new ArrayList<>();
        for (List<Cell<E>> row : rows) {
            if (columnIndex < row.size()) {
                Cell<E> cell = row.get(columnIndex);
                positions.add(cell);
            }
        }
        return positions;
    }

    public int columnCount() {
        int columnCount = 0;
        for (List<Cell<E>> row : rows) {
            if (row.size() > columnCount) {
                columnCount = row.size();
            }
        }
        return columnCount;
    }

    public List<Position<E>> getRow(int rowIndex) {
        List<Position<E>> positions = new ArrayList<>();
        for (Cell<E> cell : rows.get(rowIndex)) {
            positions.add(cell);
        }
        return positions;
    }

    public int rowCount() {
        return rows.size();
    }


    public String toString() {
        if (rows.size() == 0) {
            return "< Empty grid >";
        }

        HashMap<Integer, Integer> columnWidth = new HashMap<Integer, Integer>();

        for (List<Cell<E>> column : rows) {
            for (int columnNum = 0; columnNum < column.size(); columnNum++) {
                Cell<E> cell = column.get(columnNum);
                if (cell == null) continue;
                int textLength = cell.toString().length();
                if (!columnWidth.containsKey(columnNum) || columnWidth.get(columnNum) < textLength) {
                    columnWidth.put(columnNum, textLength);
                }
            }
        }

        StringBuilder horizontalLine = new StringBuilder();
        horizontalLine.append("+");
        for (int i = 0; i < columnWidth.size(); i++) {
            int charNum = columnWidth.get(i) + 2;
            horizontalLine.append("-".repeat(charNum));
            horizontalLine.append("+");
        }
        String lineSeparator = horizontalLine.toString();

        StringBuilder content = new StringBuilder();
        content.append(lineSeparator + System.lineSeparator());

        for (List<Cell<E>> column : rows) {
            StringBuilder lineBuilder = new StringBuilder();
            lineBuilder.append("|");
            for (int columnNum = 0; columnNum < column.size(); columnNum++) {
                Cell<E> cell = column.get(columnNum);
                String cellContent = cell == null ? "< No Cell >" : cell.toString();
                String buf = " " + cellContent + " ".repeat(columnWidth.get(columnNum) - cellContent.length()) + " ";
                lineBuilder.append(buf);
                lineBuilder.append("|");
            }
            content.append(lineBuilder + System.lineSeparator());
            content.append(lineSeparator + System.lineSeparator());
        }
        return content.toString();
    }

}
