package parser;

import document.Section;
import grid.Position;
import grid.UnbalancedGrid;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Converts a section to members
 */
public class SectionToGridConverter {

    public int getMergedLines() {
        return mergedLines;
    }

    private int mergedLines;

    /**
     * Converts a section to a grid
     */
    public UnbalancedGrid<TextPart> convert(Iterable<String> content) {
        UnbalancedGrid<TextPart> grid = new UnbalancedGrid<>();

        mergedLines = 0; // The number of lines that are merged into the lines before

        int rowNumber = 0; // vertical position in the grid
        // key value pair to get the startposition for a certain columns
        // e.g. column 2 starts at position 20
        ArrayList<Integer> partStarts = new ArrayList<>();
        // boolean if the partStarts hashmap should be reloaded e.g. after an 
        // empty line (page break)
        boolean resetPartStarts = true;

        // Itereer door de regels
        for (String line : content) {
            boolean merged = false;
            if (line.trim().equals("")) {
                resetPartStarts = true;
                continue;
            }

            // Split de regel in stukken verdeeld door 2 of meer spaties
            String[] lineParts = line.trim().split("\\s\\s+");
            // A line with more columns than the current layout (e.g. names under a role heading) defines a new layout
            if (resetPartStarts || lineParts.length > partStarts.size()) {
                partStarts.clear();
                for (int colNumber = 0; colNumber < lineParts.length; colNumber++) {
                    String text = lineParts[colNumber];
                    int index = line.indexOf(text);
                    partStarts.add(Integer.valueOf(index));
                }
                resetPartStarts = false;
            }

            boolean allTextPartsAlligned = true;
            for (int colNumber = 0; colNumber < lineParts.length; colNumber++) {
                String text = lineParts[colNumber].trim();
                int textIndex = line.indexOf(text);
                int partStart = partStarts.get(colNumber);
                if (partStart != textIndex) {
                    allTextPartsAlligned = false;
                }
            }

            if (lineParts.length == partStarts.size() && allTextPartsAlligned) {
                for (int colNumber = 0; colNumber < lineParts.length; colNumber++) {
                    grid.createCell(new TextPart(lineParts[colNumber], 0), rowNumber, colNumber);
                }
            } else {
                for (int colNumber = 0; colNumber < lineParts.length; colNumber++) {
                    String text = lineParts[colNumber].trim();
                    int textIndex = line.indexOf(text);
                    // Column where this part should be placed
                    int colNumberLinePart = 0;
                    int offset = 0;
                    int newOffset = 0;
                    for (int colIndex : partStarts) {
                        newOffset = Math.abs(textIndex - colIndex);
                        if (textIndex >= colIndex || newOffset <= 1) {
                            colNumberLinePart = partStarts.indexOf(colIndex);
                            offset = newOffset;
                        } else {
                            break;
                        }
                    }
                    Position<TextPart> previous = getCell(grid, rowNumber - 1, colNumberLinePart);
                    if (offset <= 1 || previous == null) {
                        grid.createCell(new TextPart(text, 0), rowNumber, colNumberLinePart);
                    } else {
                        String oldText = previous.getElement().getText();
                        String newText = oldText + " " + text;
                        merged = true;
                        grid.createCell(new TextPart(newText, 0), rowNumber-1, colNumberLinePart);
                    }

                } // end for col
            } // end if
            if (merged) {
                mergedLines++;
            }
            rowNumber++;
        } // end for every line
        return grid;
    }

    private Position<TextPart> getCell(UnbalancedGrid<TextPart> grid, int row, int column) {
        if (row < 0 || row >= grid.rowCount()) {
            return null;
        }
        List<Position<TextPart>> cells = grid.getRow(row);
        return column < cells.size() ? cells.get(column) : null;
    }

    /**
     *
     * @param positions die voorkomen in de tekst
     */
    public List<ColumnRange> getColumnRanges(List<Integer> positions) {
        List<ColumnRange> ranges = new ArrayList<>();
        for (Integer value : positions) {
//            System.out.println("Processing: " + value);
            boolean valuePlaced = false;

            // Eerst kijken of die binnen een range valt
            for (ColumnRange range : ranges) {
                if (value <= range.getMaximum() && value >= range.getMinimum()) {
                    range.add(value);
                    valuePlaced = true;
                    break;
                }
            }

            // Dan kijken of die aansluitend aan een range valt
            if (!valuePlaced) {
                for (ColumnRange range : ranges) {
                    if (value <= range.getMaximum() + 1 && value >= range.getMinimum() - 1) {
                        range.add(value);
                        valuePlaced = true;
                        break;
                    }
                }
            }

            if (!valuePlaced) {
                // Nieuwe maken
                ColumnRange columnRange = new ColumnRange();
                columnRange.add(value);
                ranges.add(columnRange);
            }
        }

        return ranges;
    }

    /**
     * Public for test purposes.
     */
    public List<ColumnRange> cleanColumnRanges(List<ColumnRange> ranges) {
        List<ColumnRange> returnValues = new ArrayList<>();
        Collections.sort(ranges);
        int i = 0;
        while (i < ranges.size()) {
            ColumnRange current = ranges.get(i);
            int o = i+1;
            while (o < ranges.size()) {
                ColumnRange other = ranges.get(o);
                if (current.getMaximum() + 1 == other.getMinimum()) {
                    // aansluitend
                    // mergen tot 1
                    current.merge(other);
                    o++;
                } else {
                    break;
                }
            }
            returnValues.add(current);
            i = o;
        }
        return returnValues;
    }

}
