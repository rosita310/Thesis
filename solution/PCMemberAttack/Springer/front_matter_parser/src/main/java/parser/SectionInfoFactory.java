package parser;

import document.SectionInfo;
import grid.Position;
import grid.UnbalancedGrid;

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Verantwoording van deze class is om een SectionInfo te maken op basis van de
 * grid.
 */
public class SectionInfoFactory {

    private final static Logger LOGGER = LoggerFactory.getLogger(SectionInfoFactory.class);

    public static SectionInfo GetSessionInfo(UnbalancedGrid<TextPart> grid) {
        LOGGER.trace("Start GetSessionInfo");
        SectionInfo si = new SectionInfo();
        si.setAllValuesContainsCommas(AllValuesContainsComma(grid));
        si.setNumberOfColumns(grid.columnCount());
        si.setNumberOfRows(grid.rowCount());
        si.setNumberOfTextParts(grid.getNumberOfCells());
        si.setCommaRatios(columnRatio(grid, new String[] {","}));
        si.setAffiliationRatios(columnRatio(grid, new String[] {"universi", "instut", "college"}));
        si.setPositionRations(columnRatio(grid, new String[] {"chair", "committee", "reviewer"}));
        return si;
    }

    private static List<Ratio> columnRatio(UnbalancedGrid<TextPart> grid, String[] searchArguments) {
        List<Ratio> ratios = new ArrayList<>();
        for (int columnIndex = 0; columnIndex < grid.columnCount(); columnIndex++) {
            for (Rows rows : Rows.values()) {
                Ratio ratio = columnRatio(grid, columnIndex, rows, searchArguments);
                if (ratio != null) {
                    ratios.add(ratio);
                }
            }
        }
        return ratios;
    }


    private static Ratio columnRatio(UnbalancedGrid<TextPart> grid, int columnIndex, Rows rows, String[] searchArguments) {
        LOGGER.trace("Getting columnRatio for columnIndex {} rows {} searcharguments: {}",
                columnIndex, rows, searchArguments);
        List<Position<TextPart>> positions = grid.getColumn(columnIndex);
        int numberOfRows = positions.size();
        int numberOfProcessedRows = 0;
        int resultCounter = 0;
        for (int rowIndex = 0; rowIndex < numberOfRows; rowIndex++) {
            if (
                    // rowIndex+1 omdat index is zero-based, maar
                    // ODD en EVEN zijn 'domein'-begrippen
                    rows == Rows.ALL
                    || (rows == Rows.ODD && (rowIndex+1) % 2 == 1)
                    || (rows == Rows.EVEN && (rowIndex+1) % 2 == 0)
            ) {
                LOGGER.trace("Processing rowIndex: {} (rowNumber: {})", rowIndex, rowIndex+1 );

                if (positions.get(rowIndex) != null) {
                    numberOfProcessedRows++;
                    String text = positions.get(rowIndex).getElement().getText().toLowerCase();
                    for (String s : searchArguments) {
                        if (text.contains(s)) {
                            resultCounter++;
                            break;
                        }
                    }
                }
            }
        }
        double ratio = 0;
        if (numberOfProcessedRows != 0) {
            ratio = (double) resultCounter / (double) numberOfProcessedRows;
        }
        LOGGER.trace("Result: numberOfRows: {}, resultCounter: {}, numberOfProcessedRows: {}, ratio: {}",
                numberOfRows,
                resultCounter,
                numberOfProcessedRows,
                ratio);
        if (numberOfProcessedRows > 0) {
            return new Ratio(columnIndex, rows, ratio);
        } else {
            return null;
        }

    }

    private static boolean AllValuesContainsComma(UnbalancedGrid<TextPart> grid) {
        boolean returnValue = true;
        for (Position<TextPart> tp : grid.leftRightTopBottomElements()) {
            if (tp != null && !tp.getElement().getText().contains(",")) {
                returnValue = false;
                break;
            }
        }
        return returnValue;
    }
}
