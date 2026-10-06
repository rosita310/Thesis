package document;

import com.google.gson.Gson;
import parser.Ratio;
import parser.Rows;

import java.util.ArrayList;
import java.util.List;


public class SectionInfo {
    private int numberOfRows;
    private List<Ratio> positionRations;

    public int getNumberOfColumns() {
        return numberOfColumns;
    }

    public void setNumberOfColumns(int numberOfColumns) {
        this.numberOfColumns = numberOfColumns;
    }

    public int getNumberOfTextParts() {
        return numberOfTextParts;
    }

    public void setNumberOfTextParts(int numberOfTextParts) {
        this.numberOfTextParts = numberOfTextParts;
    }

    public List<Ratio> getCommaRatios() {
        return commaRatios;
    }

    public List<Ratio> getAffiliationRatios() {
        return affiliationRatios;
    }
    public List<Ratio> getPositionRations() {
        return positionRations;
    }

    public boolean isAllValuesContainsCommas() {
        return allValuesContainsCommas;
    }

    public void setAllValuesContainsCommas(boolean allValuesContainsCommas) {
        this.allValuesContainsCommas = allValuesContainsCommas;
    }

    private int numberOfColumns;
    private int numberOfTextParts;
    private List<Ratio> commaRatios = new ArrayList<>();
    private List<Ratio> affiliationRatios = new ArrayList<>();
    private boolean allValuesContainsCommas;

    public SectionInfo() {

    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("allValuesContainsCommas:\t" + allValuesContainsCommas + System.lineSeparator());
        sb.append("numberOfColumns:\t" + numberOfColumns + System.lineSeparator());
        sb.append("numberOfTextParts:\t" + numberOfTextParts + System.lineSeparator());
        if (commaRatios.size() > 0) {
            sb.append("commaRatios:" + System.lineSeparator());
            for (Ratio ratio : commaRatios) {
                sb.append("\t" + ratio.toString() + System.lineSeparator());
            }
        }
        if (affiliationRatios.size() > 0) {
            sb.append("affiliationRatios:" + System.lineSeparator());
            for (Ratio ratio : affiliationRatios) {
                sb.append("\t" + ratio.toString() + System.lineSeparator());
            }
        }
        if (positionRations.size() > 0) {
            sb.append("positionRations:" + System.lineSeparator());
            for (Ratio ratio : positionRations) {
                sb.append("\t" + ratio.toString() + System.lineSeparator());
            }
        }
        return sb.toString();
    }

    public double getCommaRatio(int columnIndex) throws Exception {
        return getCommaRatio(columnIndex, Rows.ALL);
    }

    public double getCommaRatio(int columnIndex, Rows rows) throws Exception {
        for (Ratio ratio : commaRatios) {
            if (ratio.getRows() == rows && ratio.getColumnNumber() == columnIndex) {
                return ratio.getRatio();
            }
        }
        throw new Exception("No ColumnRatio found for columnIndex: " + columnIndex + " and rows: " + rows);
    }

    public double getAffiliationRatio(int columnIndex) throws Exception {
        return getAffiliationRatio(columnIndex, Rows.ALL);
    }

    public double getAffiliationRatio(int columnIndex, Rows rows) throws Exception {
        for (Ratio ratio : affiliationRatios) {
            if (ratio.getRows() == rows && ratio.getColumnNumber() == columnIndex) {
                return ratio.getRatio();
            }
        }
        throw new Exception("No AffiliationRatio found for columnIndex: " + columnIndex + " and rows: " + rows);
    }

    public double getPositionRatio(int columnIndex) throws Exception {
        return getPositionRatio(columnIndex, Rows.ALL);
    }

    public double getPositionRatio(int columnIndex, Rows rows) throws Exception {
        for (Ratio ratio : positionRations) {
            if (ratio.getRows() == rows && ratio.getColumnNumber() == columnIndex) {
                return ratio.getRatio();
            }
        }
        throw new Exception("No PositionRatio found for columnIndex: " + columnIndex + " and rows: " + rows);
    }

    public void setPositionRations(List<Ratio> columnRatio) {
        this.positionRations = columnRatio;
    }

    public String stringRepCommaRatios() {
        Gson gson = new Gson();
        String json = gson.toJson(commaRatios);
        return json;
    }

    public String stringRepAffiliationRatios() {
        Gson gson = new Gson();
        String json = gson.toJson(affiliationRatios);
        return json;
    }

    public String stringRepPositionRatios() {
        Gson gson = new Gson();
        String json = gson.toJson(positionRations);
        return json;
    }

    public void setCommaRatios(List<Ratio> columnRatio) {
        this.commaRatios = columnRatio;
    }

    public void setAffiliationRatios(List<Ratio> columnRatio) {
        this.affiliationRatios = columnRatio;
    }

    public void setNumberOfRows(int rowCount) {
        this.numberOfRows = rowCount;
    }

    public int getNumberOfRows() {
        return this.numberOfRows;
    }


}
