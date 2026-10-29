package parser;

import document.SectionInfo;
import grid.UnbalancedGrid;
import parser.gridparsers.*;

public class GridParserFactory {

    private static GridParserFactory _instance = null;

    private static final double COMMARATIO = 0.4;
    private static final double AFFILIATIONRATIO = 0;

    public static GridParserFactory getInstance() {
        if (_instance == null) {
            _instance = new GridParserFactory();
        }
        return _instance;
    }

    public GridParser GetParser(SectionInfo sectionInfo, UnbalancedGrid<TextPart> grid) throws Exception {

        if (sectionInfo.getNumberOfTextParts() == 0) {
            return new ZeroParser(grid);
        }

        if (sectionInfo.getNumberOfColumns() == 1) {
            if (sectionInfo.getNumberOfRows() > 1
                && sectionInfo.getAffiliationRatio(0, Rows.ODD) == 0
                && sectionInfo.getAffiliationRatio(0, Rows.EVEN) > 0) {
                return new OneColumnOddNameEvenAffiliation(grid);
            }
            if (sectionInfo.getCommaRatio(0) >= 1) {
                return new OneColumnNameAffiliation(grid);
            }
            return new OneLastNameFirstNameParser(grid);
        }

        if (sectionInfo.getNumberOfColumns() == 2) {
            if (sectionInfo.getPositionRatio(0) > 0.5) {
                return new Two_Role_NameAff(grid);
            }
            if (sectionInfo.getCommaRatio(0) < COMMARATIO && sectionInfo.getCommaRatio(1) > COMMARATIO) {
                return new Two_Name_Affiliation(grid);
            }
            else {
                return new TwoLastNameFirstNameParser(grid);
            }
        }

        if (sectionInfo.getNumberOfColumns() == 3) {
            if (sectionInfo.getAffiliationRatio(0) <= AFFILIATIONRATIO
                && sectionInfo.getAffiliationRatio(1) <= AFFILIATIONRATIO
                && sectionInfo.getAffiliationRatio(2) > AFFILIATIONRATIO
            ) {
                return new ThreeLastFirstAffParser(grid);
            } else {
                return new ThreeLastNameFirstNameParser(grid);
            }
        }

        return new ManyLastNameFirstNameParser(grid);
    }


}
