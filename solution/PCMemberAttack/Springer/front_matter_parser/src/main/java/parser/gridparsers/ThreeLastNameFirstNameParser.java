package parser.gridparsers;

import grid.UnbalancedGrid;
import parser.TextPart;

public class ThreeLastNameFirstNameParser extends AllLastNameFirstNameParser {

    public ThreeLastNameFirstNameParser(UnbalancedGrid<TextPart> grid) {
        super(grid);
    }

    @Override
    public String getName() {
        return this.getClass().getSimpleName();
    }
}
