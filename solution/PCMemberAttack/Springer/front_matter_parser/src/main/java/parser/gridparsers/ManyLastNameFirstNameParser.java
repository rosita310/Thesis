package parser.gridparsers;

import grid.UnbalancedGrid;
import parser.TextPart;

public class ManyLastNameFirstNameParser extends AllLastNameFirstNameParser {

    public ManyLastNameFirstNameParser(UnbalancedGrid<TextPart> grid) {
        super(grid);
    }

    @Override
    public String getName() {
        return this.getClass().getSimpleName();
    }
}
