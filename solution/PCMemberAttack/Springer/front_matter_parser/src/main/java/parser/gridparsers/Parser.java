package parser.gridparsers;

import grid.UnbalancedGrid;
import parser.GridParser;
import parser.TextPart;

abstract public class Parser implements GridParser {

    protected UnbalancedGrid<TextPart> grid;

    public Parser(UnbalancedGrid<TextPart> grid) {
        this.grid = grid;
    }

    @Override
    public String getName() {
        return this.getClass().getSimpleName();
    }
}
