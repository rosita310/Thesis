package parser.gridparsers;

import grid.UnbalancedGrid;
import parser.GridParser;
import parser.Member;
import parser.TextPart;

import java.util.ArrayList;
import java.util.List;

public class ZeroParser extends Parser {

    public ZeroParser(UnbalancedGrid<TextPart> grid) {
        super(grid);
    }

    @Override
    public List<Member> parse() {
        return new ArrayList<Member>();
    }


}
