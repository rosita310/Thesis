package parser.gridparsers;

import grid.Position;
import grid.UnbalancedGrid;
import parser.Member;
import parser.TextPart;

import java.util.ArrayList;
import java.util.List;

public abstract class AllLastNameFirstNameParser extends Parser {

    public AllLastNameFirstNameParser(UnbalancedGrid<TextPart> grid) {
        super(grid);
    }

    @Override
    public List<Member> parse() {
        List<Member> returnValue = new ArrayList<>();
        for (int i = 0; i < grid.rowCount(); i++) {
            List<Position<TextPart>> row = grid.getRow(i);
            if (row.size() > 0) {
                for (Position<TextPart> pos : row) {
                    if (pos != null) {
                        String text = pos.getElement().getText();
                        Member member = new Member(text);
                        returnValue.add(member);
                    }
                }
            }
        }
        return returnValue;
    }

}
