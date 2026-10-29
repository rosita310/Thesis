package parser.gridparsers;

import grid.Position;
import grid.UnbalancedGrid;
import parser.Member;
import parser.TextPart;

import java.util.ArrayList;
import java.util.List;

public class OneColumnNameAffiliation extends Parser {

    public OneColumnNameAffiliation(UnbalancedGrid<TextPart> grid) {
        super(grid);
    }

    @Override
    public List<Member> parse() {
        List<Member> returnValue = new ArrayList<>();
        for (int i = 0; i < grid.rowCount(); i++) {
            List<Position<TextPart>> row = grid.getRow(i);
            if (row.size() > 0) {
                for (Position<TextPart> pos : row) {
                    String text = pos.getElement().getText();
                    String name = text.substring( 0, text.indexOf(",")).trim();
                    String affiliation = text.substring(text.indexOf(",")+1, text.length()).trim();
                    Member member = new Member(name);
                    member.setAffiliation(affiliation);
                    returnValue.add(member);
                }
            }
        }
        return returnValue;
    }

}
