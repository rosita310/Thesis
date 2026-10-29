package parser.gridparsers;

import grid.Position;
import grid.UnbalancedGrid;
import parser.Member;
import parser.TextPart;

import java.util.ArrayList;
import java.util.List;

// Class to interpreted grids with in the first column the last name,
// second column the firstname and third column the affiliation;
public class ThreeLastFirstAffParser extends Parser {

    public ThreeLastFirstAffParser(UnbalancedGrid<TextPart> grid) {
        super(grid);
    }

    @Override
    public List<Member> parse() {
        List<Member> returnValue = new ArrayList<>();
        for (int i = 0; i < grid.rowCount(); i++) {
            List<Position<TextPart>> row = grid.getRow(i);
            if (row.size() > 0) {
                Position<TextPart> p = row.get(0);
                String lastname = null;
                String firstname = null;
                String affiliation = null;
                if (p != null) {
                    lastname = p.getElement().getText();
                }
                p = row.size() > 1 ? row.get(1) : null;
                if (p != null) {
                    firstname = p.getElement().getText();
                }
                p = row.size() > 2 ? row.get(2) : null;
                if (p != null) {
                    affiliation = p.getElement().getText();
                }
                Member member = new Member(lastname, firstname);
                member.setAffiliation(affiliation);
                returnValue.add(member);
            }
        }
        return returnValue;
    }

}
