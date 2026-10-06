package parser.gridparsers;

import grid.Position;
import grid.UnbalancedGrid;
import parser.Member;
import parser.TextPart;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Two_Role_NameAff extends Parser {

    public Two_Role_NameAff(UnbalancedGrid<TextPart> grid) {
        super(grid);
    }

    @Override
    public List<Member> parse() {
        List<Member> returnValue = new ArrayList<>();
        String role = null;
        for (int i = 0; i < grid.rowCount(); i++) {
            List<Position<TextPart>> row = grid.getRow(i);
            if (row.size() > 0 && row.get(0) != null) {
                Position<TextPart> pos = row.get(0);
                role = pos.getElement().getText().trim();
            }
            if (row.size() > 1 && row.get(1) != null) {
                Position<TextPart> pos = row.get(1);
                String nameAffiliation = pos.getElement().getText();
                Pattern pattern = Pattern.compile("\\((.*?)\\)");
                Matcher matcher = pattern.matcher(nameAffiliation);
                String affiliation = null;
                if (matcher.find())
                {
                    affiliation = matcher.group(1).trim();
                }
                String name = nameAffiliation.split("\\(")[0].trim();

                Member member = new Member(name);
                member.setAffiliation(affiliation);
                member.setRole(role);
                returnValue.add(member);
            }
        }
        return returnValue;
    }

}
