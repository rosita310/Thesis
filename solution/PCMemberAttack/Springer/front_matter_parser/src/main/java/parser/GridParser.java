package parser;

import java.util.List;

public interface GridParser {
    List<Member> parse();

    String getName();
}
