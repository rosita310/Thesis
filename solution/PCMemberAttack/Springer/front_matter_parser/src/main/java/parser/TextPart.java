package parser;

public class TextPart {
    private final String text;
    private final int offset;

    public TextPart(String text, int offset) {
        this.text = text;
        this.offset = offset;
    }

    public String getText() {
        return this.text;
    }

    public String toString() {
        return text + "(Offset: " + offset + ")";
    }
}
