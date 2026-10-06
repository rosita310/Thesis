package textstripper;

import org.apache.pdfbox.pdmodel.font.PDFont;
import org.apache.pdfbox.pdmodel.font.PDFontDescriptor;
import org.apache.pdfbox.text.TextPosition;

class CharacterFactory {
    public static Character createCharacterFromTextPosition(final TextPosition textPosition, final TextPosition previousTextPosition, final boolean firstCharacterOfLineFound) {
        long numberOfSpaces = numberOfSpacesBetweenTwoCharacters(previousTextPosition, textPosition);
        char character = getCharacterFromTextPosition(textPosition);
        boolean isCharacterPartOfPreviousWord = isCharacterPartOfPreviousWord(previousTextPosition.getUnicode().charAt(0), numberOfSpaces);
        boolean isCharacterAtTheBeginningOfNewLine = isCharacterAtTheBeginningOfNewLine(textPosition, previousTextPosition, firstCharacterOfLineFound);
        boolean isFirstCharacterOfAWord = isFirstCharacterOfAWord(firstCharacterOfLineFound, isCharacterAtTheBeginningOfNewLine, numberOfSpaces);
        boolean isCharacterCloseToPreviousWord = isCharacterCloseToPreviousWord(firstCharacterOfLineFound, numberOfSpaces);
        int index = (int)textPosition.getX() / PDFLayoutTextStripperFontSize.OUTPUT_SPACE_CHARACTER_WIDTH_IN_PT;
        boolean isBold = getIsBold(textPosition);
        return new Character(character,
            index,
            isCharacterPartOfPreviousWord,
            isFirstCharacterOfAWord,
            isCharacterAtTheBeginningOfNewLine,
            isCharacterCloseToPreviousWord,
            textPosition.getFontSize(),
            textPosition.getFontSizeInPt(),
            textPosition.getXScale(),
            isBold);
    }

    private static boolean getIsBold(final TextPosition textPosition) {
        PDFont font = textPosition.getFont();
        PDFontDescriptor fontDescriptor = font.getFontDescriptor();
        return fontDescriptor != null && fontDescriptor.isForceBold();
    }

    private static boolean isCharacterAtTheBeginningOfNewLine(final TextPosition textPosition, final TextPosition previousTextPosition, final boolean firstCharacterOfLineFound) {
        if ( ! firstCharacterOfLineFound ) {
            return true;
        }
        float previousTextYPosition = previousTextPosition.getY();
        return ( Math.round( textPosition.getY() ) < Math.round(previousTextYPosition) );
    }

    private static boolean isFirstCharacterOfAWord(final boolean firstCharacterOfLineFound, final boolean isCharacterAtTheBeginningOfNewLine, final double numberOfSpaces) {
        if ( ! firstCharacterOfLineFound ) {
            return true;
        }
        return numberOfSpaces >= 1 || isCharacterAtTheBeginningOfNewLine;
    }

    private static boolean isCharacterCloseToPreviousWord(final boolean firstCharacterOfLineFound, final double numberOfSpaces) {
        if ( ! firstCharacterOfLineFound ) {
            return false;
        }
        return numberOfSpaces <= 1;
    }

    private static boolean isCharacterPartOfPreviousWord(final char c, final long numberOfSpaces) {
        if (java.lang.Character.isWhitespace(c)) {
            return false;
        }
        return numberOfSpaces < 1;
    }

    private static long numberOfSpacesBetweenTwoCharacters(final TextPosition textPosition1, final TextPosition textPosition2) {
        TextPosition previousTP;
        TextPosition nextTP; 

        if (textPosition1.getX() < textPosition2.getX()) {
            previousTP = textPosition1;
            nextTP = textPosition2;
        } else {
            previousTP = textPosition2;
            nextTP = textPosition1;
        }

        float previousTPEndXPosition = previousTP.getX() + previousTP.getWidth();
        float widthOfSpace = (previousTP.getWidthOfSpace() + nextTP.getWidthOfSpace() + previousTP.getWidth() + nextTP.getWidth()) / 4f;
        float diff = nextTP.getX() - previousTPEndXPosition;
        long round = Math.round(diff / widthOfSpace);
        // Tight spaces fall below half a space width; pdfbox separates words at 0.3 of the average character width
        float averageCharacterWidth = (previousTP.getWidth() + nextTP.getWidth()) / 2f;
        if (round < 1 && diff > 0.3f * averageCharacterWidth) {
            round = 1;
        }
        long numberOfSpaces = Math.max(0, round);
        return numberOfSpaces;
    }

    private static char getCharacterFromTextPosition(final TextPosition textPosition) {
        String string = textPosition.getUnicode();
        char character = string.charAt(0);
        return character;
    }
}
