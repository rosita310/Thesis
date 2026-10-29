package document;

import textstripper.TextLine;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

/**
 * Responsibility is to create a tree of a document.
 */
public class DocumentFactory {

    private static final Logger LOGGER = Logger.getLogger( DocumentFactory.class.getName() );

    /**
     * Creates a document tree from textLines delivered in preorder.
     * @param textLines
     * @return Tree of the document
     */
    public Tree getDocumentTreePreOrder(List<TextLine> textLines) {
        float contentXScale = getMostUsedXScale(textLines);
        float previousLineXScale = 0;
        boolean previousBold = false;

        // Create a new tree with a root
        WeightedLinkedTree<Section> tree = new WeightedLinkedTree<>();
        Position<Section> previousProcessedPosition = tree.addRoot(new Section("DocumentRoot"));

        // Iterate through the textlines
        for (TextLine textLine : textLines) {
            // Remove watermarks (they mess up the sections)
            if (textLine.getLine().toLowerCase().contains("www.verypdf.com")) {
                continue;
            }
            float currentXScale = round(textLine.getXScale(), 2);
            boolean currentBold = textLine.isBold();
            if (currentXScale > contentXScale || (currentBold && currentXScale >= previousLineXScale)) {
                // It is a section header
                if (currentXScale == previousLineXScale && previousBold == currentBold) {
                    // Als de currentXScale gelijk is aan de vorige XScale, dan is het een uitbreiding van de titel (multiline title)
                    Section s = previousProcessedPosition.getElement();
                    s.setTitle(s.getTitle() + " " + textLine.getLine());
                } else {
                    // New section
                    previousProcessedPosition = tree.addChild(previousProcessedPosition, new Section(textLine.getLine()), currentXScale);
                }
            } else if (currentXScale == 0 || currentXScale == contentXScale) {
                previousProcessedPosition.getElement().addToContent(textLine.getLine());
            }
            previousLineXScale = currentXScale;
            previousBold = currentBold;
        }
        return tree;
    }


    /**
     * Calculates the most used XScale of the textlines
     * @param textlines
     * @return Most used XScale
     */
    private float getMostUsedXScale(List<TextLine> textlines) {
        // Create map
        Map<Float, Integer> map = new HashMap<>();

        // Load map
        for (TextLine textLine : textlines) {
            Float xScale = textLine.getXScale();
            map.put(xScale, map.getOrDefault(xScale, 0) + textLine.getNumberOfCharacters());
        }

        int maxCharacters = 0;
        float xScale = 0;
        for (Map.Entry<Float, Integer> e : map.entrySet()) {
            if (e.getValue() > maxCharacters) {
                maxCharacters = e.getValue();
                xScale = e.getKey();
            }
        }
        return round(xScale, 2);
    }

    public static float round(float d, int decimalPlace) {
        BigDecimal bd = new BigDecimal(Float.toString(d));
        bd = bd.setScale(decimalPlace, RoundingMode.HALF_EVEN);
        return bd.floatValue();
    }

}
