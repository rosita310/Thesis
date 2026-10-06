import database.Database;
import document.*;
import grid.UnbalancedGrid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.pdfbox.io.RandomAccessFile;
import org.apache.pdfbox.pdfparser.PDFParser;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.slf4j.MDC;
import parser.*;
import textstripper.PDFLayoutTextStripperFontSize;
import textstripper.TextLine;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Responsibility is to process a file.
 */
public class FileProcessor {

    private final static Logger LOGGER = LoggerFactory.getLogger(FileProcessor.class);

    private final String path;
    private final Database database;
    private final long runId;

    private int fileId;
    private File file;

    public FileProcessor(String path, Database database, long runId) {
        this.path = path;
        this.database = database;
        this.runId = runId;
    }

    /**
     * Here the document is processed
     */
    public void execute() {
        file = new File(path);
        MDC.put("filename", file.getName());
        try {
            LOGGER.info("Start processing {}", file.getName());
            if (Program.UPDATE_DATABASE)
                fileId = database.startProcessingFile(runId, file);
            PDFParser pdfParser = new PDFParser(new RandomAccessFile(file, "r"));
            pdfParser.parse();
            PDDocument pdDocument = new PDDocument(pdfParser.getDocument());
            try {
                PDFTextStripper pdfTextStripper = new PDFLayoutTextStripperFontSize();
                //Uncomment line below to write the text output to a debug file
                //Write(pdfTextStripper.getText(pdDocument),  file.getName() + ".txt");
                pdfTextStripper.writeText(pdDocument, new NullWriter());

                // Textlines of the document
                List<TextLine> textLines = ((PDFLayoutTextStripperFontSize) pdfTextStripper).getAllTextLines();

                // Make a tree structure of the document
                DocumentFactory df = new DocumentFactory();
                Tree<Section> documentTree = df.getDocumentTreePreOrder(textLines);
                String representation = documentTree.preOrderRepresentation();
                LOGGER.info("Document tree representation " + System.lineSeparator() + representation);

                // The position to start processing from. We are looking for the organization section.
                Position<Section> organisationPosition = null;
                for (Position<Section> p : documentTree.positions()) {
                    String title = p.getElement().getTitle().toLowerCase();

                    if (title.contains("organization") || title.contains("organisation")) {
                        organisationPosition = p;
                        break;
                    }

                    if (title.contains("chair") || title.contains("committee") || title.contains("reviewer")) {
                        Position<Section> parent = documentTree.parent(p);
                        int count = 0;
                        for (Position<Section> child : documentTree.children(parent)) {
                            String childTitle = child.getElement().getTitle().toLowerCase();
                            if(childTitle.contains("chair") || childTitle.contains("committee") || childTitle.contains("reviewer")) {
                                count += 1;
                            }
                        }
                        if(count >= 2) { // Magical value 2, if it has more than two it probally is an organisation position in disguise
                            organisationPosition = parent;
                            LOGGER.info("Organisation position in disguise found, childs give certainty of {}", count);
                            break;
                        }
                    }
                }
                List<Position<Section>> startPositions = new ArrayList<>();
                if (organisationPosition != null) {
                    LOGGER.info("Organisation position: {}", organisationPosition.getElement().getTitle());
                    startPositions.add(organisationPosition);
                } else {
                    // e.g. workshop volumes: one committee per workshop, without a shared organisation section
                    for (Position<Section> p : documentTree.positions()) {
                        String title = p.getElement().getTitle().toLowerCase();
                        boolean isCommittee = title.contains("committee") || title.contains("chair") || title.contains("reviewer") || title.contains("referee");
                        if (isCommittee && !hasAncestorIn(documentTree, p, startPositions)) {
                            startPositions.add(p);
                        }
                    }
                    if (startPositions.isEmpty()) {
                        throw new Exception("No organisation position found");
                    }
                    LOGGER.info("No organisation position, processing {} committee sections", startPositions.size());
                }

                // Itereer door alle posities binnen de organisatie
                for (Position<Section> start : startPositions) {
                    for (Position<Section> p : documentTree.preorder(start)) {
                        Section section = p.getElement();
                        // Verwerk de section
                        processSection(section);
                    }
                } // end loop sections

                if (Program.UPDATE_DATABASE)
                    database.endProcessingFile(fileId, representation, "SUCCEEDED", "");
            } catch (Exception e) {
                throw e;
            } finally {
                pdDocument.close();
            }
        } catch (Exception e) {
            LOGGER.error("Error occurred while processing a file", e);
            e.printStackTrace();
            if (Program.UPDATE_DATABASE)
                database.endProcessingFile(fileId, null, "FAILED", e.getMessage());
        }
        LOGGER.info("End processing");
        MDC.remove("filename");
    }

    private boolean hasAncestorIn(Tree<Section> tree, Position<Section> p, List<Position<Section>> positions) {
        for (Position<Section> parent = tree.parent(p); parent != null; parent = tree.parent(parent)) {
            if (positions.contains(parent)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Verwerkt een section.
     * @param section
     */
    private void processSection(Section section) {
        LOGGER.info("Start processing section:" + System.lineSeparator() + section.detailedRepresentation());
        try {
            // Converteer de section naar een grid
            SectionToGridConverter sectionToGridConverter = new SectionToGridConverter();
            UnbalancedGrid<TextPart> grid = sectionToGridConverter.convert(section.getContent());
            LOGGER.info("Grid representation:" + System.lineSeparator() + grid);

            // Verzamel statistieken over de grid
            SectionInfo sectionInfo = SectionInfoFactory.GetSessionInfo(grid);
            LOGGER.info("Section info:" + System.lineSeparator() + sectionInfo);
            section.setSectionInfo(sectionInfo);

            // Haal een parser op, gegeven de grid en de statistieken.
            GridParser parser = GridParserFactory.getInstance().GetParser(section.getSectionInfo(), grid);
            String parserName = parser.getName();
            LOGGER.info("Parser: {}", parserName);

            // Sla de section op in de database
            int sectionId = -1;
            if (Program.UPDATE_DATABASE)
                sectionId = database.saveSection(runId, section, parserName, fileId, sectionToGridConverter.getMergedLines());

            // Haal de members uit de section
            List<Member> members = parser.parse();
            StringBuilder logBuilder = new StringBuilder();
            logBuilder.append("Members (" + members.size() + "):" + System.lineSeparator());

            // Sla de members op
            for (Member m : members) {
                if (m.getName() == null || m.getName().isBlank()) {
                    continue;
                }
                if (m.getRole() == null) {
                    m.setRole(section.getTitle());
                }
                if (Program.UPDATE_DATABASE)
                    database.saveMember(runId, sectionId, m);
                logBuilder.append("\t" + m + System.lineSeparator());
            }
            LOGGER.info(logBuilder.toString());
        } catch (Exception ex) {
            LOGGER.error("Error occurred while processing a section", ex);
        }
        LOGGER.info("End processing section {}", section.getTitle());
    }

    private void Write(String content, String filename) {
        try{
        BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
        writer.write(content);
        writer.close();
        }catch(IOException e) {}
    }
}
