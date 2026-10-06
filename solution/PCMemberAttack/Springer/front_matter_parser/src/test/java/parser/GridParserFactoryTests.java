package parser;

import document.Section;
import document.SectionInfo;
import grid.UnbalancedGrid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GridParserFactoryTests {

    private final static Logger LOGGER = LoggerFactory.getLogger(GridParserFactoryTests.class);

//    @Before
//    public void setUp() {
//        final Logger logger = LogManager.getLogger(SectionInfoFactory.class.getName());
////        Configurator.setAllLevels("", Level.ALL);
//    }

    private GridParser getParser(SectionToGridConverter sut, Section section) {
        UnbalancedGrid<TextPart> grid = sut.convert(section.getContent());
        System.out.println(grid);
        SectionInfo sectionInfo = SectionInfoFactory.GetSessionInfo(grid);
        System.out.println(sectionInfo);
        GridParser parser = null;
        try {
            parser = GridParserFactory.getInstance().GetParser(sectionInfo, grid);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return parser;
    }

    // conf_dlt_2014
    @org.junit.Test
    public void test1() {
        SectionToGridConverter sut = new SectionToGridConverter();
        Section section = new Section("Program Committee");

        GridParser parser = getParser(sut, section);
        List<Member> result = parser.parse();

        assertEquals("ZeroParser", parser.getName());
        assertEquals(result.size(), 0);
    }

    @org.junit.Test
    public void Two_Name_Affiliation_02() {
        SectionToGridConverter sut = new SectionToGridConverter();
        Section section = new Section("Program Committee");
        section.addToContent("");
        section.addToContent("           Jorge Almeida                       University of Porto, Portugal");
        section.addToContent("           Marie-Pierre Beal                   Universite Paris-Est, France");
        section.addToContent("           Olivier Carton                      Universite Paris Diderot, France");
        section.addToContent("");
        section.addToContent("");
        section.addToContent("");

        GridParser parser = getParser(sut, section);

        List<Member> result = parser.parse();

        assertEquals("Two_Name_Affiliation", parser.getName());
        assertEquals(3, result.size());
    }

    @org.junit.Test
    public void threeLastNameFirstNameParser_1() {
        SectionToGridConverter sut = new SectionToGridConverter();
        Section section = new Section("Program Committee");
        section.addToContent("           Gusev, Vladimir               Nicaud, Cyril                Valentin, Oriol");
        section.addToContent("           Haase, Christoph              Niskanen, Reino              Vandomme, Elise");
        section.addToContent("           Harju, Tero                   Palano, Beatrice             Vanier, Pascal");
        section.addToContent("           Hirvensalo, Mika              Perrin, Dominique            Vialette, Stephane");
        section.addToContent("           Holzer, Markus                Plyushchenko, Andrei         Vorel, Vojtech");
        section.addToContent("           Huova, Mari                   Quaas, Karin                 Walen, Tomasz");
        section.addToContent("           Jez, Artur                    Ravikumar, Bala              Wang, Bow-Yaw");
        section.addToContent("           Jungers, Raphael              Reis, Rogerio                Yu, Fang");
        section.addToContent("           Kari, Jarkko                  Richomme, Gwenael");
        section.addToContent("           Klima, Ondrej                 Rodaro, Emanuele");

        GridParser parser = getParser(sut, section);
        List<Member> result = parser.parse();

        assertEquals("ThreeLastNameFirstNameParser", parser.getName());
        assertEquals(28, result.size());
        for (Member m : result) {
            LOGGER.trace("{}", m);
        }
    }

    @org.junit.Test
    public void twoLastNameFirstNameParser_1() {
        SectionToGridConverter sut = new SectionToGridConverter();
        Section section = new Section("Program Committee");
        section.addToContent("           Gusev, Vladimir               Nicaud, Cyril                ");
        section.addToContent("           Haase, Christoph              Niskanen, Reino              ");
        section.addToContent("           Harju, Tero                   Palano, Beatrice            ");
        section.addToContent("           Hirvensalo, Mika              Perrin, Dominique            ");
        section.addToContent("           Holzer, Markus                Plyushchenko, Andrei      ");
        section.addToContent("           Huova, Mari                   Quaas, Karin            ");
        section.addToContent("           Jez, Artur                    Ravikumar, Bala              ");
        section.addToContent("           Jungers, Raphael              Reis, Rogerio             ");
        section.addToContent("           Kari, Jarkko                  Richomme, Gwenael");
        section.addToContent("           Klima, Ondrej                 Rodaro, Emanuele");

        GridParser parser = getParser(sut, section);
        List<Member> result = parser.parse();

        assertEquals("TwoLastNameFirstNameParser", parser.getName());
        assertEquals(20, result.size());
        for (Member m : result) {
            LOGGER.trace("{}", m);
        }
    }

    @org.junit.Test
    public void threeLastFirstAffiliation() {
        SectionToGridConverter sut = new SectionToGridConverter();
        Section section = new Section("Program Committee");
        section.addToContent("Abe                 Shigeo           Kobe University");
        section.addToContent("Agell               Nuria            Ramon Llull University");
        section.addToContent("Aiolli              Fabio            Pisa Universit");

        GridParser parser = getParser(sut, section);
        List<Member> result = parser.parse();

        assertEquals("ThreeLastFirstAffParser", parser.getName());
        assertEquals(3, result.size());
    }

    @org.junit.Test
    public void OneColumnOddNameEvenAffiliation() {
        SectionToGridConverter sut = new SectionToGridConverter();
        Section section = new Section("Program Committee");
        section.addToContent("Wl odzis law Duch");
        section.addToContent("Torun, Poland & Singapore, ENNS President");
        section.addToContent("                                         ");
        section.addToContent("Danilo Mandic      ");
        section.addToContent("Imperial College London, UK       ");
        section.addToContent("                            ");

        GridParser parser = getParser(sut, section);
        List<Member> result = parser.parse();

        assertEquals("OneColumnOddNameEvenAffiliation", parser.getName());
        assertEquals(2, result.size());
        for (Member m : result) {
            LOGGER.debug(m.toString());
        }
    }

    @org.junit.Test
    public void OneColumnNameAffiliation() {
        SectionToGridConverter sut = new SectionToGridConverter();
        Section section = new Section("Program Committee");
        section.addToContent("Alessandro Sperduti, University of Padova, Italy");
        section.addToContent("Alessandro Villa, University of Grenoble, France");
        section.addToContent("Amir Hussain, University of Stirling, UK");
        section.addToContent("Andreas Nuernberger, University of Magdeburg, Germany");
        section.addToContent("Andreas Stafylopatis, NTUA, Greece");

        GridParser parser = getParser(sut, section);
        List<Member> result = parser.parse();

        assertEquals("OneColumnNameAffiliation", parser.getName());
        assertEquals(5, result.size());
        for (Member m : result) {
            LOGGER.debug(m.toString());
        }
    }

    @org.junit.Test
    public void TwoColumnSection_Name_Affiliation() {
        SectionToGridConverter sut = new SectionToGridConverter();
        Section section = new Section("Program Committee");
        section.addToContent("          Honorary Chairs             Yunhe Pan (Chinese Academy of Engineering,      ");
        section.addToContent("                                         China)                                       ");
        section.addToContent("                                      Songde Ma (Institute of Automation, Chinese     ");
        section.addToContent("                                         Academy of Science, China)                   ");
        section.addToContent("                                      Katsushi Ikeuchi (University of Tokyo, Japan)   ");
        section.addToContent("                                                                                      ");
        section.addToContent("          General Chairs              Tieniu Tan (Institute of Automation, Chinese    ");
        section.addToContent("                                         Academy of Science, China)                   ");
        section.addToContent("                                      Nanning Zheng (Xi’an Jiaotong University,       ");
        section.addToContent("                                         China)                                       ");
        section.addToContent("                                      Yasushi Yagi (Osaka University, Japan)          ");
        section.addToContent("                                                                                      ");
        section.addToContent("          Program Chairs              Hongbin Zha (Peking University, China)          ");
        section.addToContent("                                      Rin-ichiro Taniguchi (Kyushu University,        ");
        section.addToContent("                                         Japan)                                       ");
        section.addToContent("                                      Stephen Maybank (University of London, UK)      ");
        section.addToContent("                                                                                      ");
        section.addToContent("          Organization Chairs         Yanning Zhang (Northwestern Polytechnical       ");
        section.addToContent("                                         University, China)                           ");
        section.addToContent("                                      Jianru Xue (Xi’an Jiaotong University, China)   ");

        GridParser parser = getParser(sut, section);
        List<Member> result = parser.parse();

        assertEquals("Two_Role_NameAff", parser.getName());
        assertEquals(11, result.size());
        for (Member m : result) {
            LOGGER.debug(m.toString());
        }
    }

    //
    @org.junit.Test
    public void Two_Name_Affiliation_01() {
        SectionToGridConverter sut = new SectionToGridConverter();
        Section section = new Section("Program Committee");
        section.addToContent("              Samhaa R. El-Beltagy               Nile University, Egypt  ");

        GridParser parser = getParser(sut, section);
        List<Member> result = parser.parse();

        assertEquals("Two_Name_Affiliation", parser.getName());
        assertEquals(1, result.size());
        for (Member m : result) {
            LOGGER.debug(m.toString());
        }
    }


}
