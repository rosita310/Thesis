package parser;

import grid.UnbalancedGrid;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SectionToGridConverterTests {

    @org.junit.Test
    public void getColumnRanges_1() {
        SectionToGridConverter sut = new SectionToGridConverter();

        List<Integer> input =  new ArrayList<>();
        input.add(2);
        input.add(1);
        input.add(3);
        input.add(6);
        input.add(5);
        input.add(7);

        List<ColumnRange> intermediateResult = sut.getColumnRanges(input);
        assertEquals(2, intermediateResult.size());

        List<ColumnRange> result = sut.cleanColumnRanges(intermediateResult);
        assertEquals(2, result.size());

    }

    @org.junit.Test
    public void getColumnRanges_2() {
        SectionToGridConverter sut = new SectionToGridConverter();

        List<Integer> input =  new ArrayList<>();
        input.add(1);
        input.add(3);
        input.add(2);
        input.add(5);
        input.add(7);
        input.add(6);

        List<ColumnRange> intermediateResult = sut.getColumnRanges(input);
        assertEquals(4, intermediateResult.size());

        List<ColumnRange> result = sut.cleanColumnRanges(intermediateResult);
        assertEquals(2, result.size());
    }

    @org.junit.Test
    public void getColumnRanges_3() {
        SectionToGridConverter sut = new SectionToGridConverter();

        List<Integer> input =  new ArrayList<>();
        input.add(8);
        input.add(1);
        input.add(6);
        input.add(3);
        input.add(7);
        input.add(2);

        List<ColumnRange> intermediateResult = sut.getColumnRanges(input);
        assertEquals(4, intermediateResult.size());

        List<ColumnRange> result = sut.cleanColumnRanges(intermediateResult);
        assertEquals(2, result.size());
    }

    @org.junit.Test
    public void cleanColumnRanges_3() {
        SectionToGridConverter sut = new SectionToGridConverter();

        List<Integer> input =  new ArrayList<>();
        input.add(0);
        input.add(11);
        input.add(47);

        List<ColumnRange> intermediateResult = sut.getColumnRanges(input);
        assertEquals(3, intermediateResult.size());

        List<ColumnRange> result = sut.cleanColumnRanges(intermediateResult);
        assertEquals(3, result.size());
    }

    // conf_icann_2007-2.pdf
    @org.junit.Test
    public void test_01() {
        SectionToGridConverter sut = new SectionToGridConverter();

        ArrayList<String> input = new ArrayList<>();
        input.add("Abe                 Shigeo           Kobe University");

        UnbalancedGrid<TextPart> grid = sut.convert(input);
        System.out.println(grid.toString());
        assertEquals(3, grid.columnCount());
        assertEquals(1, grid.rowCount());
    }

    // conf_icann_2007-2.pdf
    @org.junit.Test
    public void test_02() {
        SectionToGridConverter sut = new SectionToGridConverter();

        ArrayList<String> input = new ArrayList<>();
        input.add("Abe                 Shigeo           Kobe University");
        input.add("Agell               Nuria            Ramon Llull University");
        input.add("Aiolli              Fabio            Pisa University");
        input.add("Alexandre           Frederic         INRIA Lorraine/LORIA-CNRS");
        input.add("Alexandre           Luıs             University of Beira Interior");
        input.add("Alhoniemi           Esa              Turku University");
        input.add("Andras              Peter            University of Newcastle");

        UnbalancedGrid<TextPart> grid = sut.convert(input);
        System.out.println(grid.toString());
        assertEquals(3, grid.columnCount());
        assertEquals(7, grid.rowCount());
    }

    // conf_icann_2007-2.pdf
    // Overgang pagina's
    @org.junit.Test
    public void test_03() {
        SectionToGridConverter sut = new SectionToGridConverter();

        ArrayList<String> input = new ArrayList<>();
        input.add("Andras              Peter            University of Newcastle");
        input.add("                                                ");
        input.add("                                      ");
        input.add("Anguita                   Davide               Genoa University");

        UnbalancedGrid<TextPart> grid = sut.convert(input);
        System.out.println(grid.toString());
        assertEquals(3, grid.columnCount());
        assertEquals(2, grid.rowCount());
    }

    // conf_icann_2007-2.pdf
    // Extra info volgende regel
    @org.junit.Test
    public void test_04() {
        SectionToGridConverter sut = new SectionToGridConverter();

        ArrayList<String> input = new ArrayList<>();
        input.add("          Avrithis                  Yannis               National Technical University of       ");
        input.add("                                                            Athens");

        UnbalancedGrid<TextPart> grid = sut.convert(input);
        System.out.println(grid.toString());
        assertEquals(3, grid.columnCount());
        assertEquals(1, grid.rowCount());
        assertTrue(grid.getPosition(0,2).getElement().getText().endsWith(("Athens")));
    }

    // conf_icann_2007-2.pdf
    // Extra info volgende regel
    @org.junit.Test
    public void test_05() {
        SectionToGridConverter sut = new SectionToGridConverter();

        ArrayList<String> input = new ArrayList<>();
        input.add("          Avrithis                  Yannis               National Technical University of       ");
        input.add("             a                         b                   c");

        UnbalancedGrid<TextPart> grid = sut.convert(input);
        System.out.println(grid.toString());
        assertEquals(3, grid.columnCount());
        assertEquals(1, grid.rowCount());
        assertTrue(grid.getPosition(0,0).getElement().getText().endsWith(("a")));
        assertTrue(grid.getPosition(0,1).getElement().getText().endsWith(("b")));
        assertTrue(grid.getPosition(0,2).getElement().getText().endsWith(("c")));
    }

    @org.junit.Test
    public void test_06() {
        SectionToGridConverter sut = new SectionToGridConverter();
        ArrayList<String> input = new ArrayList<>();
        input.add("           Gusev, Vladimir               Nicaud, Cyril                Valentin, Oriol");
        input.add("           Haase, Christoph              Niskanen, Reino              Vandomme, Elise");
        input.add("           Harju, Tero                   Palano, Beatrice             Vanier, Pascal");
        input.add("           Hirvensalo, Mika              Perrin, Dominique            Vialette, Stephane");
        input.add("           Holzer, Markus                Plyushchenko, Andrei         Vorel, Vojtech");
        input.add("           Huova, Mari                   Quaas, Karin                 Walen, Tomasz");
        input.add("           Jez, Artur                    Ravikumar, Bala              Wang, Bow-Yaw");
        input.add("           Jungers, Raphael              Reis, Rogerio                Yu, Fang");
        input.add("           Kari, Jarkko                  Richomme, Gwenael");
        input.add("           Klima, Ondrej                 Rodaro, Emanuele");

        UnbalancedGrid<TextPart> grid = sut.convert(input);
        System.out.println(grid.toString());
        assertEquals(3, grid.columnCount());
        assertEquals(10, grid.rowCount());
    }

    @org.junit.Test
    public void test_07() {
        SectionToGridConverter sut = new SectionToGridConverter();
        ArrayList<String> input = new ArrayList<>();
        input.add("Wl odzis law Duch");
        input.add("Torun, Poland & Singapore, ENNS President");
        input.add("                                         ");
        input.add("Danilo Mandic      ");
        input.add("Imperial College London, UK       ");
        input.add("                            ");
        UnbalancedGrid<TextPart> grid = sut.convert(input);
        System.out.println(grid.toString());
        assertEquals(1, grid.columnCount());
        assertEquals(4, grid.rowCount());
    }

}
