package parser;

import com.google.gson.Gson;

public class Ratio {
    private final int columnNumber;
    private final Rows rows;
    private final double ratio;

    public Ratio(int columnNumber, Rows rows, double ratio) {
        this.columnNumber = columnNumber;
        this.rows = rows;
        this.ratio = ratio;
    }

    public int getColumnNumber() {
        return this.columnNumber;
    }

    public Rows getRows() {
        return this.rows;
    }

    public double getRatio() {
        return this.ratio;
    }

    public String toString() {
        Gson gson = new Gson();
        String jsonString = gson.toJson(this);
        return jsonString;
    }

}
