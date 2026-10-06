package parser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class ColumnRange implements Comparable<ColumnRange> {

    private List<Integer> values = new ArrayList<>();

    public int getMinimum() {
        return Collections.min(values);
    }

    public int getMaximum() {
        return Collections.max(values);
    }

    public void add(int value) {
        values.add(value);
    }

    public int getModus() {
        HashMap<Integer, Integer> frequencies = new HashMap<>();
        int cacheValue = 0; // Placeholder for the value with highest frequency
        int cacheFreq = 0; // Number of times the cacheValue occurs
        for (int value : values) {
            int newFreq = 1;
            if (frequencies.containsKey(value)) {
                int currentFreq = frequencies.get(value);
                newFreq = currentFreq+1;
            }
            frequencies.put(value, newFreq);

            if (newFreq > cacheFreq) {
                cacheFreq = newFreq;
                cacheValue = value;
            } else if (newFreq == cacheFreq) {
                if (value < cacheValue) {
                    cacheValue = value;
                }
            }
        }
        return cacheValue;
    }

    @Override
    public int compareTo(ColumnRange other) {
        return this.getMinimum() - other.getMinimum();
    }

    public ColumnRange merge(ColumnRange other) {
        values.addAll(other.values);
        return this;
    }

    public String toString() {
        return "ColumnRange: Min: " + getMinimum() + " | Max: " + getMaximum();
    }
}
