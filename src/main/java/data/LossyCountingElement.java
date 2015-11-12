package data;

/**
 * Created by ctebbe
 */
public class LossyCountingElement {

    public final String element;
    public int frequency;
    public int delta;

    public LossyCountingElement(String e, int delta) {
        element = e;
        frequency = 1;
        this.delta = delta;
    }

    public void increment() {
        frequency++;
    }
}
