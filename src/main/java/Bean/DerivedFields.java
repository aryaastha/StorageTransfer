package Bean;

import java.io.Serializable;

public class DerivedFields implements Serializable {
    private String newfield;
    private String[] oldFields;
    private String function;

    public String getNewfield() {
        return newfield;
    }

    public String[] getOldFields() {
        return oldFields;
    }

    public String getFunction() {
        return function;
    }
}
