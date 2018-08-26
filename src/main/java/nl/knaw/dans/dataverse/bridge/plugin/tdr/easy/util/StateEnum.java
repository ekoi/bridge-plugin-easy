package nl.knaw.dans.dataverse.bridge.plugin.tdr.easy.util;
/*
 * @author: Eko Indarto
 */
public enum StateEnum {
    IN_PROGRESS("IN-PROGRESS"),
    ERROR("ERROR"),
    FAILED("FAILED"),
    ARCHIVED("ARCHIVED"),
    REJECTED("REJECTED"),
    INVALID("INVALID");

    private String value;

    StateEnum(String value) {
        this.value = value;
    }

    public String toString() {
        return String.valueOf(value);
    }

    public static StateEnum fromValue(String text) {
        for (StateEnum b : StateEnum.values()) {
            if (String.valueOf(b.value).equals(text)) {
                return b;
            }
        }
        return null;
    }
}