package com.pooli.plan.domain.enums;

public enum NetworkType {

    LTE("LTE"),
    FIVE_G("5G");

    private final String value;

    NetworkType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static NetworkType from(String value) {
        for (NetworkType type : NetworkType.values()) {
            if (type.value.equals(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown network type: " + value);
    }
}