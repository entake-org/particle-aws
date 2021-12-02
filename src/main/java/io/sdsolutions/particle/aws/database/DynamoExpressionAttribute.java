package io.sdsolutions.particle.aws.database;

import java.util.AbstractMap;
import java.util.Map;

public class DynamoExpressionAttribute {

    private String key;
    private String value;
    private String operator;

    public DynamoExpressionAttribute() {

    }

    public DynamoExpressionAttribute(String key, String value, String operator) {
        this.key = key;
        this.value = value;
        this.operator = operator;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public Map.Entry<String, Object> getExpressionAttributeValue() {
        return new AbstractMap.SimpleEntry<>(":" + key, value);
    }

    public String getFilterExpression() {
        return switch (operator) {
            case "contains" -> "contains(" + key + ", :" + key + ")";
            case "equals" -> key + " = :" + key;
            case "notEquals" -> key + " != :" + key;
            default -> "";
        };
    }
}
