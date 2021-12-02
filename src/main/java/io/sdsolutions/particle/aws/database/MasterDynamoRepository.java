package io.sdsolutions.particle.aws.database;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

public class MasterDynamoRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(MasterDynamoRepository.class);

    protected AmazonDynamoDB ddb;
    protected DynamoDB dynamoDB;
    protected ObjectMapper mapper;

    public MasterDynamoRepository(AmazonDynamoDB ddb, DynamoDB dynamoDB, ObjectMapper mapper) {
        this.ddb = ddb;
        this.dynamoDB = dynamoDB;
        this.mapper = mapper;
    }

    protected <T> T getByKey(String tableKey, String keyValue, Enum table, Class<T> clazz) throws IOException {
        T t = null;
        HashMap<String, AttributeValue> queryKey = new HashMap<>();
        queryKey.put(tableKey, new AttributeValue(keyValue));

        GetItemRequest request = new GetItemRequest()
                .withKey(queryKey)
                .withTableName(table.toString());

        try {
            Map<String, AttributeValue> returnedItem = ddb.getItem(request).getItem();
            if (returnedItem != null) {
                List<Map<String, AttributeValue>> mapList = new ArrayList<>();
                mapList.add(returnedItem);

                t = mapper.readValue(mapper.writeValueAsString(ItemUtils.toItemList(mapList).get(0).asMap()), clazz);
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new IOException(e);
        }

        return t;
    }

    protected <T> List<T> getListByKey(String tableKey, String keyValue, Enum table, Class<T> clazz) throws IOException {
        List<T> t = new ArrayList<>();

        Table ddTable = dynamoDB.getTable(table.toString());
        Index ddIndex = ddTable.getIndex(tableKey + "-index");

        QuerySpec spec = new QuerySpec()
                .withKeyConditionExpression(tableKey + " = :v_id")
                .withValueMap(new ValueMap()
                        .withString(":v_id", keyValue));

        ItemCollection<QueryOutcome> items = ddIndex.query(spec);

        Iterator<Item> iterator = items.iterator();
        Item item;
        while (iterator.hasNext()) {
            item = iterator.next();
            t.add(mapper.readValue(item.toJSONPretty(), clazz));
        }

        return t;
    }

    protected void addEntry(Enum table, Object object) throws IOException {
        Map<String, AttributeValue> itemValues =
                ItemUtils.toAttributeValues(
                        new Item().withJSON("document", mapper.writeValueAsString(nullifyStrings(object)))
                ).get("document").getM();
        ddb.putItem(table.toString(), itemValues);
    }

    private Object nullifyStrings(Object o) {
        Field[] fields;
        try {
            fields = FieldUtils.getAllFields(o.getClass());
            for (Field field : fields) {
                field.setAccessible(true);
                Object val = field.get(o);
                if (val != null && val.getClass() == String.class && StringUtils.isBlank((String) val)) {
                    field.set(o, null);
                }
            }
        } catch (IllegalAccessException e) {
            LOGGER.debug(e.getMessage(), e);
        }

        return o;
    }

    protected void deleteEntry(String tableKey, String keyValue, Enum table) {
        HashMap<String, AttributeValue> queryKey = new HashMap<>();
        queryKey.put(tableKey, new AttributeValue(keyValue));
        ddb.deleteItem(table.toString(), queryKey);
    }

    protected <T> List<T> getListByExpression(Enum table, DynamoFilterExpressionBuilder builder, Class<T>  clazz) throws IOException {
        List<T> t = new ArrayList<>();

        Table ddTable = dynamoDB.getTable(table.toString());

        Map<String, Object> expressionAttributeValues = new HashMap<>();
        for(DynamoExpressionAttribute expression: builder.attributes()) {
            expressionAttributeValues.put(expression.getExpressionAttributeValue().getKey(), expression.getExpressionAttributeValue().getValue());
        }

        String expression = builder.build();

        LOGGER.trace(expression);
        LOGGER.trace(expressionAttributeValues.toString());

        ItemCollection<ScanOutcome> items = ddTable.scan(expression, // FilterExpression
                null, // ProjectionExpression
                null, // ExpressionAttributeNames - not used in this example
                expressionAttributeValues);

        for (Item item : items) {
            t.add(mapper.readValue(item.toJSONPretty(), clazz));
        }

        return t;
    }

}
