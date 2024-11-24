package com.spark.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spark.users.User;

public class JsonParser {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static User parseJson(String jsonString) throws JsonProcessingException {
        JsonNode jsonNode = objectMapper.readTree(jsonString);
        User user = new User();
        user.setId(jsonNode.get("id").asText());
        user.setName(jsonNode.get("name").asText());
        user.setAge(jsonNode.get("age").asInt());
        user.setCreated_at(jsonNode.get("created_at").asText());
        user.setAddress(jsonNode.get("address"));
        user.setContacts(jsonNode.get("contacts"));
        user.setStreet(user.getAddress().get("street").asText());
        user.setCity(user.getAddress().get("city").asText());
        user.setState(user.getAddress().get("state").asText());
        user.setPhone(user.getContacts().get("phone").asText());
        user.setEmail(user.getContacts().get("email").asText());
        return user;
    }
}
