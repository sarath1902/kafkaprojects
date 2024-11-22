package com.spark.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spark.users.User;

public class JsonParser {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static ParsedRecord parseJson(String jsonString) throws Exception {
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



        return new ParsedRecord(user.getId(), user.getName(), user.getAge(), user.getCreated_at(), user.getStreet(), user.getCity(), user.getState(), user.getEmail(), user.getPhone());
    }

    public static class ParsedRecord {
        public final String id;
        public final String name;
        public final int age;
        public final String createdAt;
        public final String street;
        public final String city;
        public final String state;
        public final String email;
        public final String phone;

        public ParsedRecord(String id, String name, int age, String createdAt, String street, String city, String state, String email, String phone) {
            this.id = id;
            this.name = name;
            this.age = age;
            this.createdAt = createdAt;
            this.street = street;
            this.city = city;
            this.state = state;
            this.email = email;
            this.phone = phone;
        }
    }
}
