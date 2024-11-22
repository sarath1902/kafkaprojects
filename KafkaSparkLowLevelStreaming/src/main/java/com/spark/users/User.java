package com.spark.users;

import com.fasterxml.jackson.databind.JsonNode;
import org.json4s.jackson.Json;

public class User {
    private String id;
    private String name;
    private int age;
    private String created_at;
    private JsonNode address;
    private JsonNode contacts;
    private String street;
    private String city;
    private String state;
    private String email;
    private String phone;

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public JsonNode getContacts() {
        return contacts;
    }

    public void setContacts(JsonNode contacts) {
        this.contacts = contacts;
    }

    public JsonNode getAddress(){
        return address;
    }

    public void setAddress(JsonNode address) {
        this.address = address;
    }

    public String getCreated_at() {
        return created_at;
    }

    public void setCreated_at(String created_at) {
        this.created_at = created_at;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
