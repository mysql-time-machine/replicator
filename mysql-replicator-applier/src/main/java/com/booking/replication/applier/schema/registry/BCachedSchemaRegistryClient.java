package com.booking.replication.applier.schema.registry;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.security.basicauth.BasicAuthCredentialProvider;
import io.confluent.kafka.schemaregistry.client.security.basicauth.BasicAuthCredentialProviderFactory;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.*;

// This class is identical to CachedSchemaRegistry except register method which uses hashmap instead of identityHashMap there by not recognising similar schemas
//        IdentityHashMap<Schema, String> myIhashmap = new IdentityHashMap<Schema, String>();
//        HashMap<Schema, String> myhashmap = new HashMap<Schema, String>();
//        mymap.put(schema, "one");
//        myhashmap.put(schema, "one");
//        Schema schema = new Schema.Parser().parse(SCHEMA_STRING);
//        Schema identicalschema = new Schema.Parser().parse(SCHEMA_STRING);

//        myIhashmap.containsKey(identicalschema) => false. Thereby marking it as cacheMiss.
//        myhashmap.containsKey(identicalschema) => true. Used in this version.

public class BCachedSchemaRegistryClient implements SchemaRegistryClient {
    private final RestService restService;
    private final int identityMapCapacity;
    private final Map<String, Map<Schema, Integer>> schemaCache;
    private final Map<String, Map<Integer, Schema>> idCache;
    private final Map<String, Map<Schema, Integer>> versionCache;
    private final Map<String, String> defaultRequestProperties = Collections.singletonMap("Content-Type", "application/vnd.schemaregistry.v1+json");

    public BCachedSchemaRegistryClient(String baseUrl, int identityMapCapacity) {
        this(new RestService(baseUrl), identityMapCapacity);
    }

    public BCachedSchemaRegistryClient(List<String> baseUrls, int identityMapCapacity) {
        this(new RestService(baseUrls), identityMapCapacity);
    }

    public BCachedSchemaRegistryClient(RestService restService, int identityMapCapacity) {
        this(restService, identityMapCapacity, null);
    }

    public BCachedSchemaRegistryClient(String baseUrl, int identityMapCapacity, Map<String, ?> originals) {
        this(new RestService(baseUrl), identityMapCapacity, originals);
    }

    public BCachedSchemaRegistryClient(List<String> baseUrls, int identityMapCapacity, Map<String, ?> originals) {
        this(new RestService(baseUrls), identityMapCapacity, originals);
    }

    public BCachedSchemaRegistryClient(RestService restService, int identityMapCapacity, Map<String, ?> configs) {
        this.identityMapCapacity = identityMapCapacity;
        this.schemaCache = new HashMap<>();
        this.idCache = new HashMap<>();
        this.versionCache = new HashMap<>();
        this.restService = restService;
        this.idCache.put(null, new HashMap<>());
        this.configureRestService(configs);
    }

    private void configureRestService(Map<String, ?> configs) {
        if (configs != null) {
            String credentialSourceConfig = (String) configs.get("basic.auth.credentials.source");
            if (credentialSourceConfig != null && !credentialSourceConfig.isEmpty()) {
                BasicAuthCredentialProvider basicAuthCredentialProvider = BasicAuthCredentialProviderFactory.getBasicAuthCredentialProvider(credentialSourceConfig, configs);
                this.restService.setBasicAuthCredentialProvider(basicAuthCredentialProvider);
            }
        }
    }

    private int registerAndGetId(String subject, Schema schema) throws IOException, RestClientException {
        return this.restService.registerSchema(schema.toString(), subject);
    }

    private Schema getSchemaByIdFromRegistry(int id) throws IOException, RestClientException {
        SchemaString restSchema = this.restService.getId(id);
        return (new Schema.Parser()).parse(restSchema.getSchemaString());
    }

    private int getVersionFromRegistry(String subject, Schema schema) throws IOException, RestClientException {
        io.confluent.kafka.schemaregistry.client.rest.entities.Schema response = this.restService.lookUpSubjectVersion(schema.toString(), subject, true);
        return response.getVersion();
    }

    private int getIdFromRegistry(String subject, Schema schema) throws IOException, RestClientException {
        io.confluent.kafka.schemaregistry.client.rest.entities.Schema response = this.restService.lookUpSubjectVersion(schema.toString(), subject, false);
        return response.getId();
    }

    public synchronized int register(String subject, Schema schema) throws IOException, RestClientException {
        Map<Schema, Integer> schemaIdMap;
        if (this.schemaCache.containsKey(subject)) {
            schemaIdMap = this.schemaCache.get(subject);
        } else {
            schemaIdMap = new HashMap<>();
            this.schemaCache.put(subject, schemaIdMap);
        }

        if (schemaIdMap.containsKey(schema)) {
            return (Integer) ((Map) schemaIdMap).get(schema);
        } else if (schemaIdMap.size() >= this.identityMapCapacity) {
            throw new IllegalStateException("Too many schema objects created for " + subject + "!");
        } else {
            int id = this.registerAndGetId(subject, schema);
            schemaIdMap.put(schema, id);
            this.idCache.get(null).put(id, schema);
            return id;
        }
    }

    public Schema getByID(int id) throws IOException, RestClientException {
        return this.getById(id);
    }

    public synchronized Schema getById(int id) throws IOException, RestClientException {
        return this.getBySubjectAndId(null, id);
    }

    public Schema getBySubjectAndID(String subject, int id) throws IOException, RestClientException {
        return this.getBySubjectAndId(subject, id);
    }

    public synchronized Schema getBySubjectAndId(String subject, int id) throws IOException, RestClientException {
        Map<Integer, Schema> idSchemaMap;
        if (this.idCache.containsKey(subject)) {
            idSchemaMap = this.idCache.get(subject);
        } else {
            idSchemaMap = new HashMap<>();
            this.idCache.put(subject, idSchemaMap);
        }

        if (idSchemaMap.containsKey(id)) {
            return (Schema) ((Map) idSchemaMap).get(id);
        } else {
            Schema schema = this.getSchemaByIdFromRegistry(id);
            idSchemaMap.put(id, schema);
            return schema;
        }
    }

    public SchemaMetadata getSchemaMetadata(String subject, int version) throws IOException, RestClientException {
        io.confluent.kafka.schemaregistry.client.rest.entities.Schema response = this.restService.getVersion(subject, version);
        int id = response.getId();
        String schema = response.getSchema();
        return new SchemaMetadata(id, version, schema);
    }

    public synchronized SchemaMetadata getLatestSchemaMetadata(String subject) throws IOException, RestClientException {
        io.confluent.kafka.schemaregistry.client.rest.entities.Schema response = this.restService.getLatestVersion(subject);
        int id = response.getId();
        int version = response.getVersion();
        String schema = response.getSchema();
        return new SchemaMetadata(id, version, schema);
    }

    public synchronized int getVersion(String subject, Schema schema) throws IOException, RestClientException {
        Map<Schema, Integer> schemaVersionMap;
        if (this.versionCache.containsKey(subject)) {
            schemaVersionMap = this.versionCache.get(subject);
        } else {
            schemaVersionMap = new IdentityHashMap<>();
            this.versionCache.put(subject, schemaVersionMap);
        }

        if (schemaVersionMap.containsKey(schema)) {
            return (Integer) ((Map) schemaVersionMap).get(schema);
        } else if (schemaVersionMap.size() >= this.identityMapCapacity) {
            throw new IllegalStateException("Too many schema objects created for " + subject + "!");
        } else {
            int version = this.getVersionFromRegistry(subject, schema);
            schemaVersionMap.put(schema, version);
            return version;
        }
    }

    public List<Integer> getAllVersions(String subject) throws IOException, RestClientException {
        return this.restService.getAllVersions(subject);
    }

    public synchronized int getId(String subject, Schema schema) throws IOException, RestClientException {
        Map<Schema, Integer> schemaIdMap;
        if (this.schemaCache.containsKey(subject)) {
            schemaIdMap = this.schemaCache.get(subject);
        } else {
            schemaIdMap = new HashMap<>();
            this.schemaCache.put(subject, schemaIdMap);
        }

        if (schemaIdMap.containsKey(schema)) {
            return (Integer) ((Map) schemaIdMap).get(schema);
        } else if (schemaIdMap.size() >= this.identityMapCapacity) {
            throw new IllegalStateException("Too many schema objects created for " + subject + "!");
        } else {
            int id = this.getIdFromRegistry(subject, schema);
            schemaIdMap.put(schema, id);
            this.idCache.get(null).put(id, schema);
            return id;
        }
    }

    public List<Integer> deleteSubject(String subject) throws IOException, RestClientException {
        return this.deleteSubject(defaultRequestProperties, subject);
    }

    public List<Integer> deleteSubject(Map<String, String> requestProperties, String subject) throws IOException, RestClientException {
        this.versionCache.remove(subject);
        this.idCache.remove(subject);
        this.schemaCache.remove(subject);
        return this.restService.deleteSubject(requestProperties, subject);
    }

    public Integer deleteSchemaVersion(String subject, String version) throws IOException, RestClientException {
        return this.deleteSchemaVersion(defaultRequestProperties, subject, version);
    }

    public Integer deleteSchemaVersion(Map<String, String> requestProperties, String subject, String version) throws IOException, RestClientException {
        ((Map) this.versionCache.get(subject)).values().remove(Integer.valueOf(version));
        return this.restService.deleteSchemaVersion(requestProperties, subject, version);
    }

    public boolean testCompatibility(String subject, Schema schema) throws IOException, RestClientException {
        return this.restService.testCompatibility(schema.toString(), subject, "latest");
    }

    public String updateCompatibility(String subject, String compatibility) throws IOException, RestClientException {
        ConfigUpdateRequest response = this.restService.updateCompatibility(compatibility, subject);
        return response.getCompatibilityLevel();
    }

    public String getCompatibility(String subject) throws IOException, RestClientException {
        Config response = this.restService.getConfig(subject);
        return response.getCompatibilityLevel();
    }

    public Collection<String> getAllSubjects() throws IOException, RestClientException {
        return this.restService.getAllSubjects();
    }
}
