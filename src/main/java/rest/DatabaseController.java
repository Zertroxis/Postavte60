package rest;

import com.mongodb.BasicDBObject;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.JSONArray;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import javax.websocket.server.PathParam;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static org.springframework.util.MimeTypeUtils.APPLICATION_JSON_VALUE;

@RestController
@RequestMapping("/database")
public class DatabaseController {

    ConnectionString connection = new ConnectionString("mongodb+srv://admin:admin@etocluster.oo7t0.mongodb.net/?retryWrites=true&w=majority");
    MongoClientSettings connectionSettings = MongoClientSettings.builder().applyConnectionString(connection).build();
    MongoClient mongoDB = MongoClients.create(connectionSettings);

    @PutMapping(value = "/{database}/{collection}", consumes = APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    @ResponseBody
    public String putObjects(@PathVariable("database") String database, @PathVariable("collection") String collection, HttpEntity<String> httpEntity) {
        JSONArray array = new JSONArray(Objects.requireNonNull(httpEntity.getBody()));
        List<Document> documentsList = new ArrayList<>();
        for (Object object : array) {
            documentsList.add(Document.parse(object.toString()));
        }
        MongoCollection<Document> dbCollection = mongoDB.getDatabase(database).getCollection(collection);
        dbCollection.insertMany(documentsList);
        MongoCursor<Document> iterator = dbCollection.find().iterator();
        StringBuilder stringBuilder = new StringBuilder();
        while (iterator.hasNext()) {
            stringBuilder.append(iterator.next().toJson()).append("\n");
        }
        return stringBuilder.toString();
    }

    @GetMapping(value = "/{database}")
    public String getCollections(@PathVariable("database") String database) {
        Iterator<String> iterator = mongoDB.getDatabase(database).listCollectionNames().iterator();
        StringBuilder stringBuilder = new StringBuilder();
        while (iterator.hasNext()) {
            stringBuilder.append(iterator.next()).append("\n");
        }
        return stringBuilder.toString();
    }

    @GetMapping(value = "/{database}/{collection}", params = "name")
    public String getDocumentsByFilter(@PathVariable("database") String database, @PathVariable("collection") String collection, @PathParam("name") String name, @PathParam("value") String value) {
        Bson bson = new BasicDBObject(name, value);
        MongoCursor<Document> iterator = mongoDB.getDatabase(database).getCollection(collection).find(bson).iterator();
        StringBuilder stringBuilder = new StringBuilder();
        while (iterator.hasNext()) {
            stringBuilder.append(iterator.next().toJson()).append("\n");
        }
        return stringBuilder.toString();
    }

    @GetMapping(value = "/{database}/{collection}")
    public String getDocuments(@PathVariable("database") String database, @PathVariable("collection") String collection) {
        MongoCursor<Document> iterator = mongoDB.getDatabase(database).getCollection(collection).find().iterator();
        StringBuilder stringBuilder = new StringBuilder();
        while (iterator.hasNext()) {
            stringBuilder.append(iterator.next().toJson()).append("\n");
        }
        return stringBuilder.toString();
    }

    @PostMapping(value = "/{database}/{collection}", consumes = APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    @ResponseBody
    public String updateObjects(@PathVariable("database") String database,
                                @PathVariable("collection") String collection,
                                @RequestParam String filterName,
                                @RequestParam String filterValue,
                                @RequestParam String fieldName,
                                @RequestParam String fieldValue) {
        MongoCollection<Document> dbCollection = mongoDB.getDatabase(database).getCollection(collection);
        dbCollection.updateMany(Filters.eq(filterName, filterValue), Updates.set(fieldName, fieldValue));
        MongoCursor<Document> iterator = dbCollection.find().iterator();
        StringBuilder stringBuilder = new StringBuilder();
        while (iterator.hasNext()) {
            stringBuilder.append(iterator.next().toJson()).append("\n");
        }
        return stringBuilder.toString();
    }

    @DeleteMapping(value = "/{database}/{collection}", consumes = APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.CREATED)
    @ResponseBody
    public String deleteObjects(@PathVariable("database") String database,
                                @PathVariable("collection") String collection,
                                @RequestParam String filterName,
                                @RequestParam String filterValue) {
        MongoCollection<Document> dbCollection = mongoDB.getDatabase(database).getCollection(collection);
        dbCollection.deleteMany(Filters.regex(filterName, filterValue));
        MongoCursor<Document> iterator = dbCollection.find().iterator();
        StringBuilder stringBuilder = new StringBuilder();
        while (iterator.hasNext()) {
            stringBuilder.append(iterator.next().toJson()).append("\n");
        }
        return stringBuilder.toString();
    }

    @GetMapping
    public String getDBs() {
        Iterator<String> iterator = mongoDB.listDatabaseNames().iterator();
        StringBuilder stringBuilder = new StringBuilder();
        while (iterator.hasNext()) {
            stringBuilder.append(iterator.next()).append("\n");
        }
        return stringBuilder.toString();
    }

}