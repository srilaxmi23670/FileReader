package com.github.srilaxmi.filereader.mongo;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.srilaxmi.filereader.util.DataTypeConversionUtil;
import com.github.srilaxmi.filereader.util.DataTypeUtil;
import com.mongodb.ConnectionString;
import com.mongodb.MongoNamespace;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.reactivestreams.client.MongoCollection;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.srilaxmi.filereader.constants.GlobalConstants.BATCH_SIZE;
import static com.github.srilaxmi.filereader.constants.GlobalConstants._ID;


@Component
@Slf4j
public class MongoQueryServiceImpl implements MongoQueryService {

    private final static Integer MAX_TIME = 300;
    private final static ObjectMapper objectMapper = new ObjectMapper();
    public final static AggregationOptions aggregationOptions = AggregationOptions
            .builder()
            .maxTime(Duration.ofSeconds(MAX_TIME))
            .allowDiskUse(true)
            .build();
    public static final String DUP_KEY_WITH_BRACES_REGEX = "dup key: \\{([^}]*)\\}";

    @Value("${spring.data.mongodb.uri}")
    private String MONGO_DB_URI;

    @Autowired
    private ReactiveMongoOperations reactiveMongoOperations;

    @Override
    public <T> Flux<T> getAll(Class<T> tClass, String collection) {
        return reactiveMongoOperations.findAll(tClass, collection);
    }

    @Override
    public <T> Flux<T> getByQuery(Query query, Class<T> tClass, String collection) {
        return reactiveMongoOperations.find(query, tClass, collection);
    }

    @Override
    public <T> Mono<T> getFirstByQuery(Query query, Class<T> tClass, String collection) {
        return reactiveMongoOperations.find(query.limit(1), tClass, collection)
                .take(1)
                .singleOrEmpty();
    }

    /**
     * Returns record based on _id field and if no record found calls getByIdField method
     */

    @Override
    public <T> Mono<T> getById(Object value, Class<T> tClass, String collection) {

        if (Objects.isNull(value)) {
            return Mono.empty();
        }

        Query query = buildQueryForPossibleIdValues(List.of(value));
        return getFirstByQuery(query, tClass, collection);
    }

    @Override
    public <T> Flux<T> getByIdIn(List<Object> values, Class<T> tClass, String collection) {

        if (CollectionUtils.isEmpty(values)) {
            return Flux.empty();
        }

        Query query = buildQueryForPossibleIdValues(values);
        return getByQuery(query, tClass, collection);
    }

    @Override
    public <T> Flux<T> getByIdsIn(List<String> values, Class<T> tClass, String collection) {

        if (CollectionUtils.isEmpty(values)) {
            return Flux.empty();
        }

        Query query = buildQueryForPossibleStringIdValues(values);
        return getByQuery(query, tClass, collection);
    }

    @Override
    public <T> Mono<T> getByExactId(Object idValue, Class<T> tClass, String collection) {
        return reactiveMongoOperations.findById(idValue, tClass, collection);
    }

    @Override
    public <T> Flux<T> getByExactIdIn(List<Object> idValues, Class<T> tClass, String collection) {

        if (CollectionUtils.isEmpty(idValues)) {
            return Flux.empty();
        }

        Query query = new Query();
        query.addCriteria(Criteria.where(_ID).in(idValues));
        return getByQuery(query, tClass, collection);
    }


    @Override
    public <T> Flux<T> applyAggregationOperations(List<AggregationOperation> operations, Class<T> tClass, String collection) {

        Aggregation aggregation = Aggregation.newAggregation(
                operations
        ).withOptions(aggregationOptions);

        log.info("{} :: Aggregator Query on collection : {}", collection, aggregation);
        return reactiveMongoOperations.aggregate(aggregation, collection, tClass);

    }

    @Override
    public <T> Flux<T> copyCollection(String sourceCollection, String targetCollection, Class<T> tClass) {

        OutOperation outOperation = Aggregation.out(targetCollection);
        Aggregation aggregation = Aggregation
                .newAggregation(outOperation)
                .withOptions(MongoQueryServiceImpl.aggregationOptions);
        return applyAggregation(aggregation, tClass, sourceCollection);
    }

    @Override
    public Mono<Long> getCount(String collection) {
        return getCountByQuery(new Query(), collection);
    }

    @Override
    public Mono<Long> getCountByQuery(Query query, String collection) {
        return reactiveMongoOperations.count(query, collection);
    }

    @Override
    public <T> Mono<T> save(T data, String collection) {

        return reactiveMongoOperations.save(data, collection);
    }

    @Override
    public <T> Mono<T> update(String id, T data, String collection) {
        Query query = buildQueryForPossibleIdValues(List.of(id));
        return deleteByQuery(query, collection)
                .flatMap(deleteResult -> save(data, collection));
    }

    @Override
    public <T> Mono<T> delete(T data, String collection) {
        return reactiveMongoOperations.remove(data, collection)
                .map(deleteResult -> data);
    }

    @Override
    public Mono<Long> deleteByQuery(Query query, String collection) {
        return reactiveMongoOperations.remove(query, collection)
                .map(DeleteResult::getDeletedCount);
    }

    @Override
    public <T> Mono<Long> deleteByQuery(Query query, Class<T> tClass) {
        return reactiveMongoOperations.remove(query, tClass)
                .map(DeleteResult::getDeletedCount);
    }

    @Override
    public Mono<Boolean> hasSameSizedCollections(String sourceCollection, String targetCollection) {

        Mono<Long> sourceCollectionSize = getCountByQuery(new Query(), sourceCollection);
        Mono<Long> targetCollectionSize = getCountByQuery(new Query(), targetCollection);

        return Mono.zip(sourceCollectionSize, targetCollectionSize)
                .map(result -> result.getT1().equals(result.getT2()));
    }

    @Override
    public Mono<Boolean> dropCollection(String collection) {

        return reactiveMongoOperations.dropCollection(collection)
                .thenReturn(Boolean.TRUE);
    }

    @Override
    public Mono<Boolean> dropCollections(List<String> collections) {

        return Mono.defer(() -> {
            Mono<Void> dropMono = Mono.empty();

            for (String collection : collections) {
                if (StringUtils.isNotBlank(collection)) {
                    dropMono = dropMono.then(reactiveMongoOperations.dropCollection(collection));
                }
            }

            return dropMono.thenReturn(Boolean.TRUE);
        });
    }


    @Override
    public Mono<Boolean> collectionExists(String collection) {
        return reactiveMongoOperations.collectionExists(collection);
    }

    @Override
    public Mono<Boolean> collectionExists(String collection, Exception doesNotExistException) {
        return collectionExists(collection)
                .flatMap(exists -> {
                    if (!exists) return Mono.error(doesNotExistException);
                    return Mono.just(exists);
                });
    }

    @Override
    public Mono<Boolean> renameCollection(String collectionName, String newCollectionName) {
        return renameCollection(collectionName, newCollectionName, false);
    }

    @Override
    public Mono<Boolean> renameCollection(String collectionName, String newCollectionName, boolean dropTarget) {

        return getCollection(collectionName)
                .flatMap(data -> {

                    ConnectionString connectionString = new ConnectionString(MONGO_DB_URI);
                    String databaseName = connectionString.getDatabase();
                    MongoNamespace mongoNameSpace = new MongoNamespace(Objects.requireNonNull(databaseName), newCollectionName);
                    RenameCollectionOptions options = new RenameCollectionOptions();
                    if (dropTarget) {
                        options.dropTarget(true);
                    }
                    return Mono.from(data.renameCollection(mongoNameSpace, options))
                            .thenReturn(Boolean.TRUE);
                })
                .switchIfEmpty(Mono.just(Boolean.TRUE));
    }

    @Override
    public Mono<List<IndexInfo>> getIndicesOnCollection(String collection) {
        return reactiveMongoOperations.indexOps(collection).getIndexInfo().collectList();
    }

    @Override
    public <T> Flux<T> getByQuery(Query query, Class<T> tClass) {
        return reactiveMongoOperations.find(query, tClass);
    }

    @Override
    public Flux<String> getFieldsOfCollection(Boolean useOne, String collection) {

        if (useOne) {
            return reactiveMongoOperations.findOne(new Query(), Document.class, collection)
                    .flatMapMany(data -> Flux.fromIterable(data.keySet()));
        } else {

            Aggregation aggregation = Aggregation.newAggregation(
                    Aggregation.project().and(ObjectOperators.ObjectToArray.toArray("$$ROOT")).as("fields"),
                    Aggregation.unwind("fields"),
                    Aggregation.group().addToSet("fields.k").as("fieldNames")
            ).withOptions(aggregationOptions);

            return reactiveMongoOperations.aggregate(aggregation, collection, Document.class)
                    .take(1, true)
                    .flatMap(data -> {
                        Object value = data.get("fieldNames");
                        List<String> fields = objectMapper.convertValue(value, new TypeReference<List<String>>() {
                        });

                        return Flux.fromIterable(fields);
                    });
        }
    }

    @Override
    public Flux<Object> getDistinctForField(String field, String collection) {
        return reactiveMongoOperations.findDistinct(new Query(), field, collection, Object.class);
    }

    @Override
    public <T> Flux<T> getRandomRecords(Integer limit, Class<T> tClass, String collection) {

        SampleOperation sampleOperation = Aggregation.sample(limit);
        Aggregation aggregation = Aggregation.newAggregation(sampleOperation)
                .withOptions(aggregationOptions);
        return reactiveMongoOperations.aggregate(aggregation, collection, tClass);
    }

    @Override
    public Mono<Boolean> isDocumentPresent(Query query, String collection) {
        return reactiveMongoOperations.exists(query, collection);
    }

    @Override
    public <T> Flux<T> saveMany(Mono<List<T>> dataMono, String collection) {
        return reactiveMongoOperations.insertAll(dataMono, collection);
    }

    @Override
    public <T> Flux<T> updateMany(Query query, Mono<List<T>> dataMono, String collection) {

        return deleteByQuery(query, collection)
                .flatMapMany(deleteResult -> saveMany(dataMono, collection));
    }

    @Override
    public <T> Flux<T> overWriteCollection(List<T> data, String collection) {

        return dropCollections(List.of(collection))
                .flatMapMany(deleted -> {
                    return saveMany(Mono.just(data), collection);
                });
    }

    @Override
    public <T> Flux<T> leftOuterJoin(String leftCollection, String leftField, String rightCollection, String rightField,
                                     String asField, Class<T> tClass, String outputCollection) {

        LookupOperation lookupOperation = Aggregation.lookup(rightCollection, leftField, rightField, asField);
        OutOperation outOperation = Aggregation.out(outputCollection);

        Aggregation aggregation = Aggregation.newAggregation(
                lookupOperation,
                outOperation
        ).withOptions(aggregationOptions);

        return reactiveMongoOperations.aggregate(aggregation, leftCollection, tClass);
    }

    @Override
    public <T> Flux<T> projectFields(String[] excludeFields, Map<String, String> includeWithAlias, String collection,
                                     Class<T> tClass, String outputCollection) {

        ProjectionOperation projectionOperation = Aggregation.project();

        if (Objects.nonNull(excludeFields)) {
            projectionOperation = projectionOperation.andExclude(excludeFields);
        }

        for (Map.Entry<String, String> entry : includeWithAlias.entrySet()) {
            projectionOperation = projectionOperation.and(entry.getValue()).as(entry.getKey());
        }

        OutOperation outOperation = Aggregation.out(outputCollection);
        Aggregation aggregation = Aggregation.newAggregation(
                projectionOperation,
                outOperation
        ).withOptions(aggregationOptions);

        return reactiveMongoOperations.aggregate(aggregation, collection, tClass);
    }

    @Override
    public <T> Flux<T> unWindField(List<String> fields, String collection, Class<T> tClass, String outputCollection) {

        List<AggregationOperation> aggregationOperations = new ArrayList<>();
        UnsetOperation unsetOperation = UnsetOperation.unset(_ID);
        aggregationOperations.add(unsetOperation);
        for (String field : fields) {
            aggregationOperations.add(Aggregation.unwind(field, true));
        }
        aggregationOperations.add(Aggregation.out(outputCollection));
        Aggregation aggregation = Aggregation.newAggregation(
                aggregationOperations
        ).withOptions(aggregationOptions);

        return reactiveMongoOperations.aggregate(aggregation, collection, tClass);
    }

    @Override
    public <T> Flux<T> getDuplicatesByAggregateWithIdColumn(String idField, Class<T> tClass, String collection) {
        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.group(idField).push(Aggregation.ROOT).as("documents"),
                Aggregation.match(Criteria.where("$expr").is(new Document("$gt", Arrays.asList(new Document("$size", "$documents"), 1L))))
        );
        return reactiveMongoOperations.aggregate(aggregation, collection, tClass);
    }

    @Override
    public <T> Flux<T> applyAggregation(Aggregation aggregation, Class<T> tClass, String collection) {

        log.info("{} :: Aggregator Query on collection : {}", collection, aggregation);
        return reactiveMongoOperations.aggregate(aggregation, collection, tClass);

    }

    @Override
    public Mono<Boolean> appendDataToCollection(String sourceCollection, String destinationCollection) {

        AtomicInteger batchNo = new AtomicInteger(0);
        return getAll(Document.class, sourceCollection)
                .buffer(BATCH_SIZE)
                .concatMap(data -> {
                    batchNo.incrementAndGet();
                    log.info("Batch {} :: Appending the data of {} collection to {} collection", batchNo.get(), sourceCollection, destinationCollection);
                    return saveMany(Mono.just(data), destinationCollection);
                })
                .count()
                .thenReturn(Boolean.TRUE);
    }

    @Override
    public Mono<Long> findDistinctCountOfFieldByQuery(String field, Query query, String collection) {
        return reactiveMongoOperations.findDistinct(query, field, collection, Object.class).count();
    }

    private Mono<MongoCollection<Document>> getCollection(String collection) {

        return collectionExists(collection)
                .filter(exists -> Objects.equals(exists, Boolean.TRUE))
                .flatMap(exists -> reactiveMongoOperations.getCollection(collection));
    }

    public Query buildQueryForPossibleIdValues(List<Object> idValues) {

        List<Object> possibleIdValues = new ArrayList<>();

        idValues.forEach(idValue -> possibleIdValues.addAll(getPossibleIdValues(idValue)));

        Query query = new Query();
        query.addCriteria(new Criteria().orOperator(
                Criteria.where(_ID).in(possibleIdValues)
        ));
        return query;
    }

    public Query buildQueryForPossibleStringIdValues(List<String> idValues) {

        List<Object> possibleIdValues = new ArrayList<>();

        idValues.forEach(idValue -> possibleIdValues.addAll(getPossibleIdValues(idValue)));

        Query query = new Query();
        query.addCriteria(new Criteria().orOperator(
                Criteria.where(_ID).in(possibleIdValues)
        ));
        return query;
    }

    private List<Object> getPossibleIdValues(Object value) {

        return DataTypeUtil.isValueIntegerType(value, Boolean.FALSE) ?
                List.of(DataTypeConversionUtil.convertToInteger(value), value.toString(), value) :
                List.of(value, value.toString());
    }

}
