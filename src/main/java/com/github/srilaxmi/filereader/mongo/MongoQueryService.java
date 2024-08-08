package com.github.srilaxmi.filereader.mongo;

import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.data.mongodb.core.query.Query;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

public interface MongoQueryService {

    <T> Flux<T> getAll(Class<T> tClass, String collection);

    Query buildQueryForPossibleIdValues(List<Object> idValues);

    <T> Flux<T> getByQuery(Query query, Class<T> tClass, String collection);

    <T> Mono<T> getFirstByQuery(Query query, Class<T> tClass, String collection);

    <T> Flux<T> getByExactIdIn(List<Object> idValues, Class<T> tClass, String collection);

    <T> Flux<T> getByIdIn(List<Object> values, Class<T> tClass, String collection);

    <T> Flux<T> getByIdsIn(List<String> values, Class<T> tClass, String collection);

    <T> Mono<T> getByExactId(Object idValue, Class<T> tClass, String collection);

    <T> Mono<T> getById(Object value, Class<T> tClass, String collection);

    Mono<Long> getCount(String collection);

    Mono<Long> getCountByQuery(Query query, String collection);

    Flux<String> getFieldsOfCollection(Boolean useOne, String collection);

    Flux<Object> getDistinctForField(String field, String collection);

    <T> Flux<T> getRandomRecords(Integer limit, Class<T> tClass, String collection);

    <T> Flux<T> getByQuery(Query query, Class<T> tClass);

    Mono<Boolean> isDocumentPresent(Query query, String collection);

    <T> Mono<T> save(T data, String collection);

    <T> Flux<T> saveMany(Mono<List<T>> dataMono, String collection);

    <T> Flux<T> updateMany(Query query, Mono<List<T>> dataMono, String collection);

    <T> Flux<T> overWriteCollection(List<T> data, String collection);

    <T> Mono<T> update(String id, T data, String collection);

    <T> Mono<T> delete(T data, String collection);

    Mono<Long> deleteByQuery(Query query, String collection);

    <T> Mono<Long> deleteByQuery(Query query, Class<T> tClass);

    <T> Flux<T> applyAggregationOperations(List<AggregationOperation> operations, Class<T> tClass, String collection);

    <T> Flux<T> copyCollection(String sourceCollection, String targetCollection, Class<T> tClass);

    Mono<Boolean> hasSameSizedCollections(String sourceCollection, String targetCollection);

    Mono<Boolean> dropCollection(String collection);

    Mono<Boolean> dropCollections(List<String> collections);

    Mono<Boolean> collectionExists(String collection);

    Mono<Boolean> collectionExists(String collection, Exception doesNotExistException);

    Mono<Boolean> renameCollection(String collectionName, String newCollectionName);

    Mono<Boolean> renameCollection(String collectionName, String newCollectionName, boolean dropTarget);

    Mono<List<IndexInfo>> getIndicesOnCollection(String collection);

    <T> Flux<T> leftOuterJoin(String leftCollection, String leftField, String rightCollection, String rightField,
                              String asField, Class<T> tClass, String outputCollection);

    <T> Flux<T> projectFields(String[] excludeFields, Map<String, String> includeWithAlias, String collection,
                              Class<T> tClass, String outputCollection);

    <T> Flux<T> unWindField(List<String> fields, String collection, Class<T> tClass, String outputCollection);

    <T> Flux<T> getDuplicatesByAggregateWithIdColumn(String idField, Class<T> tClass, String collection);

    <T> Flux<T> applyAggregation(Aggregation aggregation, Class<T> tClass, String collection);

    Mono<Boolean> appendDataToCollection(String sourceCollection, String destinationCollection);

    Mono<Long> findDistinctCountOfFieldByQuery(String field, Query query, String collection);
}