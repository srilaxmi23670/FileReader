package com.github.srilaxmi.filereader.service;

import com.github.srilaxmi.filereader.mongo.MongoQueryService;
import com.github.srilaxmi.filereader.dto.FileRow;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.srilaxmi.filereader.constants.GlobalConstants.BATCH_SIZE;
import static com.github.srilaxmi.filereader.constants.GlobalConstants._ID;

@Component
@Slf4j
public class BatchOperationService {

    @Autowired
    private FileExtractionService fileExtractionService;

    @Autowired
    private MongoQueryService mongoQueryService;

    public Mono<Boolean> saveFileDataToCollection(
            String path, String sheetName, String collection
    ) throws IOException {

        return fileExtractionService.extractFile(path, sheetName)
                .concatMap(FileContentReader::getAllRows)
                .map(FileRow::getRow)
                .buffer(BATCH_SIZE)
                .concatMap(data -> mongoQueryService.saveMany(Mono.just(data), collection))
                .count()
                .map(dataUploaded -> {
                    log.info("{} :: Count of entries uploaded to collection :: {}", collection, dataUploaded);
                    return Boolean.TRUE;
                });
    }

    public Mono<Boolean> performBatchInsertion(String sourceCollection, String targetCollection, Boolean keepId) {

        log.info(":: Performing Batch Insertion into {} collection :: useId :: {} Total Memory :: {}, Free Memory :: {}", targetCollection, keepId, (Runtime.getRuntime().totalMemory() / (1024 * 1024)), (Runtime.getRuntime().freeMemory() / (1024 * 1024)));
        return mongoQueryService.dropCollections(List.of(targetCollection))
                .thenMany(Flux.defer(() -> {

                    log.info("Dropped collection :: {} Total Memory :: {}, Free Memory :: {}", targetCollection, (Runtime.getRuntime().totalMemory() / (1024 * 1024)), (Runtime.getRuntime().freeMemory() / (1024 * 1024)));

                    AtomicInteger counter = new AtomicInteger(1);
                    Query query = new Query();
                    query.fields().exclude(_ID);
                    Flux<Document> dataFlux = keepId ? mongoQueryService.getAll(Document.class, sourceCollection) :
                            mongoQueryService.getByQuery(query, Document.class, sourceCollection);

                    return dataFlux
                            .buffer(BATCH_SIZE)
                            .concatMap(data -> {

                                log.info("{} :: Got data with size :: {} Total Memory :: {}, Free Memory :: {}", targetCollection, data.size(), (Runtime.getRuntime().totalMemory() / (1024 * 1024)), (Runtime.getRuntime().freeMemory() / (1024 * 1024)));
                                final Integer batchNumber = counter.getAndIncrement();

                                return mongoQueryService.saveMany(Mono.just(data), targetCollection)
                                        .count()
                                        .map(insertedCount -> {
                                            log.info("{} :: inserted documents count :: {} for batch number :: {} Total Memory :: {}, Free Memory :: {}", targetCollection, insertedCount, batchNumber, (Runtime.getRuntime().totalMemory() / (1024 * 1024)), (Runtime.getRuntime().freeMemory() / (1024 * 1024)));
                                            return insertedCount;
                                        });
                            });
                }))
                .collectList()
                .flatMap(dataCount -> mongoQueryService.hasSameSizedCollections(sourceCollection, targetCollection));
    }

}
