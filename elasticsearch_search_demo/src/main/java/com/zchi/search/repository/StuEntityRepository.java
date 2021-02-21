package com.zchi.search.repository;

import com.zchi.search.entity.es.StuEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.annotations.Query;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

public interface StuEntityRepository extends ElasticsearchRepository<StuEntity, Long> {
    List<StuEntity> findAllByName(String name);

    @Query("{\"match\": {\"name\": {\"query\": \"?0\"}}}")
    Page<StuEntity> findByName(String name, Pageable pageable);
}
