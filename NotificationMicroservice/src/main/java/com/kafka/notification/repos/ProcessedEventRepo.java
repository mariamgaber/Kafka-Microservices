package com.kafka.notification.repos;

import com.kafka.notification.entities.ProcessedEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProcessedEventRepo extends JpaRepository<ProcessedEventEntity, Long> {
    boolean existsByMessageId(String messageId);

}
