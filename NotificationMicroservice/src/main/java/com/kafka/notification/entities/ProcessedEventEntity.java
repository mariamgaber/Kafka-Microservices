package com.kafka.notification.entities;

import jakarta.persistence.*;
import lombok.*;


@Setter
@Getter
@Entity
@Table(name = "processed_events")
@NoArgsConstructor
@AllArgsConstructor
public class ProcessedEventEntity{
    @Id
    @GeneratedValue
    private Long id;
    @Column(nullable = false, unique = true)
    private String messageId;
    @Column(nullable = false)
    private String productId;

    public ProcessedEventEntity(String messageId, String messageKey) {
        this.messageId = messageId;
        this.productId = messageKey;
    }
}
