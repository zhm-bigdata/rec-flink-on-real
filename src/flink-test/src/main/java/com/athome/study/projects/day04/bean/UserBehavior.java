package com.athome.study.projects.day04.bean;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserBehavior implements Serializable {
    private Long userId;
    private Long itemId;
    private Integer categoryId;
    private String behavior;
    private Long timestamp;

}
