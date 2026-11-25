package com.qiujie.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.experimental.Accessors;

import java.time.LocalDateTime;

@Data
@Accessors(chain = true)
@TableName("cs_retrieval_test")
public class RetrievalTest {
    @TableId(type = IdType.AUTO)
    private Long id;
    private String queryText;
    private String params;
    private String results;
    private Integer operatorId;
    private LocalDateTime createTime;
}
