package com.coreclass.fileadapter.task;

import java.math.BigDecimal;

/**
 * Dictionary entry. 
 */
public class DictEntry {

    private Long id;

    private String name;

    private BigDecimal value;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public BigDecimal getValue() {
        return value;
    }

    public void setValue(BigDecimal value) {
        this.value = value;
    }
}
