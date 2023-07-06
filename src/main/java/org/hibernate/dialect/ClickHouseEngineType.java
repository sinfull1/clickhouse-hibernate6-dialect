package org.hibernate.dialect;



public enum ClickHouseEngineType {

    MERGE_TREE("MergeTree");

    private String engine;
    ClickHouseEngineType(String engine) {
        this.engine = engine;
    }
}
