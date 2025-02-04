# Schema

## Conformance Data Model

``` mermaid
erDiagram
    BUILDINGS {
        string id PK "Provider-specific, unique identifier"
        string classification "Building classification category"
        string address "Postal street address"
        float64 height "Building height in meters from foundation to roof"
        int32 levels "Building levels/stories count"
        blob geometry "Footprint or address point geometry in well-known binary"
    }
```

## Conflation Data Model

``` mermaid
erDiagram
    CROSSREF zero or more to zero or one ENTITIES : contains
    CROSSREF zero or more to zero or one FOOTPRINTS : references
    CROSSREF {
        pluscode(13) entity_id PK
        string footprint_provider FK
        string footprint_id FK
    }
    ENTITIES {
        pluscode(13) id PK "13-length open location code identifier"
        string classification "Building classification category"
        string address "Normalized postal street address"
        float64 height "Building height in meters from foundation to roof"
        int32 levels "Building levels/stories count"
        float64 x "X-coordinate in EPSG:5070"
        float64 y "Y-coordinate in EPSG:5070"
    }
    FOOTPRINTS {
        string provider PK "Provider key"
        string id PK "Provider-specific, unique identifier"
        blob geometry "Footprint geometry in well-known binary"
    }
```
