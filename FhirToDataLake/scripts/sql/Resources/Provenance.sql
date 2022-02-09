CREATE EXTERNAL TABLE [fhir].[Provenance] (
    [resourceType] NVARCHAR(4000),
    [id] VARCHAR(64),
    [meta.id] NVARCHAR(100),
    [meta.extension] NVARCHAR(MAX),
    [meta.versionId] VARCHAR(64),
    [meta.lastUpdated] VARCHAR(64),
    [meta.source] VARCHAR(256),
    [meta.profile] VARCHAR(MAX),
    [meta.security] VARCHAR(MAX),
    [meta.tag] VARCHAR(MAX),
    [implicitRules] VARCHAR(256),
    [language] NVARCHAR(100),
    [text.id] NVARCHAR(100),
    [text.extension] NVARCHAR(MAX),
    [text.status] NVARCHAR(64),
    [text.div] NVARCHAR(MAX),
    [extension] NVARCHAR(MAX),
    [modifierExtension] NVARCHAR(MAX),
    [target] VARCHAR(MAX),
    [recorded] VARCHAR(64),
    [policy] VARCHAR(MAX),
    [location.id] NVARCHAR(100),
    [location.extension] NVARCHAR(MAX),
    [location.reference] NVARCHAR(4000),
    [location.type] VARCHAR(256),
    [location.identifier.id] NVARCHAR(100),
    [location.identifier.extension] NVARCHAR(MAX),
    [location.identifier.use] NVARCHAR(64),
    [location.identifier.type] NVARCHAR(MAX),
    [location.identifier.system] VARCHAR(256),
    [location.identifier.value] NVARCHAR(4000),
    [location.identifier.period] NVARCHAR(MAX),
    [location.identifier.assigner] NVARCHAR(MAX),
    [location.display] NVARCHAR(4000),
    [reason] VARCHAR(MAX),
    [activity.id] NVARCHAR(100),
    [activity.extension] NVARCHAR(MAX),
    [activity.coding] VARCHAR(MAX),
    [activity.text] NVARCHAR(4000),
    [agent] VARCHAR(MAX),
    [entity] VARCHAR(MAX),
    [signature] VARCHAR(MAX),
    [occurred.period.id] NVARCHAR(100),
    [occurred.period.extension] NVARCHAR(MAX),
    [occurred.period.start] VARCHAR(64),
    [occurred.period.end] VARCHAR(64),
    [occurred.dateTime] VARCHAR(64),
) WITH (
    LOCATION='/Provenance/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.ProvenanceTarget AS
SELECT
    [id],
    [target.JSON],
    [target.id],
    [target.extension],
    [target.reference],
    [target.type],
    [target.identifier.id],
    [target.identifier.extension],
    [target.identifier.use],
    [target.identifier.type],
    [target.identifier.system],
    [target.identifier.value],
    [target.identifier.period],
    [target.identifier.assigner],
    [target.display]
FROM openrowset (
        BULK 'Provenance/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [target.JSON]  VARCHAR(MAX) '$.target'
    ) AS rowset
    CROSS APPLY openjson (rowset.[target.JSON]) with (
        [target.id]                    NVARCHAR(100)       '$.id',
        [target.extension]             NVARCHAR(MAX)       '$.extension',
        [target.reference]             NVARCHAR(4000)      '$.reference',
        [target.type]                  VARCHAR(256)        '$.type',
        [target.identifier.id]         NVARCHAR(100)       '$.identifier.id',
        [target.identifier.extension]  NVARCHAR(MAX)       '$.identifier.extension',
        [target.identifier.use]        NVARCHAR(64)        '$.identifier.use',
        [target.identifier.type]       NVARCHAR(MAX)       '$.identifier.type',
        [target.identifier.system]     VARCHAR(256)        '$.identifier.system',
        [target.identifier.value]      NVARCHAR(4000)      '$.identifier.value',
        [target.identifier.period]     NVARCHAR(MAX)       '$.identifier.period',
        [target.identifier.assigner]   NVARCHAR(MAX)       '$.identifier.assigner',
        [target.display]               NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ProvenancePolicy AS
SELECT
    [id],
    [policy.JSON],
    [policy]
FROM openrowset (
        BULK 'Provenance/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [policy.JSON]  VARCHAR(MAX) '$.policy'
    ) AS rowset
    CROSS APPLY openjson (rowset.[policy.JSON]) with (
        [policy]                       NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.ProvenanceReason AS
SELECT
    [id],
    [reason.JSON],
    [reason.id],
    [reason.extension],
    [reason.coding],
    [reason.text]
FROM openrowset (
        BULK 'Provenance/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [reason.JSON]  VARCHAR(MAX) '$.reason'
    ) AS rowset
    CROSS APPLY openjson (rowset.[reason.JSON]) with (
        [reason.id]                    NVARCHAR(100)       '$.id',
        [reason.extension]             NVARCHAR(MAX)       '$.extension',
        [reason.coding]                NVARCHAR(MAX)       '$.coding' AS JSON,
        [reason.text]                  NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.ProvenanceAgent AS
SELECT
    [id],
    [agent.JSON],
    [agent.id],
    [agent.extension],
    [agent.modifierExtension],
    [agent.type.id],
    [agent.type.extension],
    [agent.type.coding],
    [agent.type.text],
    [agent.role],
    [agent.who.id],
    [agent.who.extension],
    [agent.who.reference],
    [agent.who.type],
    [agent.who.identifier],
    [agent.who.display],
    [agent.onBehalfOf.id],
    [agent.onBehalfOf.extension],
    [agent.onBehalfOf.reference],
    [agent.onBehalfOf.type],
    [agent.onBehalfOf.identifier],
    [agent.onBehalfOf.display]
FROM openrowset (
        BULK 'Provenance/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [agent.JSON]  VARCHAR(MAX) '$.agent'
    ) AS rowset
    CROSS APPLY openjson (rowset.[agent.JSON]) with (
        [agent.id]                     NVARCHAR(100)       '$.id',
        [agent.extension]              NVARCHAR(MAX)       '$.extension',
        [agent.modifierExtension]      NVARCHAR(MAX)       '$.modifierExtension',
        [agent.type.id]                NVARCHAR(100)       '$.type.id',
        [agent.type.extension]         NVARCHAR(MAX)       '$.type.extension',
        [agent.type.coding]            NVARCHAR(MAX)       '$.type.coding',
        [agent.type.text]              NVARCHAR(4000)      '$.type.text',
        [agent.role]                   NVARCHAR(MAX)       '$.role' AS JSON,
        [agent.who.id]                 NVARCHAR(100)       '$.who.id',
        [agent.who.extension]          NVARCHAR(MAX)       '$.who.extension',
        [agent.who.reference]          NVARCHAR(4000)      '$.who.reference',
        [agent.who.type]               VARCHAR(256)        '$.who.type',
        [agent.who.identifier]         NVARCHAR(MAX)       '$.who.identifier',
        [agent.who.display]            NVARCHAR(4000)      '$.who.display',
        [agent.onBehalfOf.id]          NVARCHAR(100)       '$.onBehalfOf.id',
        [agent.onBehalfOf.extension]   NVARCHAR(MAX)       '$.onBehalfOf.extension',
        [agent.onBehalfOf.reference]   NVARCHAR(4000)      '$.onBehalfOf.reference',
        [agent.onBehalfOf.type]        VARCHAR(256)        '$.onBehalfOf.type',
        [agent.onBehalfOf.identifier]  NVARCHAR(MAX)       '$.onBehalfOf.identifier',
        [agent.onBehalfOf.display]     NVARCHAR(4000)      '$.onBehalfOf.display'
    ) j

GO

CREATE VIEW fhir.ProvenanceEntity AS
SELECT
    [id],
    [entity.JSON],
    [entity.id],
    [entity.extension],
    [entity.modifierExtension],
    [entity.role],
    [entity.what.id],
    [entity.what.extension],
    [entity.what.reference],
    [entity.what.type],
    [entity.what.identifier],
    [entity.what.display],
    [entity.agent]
FROM openrowset (
        BULK 'Provenance/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [entity.JSON]  VARCHAR(MAX) '$.entity'
    ) AS rowset
    CROSS APPLY openjson (rowset.[entity.JSON]) with (
        [entity.id]                    NVARCHAR(100)       '$.id',
        [entity.extension]             NVARCHAR(MAX)       '$.extension',
        [entity.modifierExtension]     NVARCHAR(MAX)       '$.modifierExtension',
        [entity.role]                  NVARCHAR(64)        '$.role',
        [entity.what.id]               NVARCHAR(100)       '$.what.id',
        [entity.what.extension]        NVARCHAR(MAX)       '$.what.extension',
        [entity.what.reference]        NVARCHAR(4000)      '$.what.reference',
        [entity.what.type]             VARCHAR(256)        '$.what.type',
        [entity.what.identifier]       NVARCHAR(MAX)       '$.what.identifier',
        [entity.what.display]          NVARCHAR(4000)      '$.what.display',
        [entity.agent]                 NVARCHAR(MAX)       '$.agent' AS JSON
    ) j

GO

CREATE VIEW fhir.ProvenanceSignature AS
SELECT
    [id],
    [signature.JSON],
    [signature.id],
    [signature.extension],
    [signature.type],
    [signature.when],
    [signature.who.id],
    [signature.who.extension],
    [signature.who.reference],
    [signature.who.type],
    [signature.who.identifier],
    [signature.who.display],
    [signature.onBehalfOf.id],
    [signature.onBehalfOf.extension],
    [signature.onBehalfOf.reference],
    [signature.onBehalfOf.type],
    [signature.onBehalfOf.identifier],
    [signature.onBehalfOf.display],
    [signature.targetFormat],
    [signature.sigFormat],
    [signature.data]
FROM openrowset (
        BULK 'Provenance/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [signature.JSON]  VARCHAR(MAX) '$.signature'
    ) AS rowset
    CROSS APPLY openjson (rowset.[signature.JSON]) with (
        [signature.id]                 NVARCHAR(100)       '$.id',
        [signature.extension]          NVARCHAR(MAX)       '$.extension',
        [signature.type]               NVARCHAR(MAX)       '$.type' AS JSON,
        [signature.when]               VARCHAR(64)         '$.when',
        [signature.who.id]             NVARCHAR(100)       '$.who.id',
        [signature.who.extension]      NVARCHAR(MAX)       '$.who.extension',
        [signature.who.reference]      NVARCHAR(4000)      '$.who.reference',
        [signature.who.type]           VARCHAR(256)        '$.who.type',
        [signature.who.identifier]     NVARCHAR(MAX)       '$.who.identifier',
        [signature.who.display]        NVARCHAR(4000)      '$.who.display',
        [signature.onBehalfOf.id]      NVARCHAR(100)       '$.onBehalfOf.id',
        [signature.onBehalfOf.extension] NVARCHAR(MAX)       '$.onBehalfOf.extension',
        [signature.onBehalfOf.reference] NVARCHAR(4000)      '$.onBehalfOf.reference',
        [signature.onBehalfOf.type]    VARCHAR(256)        '$.onBehalfOf.type',
        [signature.onBehalfOf.identifier] NVARCHAR(MAX)       '$.onBehalfOf.identifier',
        [signature.onBehalfOf.display] NVARCHAR(4000)      '$.onBehalfOf.display',
        [signature.targetFormat]       NVARCHAR(100)       '$.targetFormat',
        [signature.sigFormat]          NVARCHAR(100)       '$.sigFormat',
        [signature.data]               NVARCHAR(MAX)       '$.data'
    ) j
