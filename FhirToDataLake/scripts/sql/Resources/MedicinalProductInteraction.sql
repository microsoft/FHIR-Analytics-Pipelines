CREATE EXTERNAL TABLE [fhir].[MedicinalProductInteraction] (
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
    [subject] VARCHAR(MAX),
    [description] NVARCHAR(4000),
    [interactant] VARCHAR(MAX),
    [type.id] NVARCHAR(100),
    [type.extension] NVARCHAR(MAX),
    [type.coding] VARCHAR(MAX),
    [type.text] NVARCHAR(4000),
    [effect.id] NVARCHAR(100),
    [effect.extension] NVARCHAR(MAX),
    [effect.coding] VARCHAR(MAX),
    [effect.text] NVARCHAR(4000),
    [incidence.id] NVARCHAR(100),
    [incidence.extension] NVARCHAR(MAX),
    [incidence.coding] VARCHAR(MAX),
    [incidence.text] NVARCHAR(4000),
    [management.id] NVARCHAR(100),
    [management.extension] NVARCHAR(MAX),
    [management.coding] VARCHAR(MAX),
    [management.text] NVARCHAR(4000),
) WITH (
    LOCATION='/MedicinalProductInteraction/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.MedicinalProductInteractionSubject AS
SELECT
    [id],
    [subject.JSON],
    [subject.id],
    [subject.extension],
    [subject.reference],
    [subject.type],
    [subject.identifier.id],
    [subject.identifier.extension],
    [subject.identifier.use],
    [subject.identifier.type],
    [subject.identifier.system],
    [subject.identifier.value],
    [subject.identifier.period],
    [subject.identifier.assigner],
    [subject.display]
FROM openrowset (
        BULK 'MedicinalProductInteraction/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [subject.JSON]  VARCHAR(MAX) '$.subject'
    ) AS rowset
    CROSS APPLY openjson (rowset.[subject.JSON]) with (
        [subject.id]                   NVARCHAR(100)       '$.id',
        [subject.extension]            NVARCHAR(MAX)       '$.extension',
        [subject.reference]            NVARCHAR(4000)      '$.reference',
        [subject.type]                 VARCHAR(256)        '$.type',
        [subject.identifier.id]        NVARCHAR(100)       '$.identifier.id',
        [subject.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [subject.identifier.use]       NVARCHAR(64)        '$.identifier.use',
        [subject.identifier.type]      NVARCHAR(MAX)       '$.identifier.type',
        [subject.identifier.system]    VARCHAR(256)        '$.identifier.system',
        [subject.identifier.value]     NVARCHAR(4000)      '$.identifier.value',
        [subject.identifier.period]    NVARCHAR(MAX)       '$.identifier.period',
        [subject.identifier.assigner]  NVARCHAR(MAX)       '$.identifier.assigner',
        [subject.display]              NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.MedicinalProductInteractionInteractant AS
SELECT
    [id],
    [interactant.JSON],
    [interactant.id],
    [interactant.extension],
    [interactant.modifierExtension],
    [interactant.item.reference.id],
    [interactant.item.reference.extension],
    [interactant.item.reference.reference],
    [interactant.item.reference.type],
    [interactant.item.reference.identifier],
    [interactant.item.reference.display],
    [interactant.item.codeableConcept.id],
    [interactant.item.codeableConcept.extension],
    [interactant.item.codeableConcept.coding],
    [interactant.item.codeableConcept.text]
FROM openrowset (
        BULK 'MedicinalProductInteraction/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [interactant.JSON]  VARCHAR(MAX) '$.interactant'
    ) AS rowset
    CROSS APPLY openjson (rowset.[interactant.JSON]) with (
        [interactant.id]               NVARCHAR(100)       '$.id',
        [interactant.extension]        NVARCHAR(MAX)       '$.extension',
        [interactant.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [interactant.item.reference.id] NVARCHAR(100)       '$.item.reference.id',
        [interactant.item.reference.extension] NVARCHAR(MAX)       '$.item.reference.extension',
        [interactant.item.reference.reference] NVARCHAR(4000)      '$.item.reference.reference',
        [interactant.item.reference.type] VARCHAR(256)        '$.item.reference.type',
        [interactant.item.reference.identifier] NVARCHAR(MAX)       '$.item.reference.identifier',
        [interactant.item.reference.display] NVARCHAR(4000)      '$.item.reference.display',
        [interactant.item.codeableConcept.id] NVARCHAR(100)       '$.item.codeableConcept.id',
        [interactant.item.codeableConcept.extension] NVARCHAR(MAX)       '$.item.codeableConcept.extension',
        [interactant.item.codeableConcept.coding] NVARCHAR(MAX)       '$.item.codeableConcept.coding',
        [interactant.item.codeableConcept.text] NVARCHAR(4000)      '$.item.codeableConcept.text'
    ) j
