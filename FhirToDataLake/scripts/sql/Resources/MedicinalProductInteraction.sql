CREATE EXTERNAL TABLE [fhir].[MedicinalProductInteraction] (
    [resourceType] NVARCHAR(4000),
    [id] VARCHAR(64),
    [meta.id] NVARCHAR(4000),
    [meta.extension] NVARCHAR(MAX),
    [meta.versionId] VARCHAR(64),
    [meta.lastUpdated] VARCHAR(30),
    [meta.source] VARCHAR(256),
    [meta.profile] VARCHAR(MAX),
    [meta.security] VARCHAR(MAX),
    [meta.tag] VARCHAR(MAX),
    [implicitRules] VARCHAR(256),
    [language] NVARCHAR(4000),
    [text.id] NVARCHAR(4000),
    [text.extension] NVARCHAR(MAX),
    [text.status] NVARCHAR(64),
    [text.div] NVARCHAR(MAX),
    [extension] NVARCHAR(MAX),
    [modifierExtension] NVARCHAR(MAX),
    [subject] VARCHAR(MAX),
    [description] NVARCHAR(4000),
    [interactant] VARCHAR(MAX),
    [type.id] NVARCHAR(4000),
    [type.extension] NVARCHAR(MAX),
    [type.coding] VARCHAR(MAX),
    [type.text] NVARCHAR(4000),
    [effect.id] NVARCHAR(4000),
    [effect.extension] NVARCHAR(MAX),
    [effect.coding] VARCHAR(MAX),
    [effect.text] NVARCHAR(4000),
    [incidence.id] NVARCHAR(4000),
    [incidence.extension] NVARCHAR(MAX),
    [incidence.coding] VARCHAR(MAX),
    [incidence.text] NVARCHAR(4000),
    [management.id] NVARCHAR(4000),
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
        [subject.id]                   NVARCHAR(4000)      '$.id',
        [subject.extension]            NVARCHAR(MAX)       '$.extension',
        [subject.reference]            NVARCHAR(4000)      '$.reference',
        [subject.type]                 VARCHAR(256)        '$.type',
        [subject.identifier.id]        NVARCHAR(4000)      '$.identifier.id',
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
    [interactant.item.Reference.id],
    [interactant.item.Reference.extension],
    [interactant.item.Reference.reference],
    [interactant.item.Reference.type],
    [interactant.item.Reference.identifier],
    [interactant.item.Reference.display],
    [interactant.item.CodeableConcept.id],
    [interactant.item.CodeableConcept.extension],
    [interactant.item.CodeableConcept.coding],
    [interactant.item.CodeableConcept.text]
FROM openrowset (
        BULK 'MedicinalProductInteraction/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [interactant.JSON]  VARCHAR(MAX) '$.interactant'
    ) AS rowset
    CROSS APPLY openjson (rowset.[interactant.JSON]) with (
        [interactant.id]               NVARCHAR(4000)      '$.id',
        [interactant.extension]        NVARCHAR(MAX)       '$.extension',
        [interactant.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [interactant.item.Reference.id] NVARCHAR(4000)      '$.item.Reference.id',
        [interactant.item.Reference.extension] NVARCHAR(MAX)       '$.item.Reference.extension',
        [interactant.item.Reference.reference] NVARCHAR(4000)      '$.item.Reference.reference',
        [interactant.item.Reference.type] VARCHAR(256)        '$.item.Reference.type',
        [interactant.item.Reference.identifier] NVARCHAR(MAX)       '$.item.Reference.identifier',
        [interactant.item.Reference.display] NVARCHAR(4000)      '$.item.Reference.display',
        [interactant.item.CodeableConcept.id] NVARCHAR(4000)      '$.item.CodeableConcept.id',
        [interactant.item.CodeableConcept.extension] NVARCHAR(MAX)       '$.item.CodeableConcept.extension',
        [interactant.item.CodeableConcept.coding] NVARCHAR(MAX)       '$.item.CodeableConcept.coding',
        [interactant.item.CodeableConcept.text] NVARCHAR(4000)      '$.item.CodeableConcept.text'
    ) j
