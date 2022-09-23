CREATE EXTERNAL TABLE [fhir].[ObservationDefinition] (
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
    [url] VARCHAR(256),
    [identifier.id] NVARCHAR(100),
    [identifier.extension] NVARCHAR(MAX),
    [identifier.use] NVARCHAR(64),
    [identifier.type.id] NVARCHAR(100),
    [identifier.type.extension] NVARCHAR(MAX),
    [identifier.type.coding] NVARCHAR(MAX),
    [identifier.type.text] NVARCHAR(4000),
    [identifier.system] VARCHAR(256),
    [identifier.value] NVARCHAR(4000),
    [identifier.period.id] NVARCHAR(100),
    [identifier.period.extension] NVARCHAR(MAX),
    [identifier.period.start] VARCHAR(64),
    [identifier.period.end] VARCHAR(64),
    [identifier.assigner.id] NVARCHAR(100),
    [identifier.assigner.extension] NVARCHAR(MAX),
    [identifier.assigner.reference] NVARCHAR(4000),
    [identifier.assigner.type] VARCHAR(256),
    [identifier.assigner.identifier] NVARCHAR(MAX),
    [identifier.assigner.display] NVARCHAR(4000),
    [version] NVARCHAR(100),
    [name] NVARCHAR(500),
    [title] NVARCHAR(4000),
    [status] NVARCHAR(100),
    [experimental] bit,
    [date] VARCHAR(64),
    [publisher.id] NVARCHAR(100),
    [publisher.extension] NVARCHAR(MAX),
    [publisher.reference] NVARCHAR(4000),
    [publisher.type] VARCHAR(256),
    [publisher.identifier.id] NVARCHAR(100),
    [publisher.identifier.extension] NVARCHAR(MAX),
    [publisher.identifier.use] NVARCHAR(64),
    [publisher.identifier.type] NVARCHAR(MAX),
    [publisher.identifier.system] VARCHAR(256),
    [publisher.identifier.value] NVARCHAR(4000),
    [publisher.identifier.period] NVARCHAR(MAX),
    [publisher.identifier.assigner] NVARCHAR(MAX),
    [publisher.display] NVARCHAR(4000),
    [contact] VARCHAR(MAX),
    [description] NVARCHAR(MAX),
    [useContext] VARCHAR(MAX),
    [jurisdiction] VARCHAR(MAX),
    [purpose] NVARCHAR(MAX),
    [copyright] NVARCHAR(MAX),
    [approvalDate] VARCHAR(64),
    [lastReviewDate] VARCHAR(64),
    [effectivePeriod.id] NVARCHAR(100),
    [effectivePeriod.extension] NVARCHAR(MAX),
    [effectivePeriod.start] VARCHAR(64),
    [effectivePeriod.end] VARCHAR(64),
    [derivedFromCanonical] VARCHAR(MAX),
    [derivedFromUri] VARCHAR(MAX),
    [subject] VARCHAR(MAX),
    [performerType.id] NVARCHAR(100),
    [performerType.extension] NVARCHAR(MAX),
    [performerType.coding] VARCHAR(MAX),
    [performerType.text] NVARCHAR(4000),
    [category] VARCHAR(MAX),
    [code.id] NVARCHAR(100),
    [code.extension] NVARCHAR(MAX),
    [code.coding] VARCHAR(MAX),
    [code.text] NVARCHAR(4000),
    [permittedDataType] VARCHAR(MAX),
    [multipleResultsAllowed] bit,
    [bodySite.id] NVARCHAR(100),
    [bodySite.extension] NVARCHAR(MAX),
    [bodySite.coding] VARCHAR(MAX),
    [bodySite.text] NVARCHAR(4000),
    [method.id] NVARCHAR(100),
    [method.extension] NVARCHAR(MAX),
    [method.coding] VARCHAR(MAX),
    [method.text] NVARCHAR(4000),
    [specimen] VARCHAR(MAX),
    [device] VARCHAR(MAX),
    [preferredReportName] NVARCHAR(500),
    [quantitativeDetails.id] NVARCHAR(100),
    [quantitativeDetails.extension] NVARCHAR(MAX),
    [quantitativeDetails.modifierExtension] NVARCHAR(MAX),
    [quantitativeDetails.unit.id] NVARCHAR(100),
    [quantitativeDetails.unit.extension] NVARCHAR(MAX),
    [quantitativeDetails.unit.coding] NVARCHAR(MAX),
    [quantitativeDetails.unit.text] NVARCHAR(4000),
    [quantitativeDetails.customaryUnit.id] NVARCHAR(100),
    [quantitativeDetails.customaryUnit.extension] NVARCHAR(MAX),
    [quantitativeDetails.customaryUnit.coding] NVARCHAR(MAX),
    [quantitativeDetails.customaryUnit.text] NVARCHAR(4000),
    [quantitativeDetails.conversionFactor] float,
    [quantitativeDetails.decimalPrecision] bigint,
    [qualifiedValue] VARCHAR(MAX),
    [hasMember] VARCHAR(MAX),
    [component] VARCHAR(MAX),
) WITH (
    LOCATION='/ObservationDefinition/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.ObservationDefinitionContact AS
SELECT
    [id],
    [contact.JSON],
    [contact.id],
    [contact.extension],
    [contact.name],
    [contact.telecom]
FROM openrowset (
        BULK 'ObservationDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [contact.JSON]  VARCHAR(MAX) '$.contact'
    ) AS rowset
    CROSS APPLY openjson (rowset.[contact.JSON]) with (
        [contact.id]                   NVARCHAR(100)       '$.id',
        [contact.extension]            NVARCHAR(MAX)       '$.extension',
        [contact.name]                 NVARCHAR(500)       '$.name',
        [contact.telecom]              NVARCHAR(MAX)       '$.telecom' AS JSON
    ) j

GO

CREATE VIEW fhir.ObservationDefinitionUseContext AS
SELECT
    [id],
    [useContext.JSON],
    [useContext.id],
    [useContext.extension],
    [useContext.code.id],
    [useContext.code.extension],
    [useContext.code.system],
    [useContext.code.version],
    [useContext.code.code],
    [useContext.code.display],
    [useContext.code.userSelected],
    [useContext.value.codeableConcept.id],
    [useContext.value.codeableConcept.extension],
    [useContext.value.codeableConcept.coding],
    [useContext.value.codeableConcept.text],
    [useContext.value.quantity.id],
    [useContext.value.quantity.extension],
    [useContext.value.quantity.value],
    [useContext.value.quantity.comparator],
    [useContext.value.quantity.unit],
    [useContext.value.quantity.system],
    [useContext.value.quantity.code],
    [useContext.value.range.id],
    [useContext.value.range.extension],
    [useContext.value.range.low],
    [useContext.value.range.high],
    [useContext.value.reference.id],
    [useContext.value.reference.extension],
    [useContext.value.reference.reference],
    [useContext.value.reference.type],
    [useContext.value.reference.identifier],
    [useContext.value.reference.display]
FROM openrowset (
        BULK 'ObservationDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [useContext.JSON]  VARCHAR(MAX) '$.useContext'
    ) AS rowset
    CROSS APPLY openjson (rowset.[useContext.JSON]) with (
        [useContext.id]                NVARCHAR(100)       '$.id',
        [useContext.extension]         NVARCHAR(MAX)       '$.extension',
        [useContext.code.id]           NVARCHAR(100)       '$.code.id',
        [useContext.code.extension]    NVARCHAR(MAX)       '$.code.extension',
        [useContext.code.system]       VARCHAR(256)        '$.code.system',
        [useContext.code.version]      NVARCHAR(100)       '$.code.version',
        [useContext.code.code]         NVARCHAR(4000)      '$.code.code',
        [useContext.code.display]      NVARCHAR(4000)      '$.code.display',
        [useContext.code.userSelected] bit                 '$.code.userSelected',
        [useContext.value.codeableConcept.id] NVARCHAR(100)       '$.value.codeableConcept.id',
        [useContext.value.codeableConcept.extension] NVARCHAR(MAX)       '$.value.codeableConcept.extension',
        [useContext.value.codeableConcept.coding] NVARCHAR(MAX)       '$.value.codeableConcept.coding',
        [useContext.value.codeableConcept.text] NVARCHAR(4000)      '$.value.codeableConcept.text',
        [useContext.value.quantity.id] NVARCHAR(100)       '$.value.quantity.id',
        [useContext.value.quantity.extension] NVARCHAR(MAX)       '$.value.quantity.extension',
        [useContext.value.quantity.value] float               '$.value.quantity.value',
        [useContext.value.quantity.comparator] NVARCHAR(64)        '$.value.quantity.comparator',
        [useContext.value.quantity.unit] NVARCHAR(100)       '$.value.quantity.unit',
        [useContext.value.quantity.system] VARCHAR(256)        '$.value.quantity.system',
        [useContext.value.quantity.code] NVARCHAR(4000)      '$.value.quantity.code',
        [useContext.value.range.id]    NVARCHAR(100)       '$.value.range.id',
        [useContext.value.range.extension] NVARCHAR(MAX)       '$.value.range.extension',
        [useContext.value.range.low]   NVARCHAR(MAX)       '$.value.range.low',
        [useContext.value.range.high]  NVARCHAR(MAX)       '$.value.range.high',
        [useContext.value.reference.id] NVARCHAR(100)       '$.value.reference.id',
        [useContext.value.reference.extension] NVARCHAR(MAX)       '$.value.reference.extension',
        [useContext.value.reference.reference] NVARCHAR(4000)      '$.value.reference.reference',
        [useContext.value.reference.type] VARCHAR(256)        '$.value.reference.type',
        [useContext.value.reference.identifier] NVARCHAR(MAX)       '$.value.reference.identifier',
        [useContext.value.reference.display] NVARCHAR(4000)      '$.value.reference.display'
    ) j

GO

CREATE VIEW fhir.ObservationDefinitionJurisdiction AS
SELECT
    [id],
    [jurisdiction.JSON],
    [jurisdiction.id],
    [jurisdiction.extension],
    [jurisdiction.coding],
    [jurisdiction.text]
FROM openrowset (
        BULK 'ObservationDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [jurisdiction.JSON]  VARCHAR(MAX) '$.jurisdiction'
    ) AS rowset
    CROSS APPLY openjson (rowset.[jurisdiction.JSON]) with (
        [jurisdiction.id]              NVARCHAR(100)       '$.id',
        [jurisdiction.extension]       NVARCHAR(MAX)       '$.extension',
        [jurisdiction.coding]          NVARCHAR(MAX)       '$.coding' AS JSON,
        [jurisdiction.text]            NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.ObservationDefinitionDerivedFromCanonical AS
SELECT
    [id],
    [derivedFromCanonical.JSON],
    [derivedFromCanonical]
FROM openrowset (
        BULK 'ObservationDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [derivedFromCanonical.JSON]  VARCHAR(MAX) '$.derivedFromCanonical'
    ) AS rowset
    CROSS APPLY openjson (rowset.[derivedFromCanonical.JSON]) with (
        [derivedFromCanonical]         NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.ObservationDefinitionDerivedFromUri AS
SELECT
    [id],
    [derivedFromUri.JSON],
    [derivedFromUri]
FROM openrowset (
        BULK 'ObservationDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [derivedFromUri.JSON]  VARCHAR(MAX) '$.derivedFromUri'
    ) AS rowset
    CROSS APPLY openjson (rowset.[derivedFromUri.JSON]) with (
        [derivedFromUri]               NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.ObservationDefinitionSubject AS
SELECT
    [id],
    [subject.JSON],
    [subject.id],
    [subject.extension],
    [subject.coding],
    [subject.text]
FROM openrowset (
        BULK 'ObservationDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [subject.JSON]  VARCHAR(MAX) '$.subject'
    ) AS rowset
    CROSS APPLY openjson (rowset.[subject.JSON]) with (
        [subject.id]                   NVARCHAR(100)       '$.id',
        [subject.extension]            NVARCHAR(MAX)       '$.extension',
        [subject.coding]               NVARCHAR(MAX)       '$.coding' AS JSON,
        [subject.text]                 NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.ObservationDefinitionCategory AS
SELECT
    [id],
    [category.JSON],
    [category.id],
    [category.extension],
    [category.coding],
    [category.text]
FROM openrowset (
        BULK 'ObservationDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [category.JSON]  VARCHAR(MAX) '$.category'
    ) AS rowset
    CROSS APPLY openjson (rowset.[category.JSON]) with (
        [category.id]                  NVARCHAR(100)       '$.id',
        [category.extension]           NVARCHAR(MAX)       '$.extension',
        [category.coding]              NVARCHAR(MAX)       '$.coding' AS JSON,
        [category.text]                NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.ObservationDefinitionPermittedDataType AS
SELECT
    [id],
    [permittedDataType.JSON],
    [permittedDataType]
FROM openrowset (
        BULK 'ObservationDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [permittedDataType.JSON]  VARCHAR(MAX) '$.permittedDataType'
    ) AS rowset
    CROSS APPLY openjson (rowset.[permittedDataType.JSON]) with (
        [permittedDataType]            NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.ObservationDefinitionSpecimen AS
SELECT
    [id],
    [specimen.JSON],
    [specimen.id],
    [specimen.extension],
    [specimen.reference],
    [specimen.type],
    [specimen.identifier.id],
    [specimen.identifier.extension],
    [specimen.identifier.use],
    [specimen.identifier.type],
    [specimen.identifier.system],
    [specimen.identifier.value],
    [specimen.identifier.period],
    [specimen.identifier.assigner],
    [specimen.display]
FROM openrowset (
        BULK 'ObservationDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [specimen.JSON]  VARCHAR(MAX) '$.specimen'
    ) AS rowset
    CROSS APPLY openjson (rowset.[specimen.JSON]) with (
        [specimen.id]                  NVARCHAR(100)       '$.id',
        [specimen.extension]           NVARCHAR(MAX)       '$.extension',
        [specimen.reference]           NVARCHAR(4000)      '$.reference',
        [specimen.type]                VARCHAR(256)        '$.type',
        [specimen.identifier.id]       NVARCHAR(100)       '$.identifier.id',
        [specimen.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [specimen.identifier.use]      NVARCHAR(64)        '$.identifier.use',
        [specimen.identifier.type]     NVARCHAR(MAX)       '$.identifier.type',
        [specimen.identifier.system]   VARCHAR(256)        '$.identifier.system',
        [specimen.identifier.value]    NVARCHAR(4000)      '$.identifier.value',
        [specimen.identifier.period]   NVARCHAR(MAX)       '$.identifier.period',
        [specimen.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [specimen.display]             NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ObservationDefinitionDevice AS
SELECT
    [id],
    [device.JSON],
    [device.id],
    [device.extension],
    [device.reference],
    [device.type],
    [device.identifier.id],
    [device.identifier.extension],
    [device.identifier.use],
    [device.identifier.type],
    [device.identifier.system],
    [device.identifier.value],
    [device.identifier.period],
    [device.identifier.assigner],
    [device.display]
FROM openrowset (
        BULK 'ObservationDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [device.JSON]  VARCHAR(MAX) '$.device'
    ) AS rowset
    CROSS APPLY openjson (rowset.[device.JSON]) with (
        [device.id]                    NVARCHAR(100)       '$.id',
        [device.extension]             NVARCHAR(MAX)       '$.extension',
        [device.reference]             NVARCHAR(4000)      '$.reference',
        [device.type]                  VARCHAR(256)        '$.type',
        [device.identifier.id]         NVARCHAR(100)       '$.identifier.id',
        [device.identifier.extension]  NVARCHAR(MAX)       '$.identifier.extension',
        [device.identifier.use]        NVARCHAR(64)        '$.identifier.use',
        [device.identifier.type]       NVARCHAR(MAX)       '$.identifier.type',
        [device.identifier.system]     VARCHAR(256)        '$.identifier.system',
        [device.identifier.value]      NVARCHAR(4000)      '$.identifier.value',
        [device.identifier.period]     NVARCHAR(MAX)       '$.identifier.period',
        [device.identifier.assigner]   NVARCHAR(MAX)       '$.identifier.assigner',
        [device.display]               NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ObservationDefinitionQualifiedValue AS
SELECT
    [id],
    [qualifiedValue.JSON],
    [qualifiedValue.id],
    [qualifiedValue.extension],
    [qualifiedValue.modifierExtension],
    [qualifiedValue.context.id],
    [qualifiedValue.context.extension],
    [qualifiedValue.context.coding],
    [qualifiedValue.context.text],
    [qualifiedValue.appliesTo],
    [qualifiedValue.gender],
    [qualifiedValue.age.id],
    [qualifiedValue.age.extension],
    [qualifiedValue.age.low],
    [qualifiedValue.age.high],
    [qualifiedValue.gestationalAge.id],
    [qualifiedValue.gestationalAge.extension],
    [qualifiedValue.gestationalAge.low],
    [qualifiedValue.gestationalAge.high],
    [qualifiedValue.condition],
    [qualifiedValue.rangeCategory],
    [qualifiedValue.range.id],
    [qualifiedValue.range.extension],
    [qualifiedValue.range.low],
    [qualifiedValue.range.high],
    [qualifiedValue.validCodedValueSet],
    [qualifiedValue.normalCodedValueSet],
    [qualifiedValue.abnormalCodedValueSet],
    [qualifiedValue.criticalCodedValueSet]
FROM openrowset (
        BULK 'ObservationDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [qualifiedValue.JSON]  VARCHAR(MAX) '$.qualifiedValue'
    ) AS rowset
    CROSS APPLY openjson (rowset.[qualifiedValue.JSON]) with (
        [qualifiedValue.id]            NVARCHAR(100)       '$.id',
        [qualifiedValue.extension]     NVARCHAR(MAX)       '$.extension',
        [qualifiedValue.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [qualifiedValue.context.id]    NVARCHAR(100)       '$.context.id',
        [qualifiedValue.context.extension] NVARCHAR(MAX)       '$.context.extension',
        [qualifiedValue.context.coding] NVARCHAR(MAX)       '$.context.coding',
        [qualifiedValue.context.text]  NVARCHAR(4000)      '$.context.text',
        [qualifiedValue.appliesTo]     NVARCHAR(MAX)       '$.appliesTo' AS JSON,
        [qualifiedValue.gender]        NVARCHAR(4000)      '$.gender',
        [qualifiedValue.age.id]        NVARCHAR(100)       '$.age.id',
        [qualifiedValue.age.extension] NVARCHAR(MAX)       '$.age.extension',
        [qualifiedValue.age.low]       NVARCHAR(MAX)       '$.age.low',
        [qualifiedValue.age.high]      NVARCHAR(MAX)       '$.age.high',
        [qualifiedValue.gestationalAge.id] NVARCHAR(100)       '$.gestationalAge.id',
        [qualifiedValue.gestationalAge.extension] NVARCHAR(MAX)       '$.gestationalAge.extension',
        [qualifiedValue.gestationalAge.low] NVARCHAR(MAX)       '$.gestationalAge.low',
        [qualifiedValue.gestationalAge.high] NVARCHAR(MAX)       '$.gestationalAge.high',
        [qualifiedValue.condition]     NVARCHAR(500)       '$.condition',
        [qualifiedValue.rangeCategory] NVARCHAR(4000)      '$.rangeCategory',
        [qualifiedValue.range.id]      NVARCHAR(100)       '$.range.id',
        [qualifiedValue.range.extension] NVARCHAR(MAX)       '$.range.extension',
        [qualifiedValue.range.low]     NVARCHAR(MAX)       '$.range.low',
        [qualifiedValue.range.high]    NVARCHAR(MAX)       '$.range.high',
        [qualifiedValue.validCodedValueSet] VARCHAR(256)        '$.validCodedValueSet',
        [qualifiedValue.normalCodedValueSet] VARCHAR(256)        '$.normalCodedValueSet',
        [qualifiedValue.abnormalCodedValueSet] VARCHAR(256)        '$.abnormalCodedValueSet',
        [qualifiedValue.criticalCodedValueSet] VARCHAR(256)        '$.criticalCodedValueSet'
    ) j

GO

CREATE VIEW fhir.ObservationDefinitionHasMember AS
SELECT
    [id],
    [hasMember.JSON],
    [hasMember.id],
    [hasMember.extension],
    [hasMember.reference],
    [hasMember.type],
    [hasMember.identifier.id],
    [hasMember.identifier.extension],
    [hasMember.identifier.use],
    [hasMember.identifier.type],
    [hasMember.identifier.system],
    [hasMember.identifier.value],
    [hasMember.identifier.period],
    [hasMember.identifier.assigner],
    [hasMember.display]
FROM openrowset (
        BULK 'ObservationDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [hasMember.JSON]  VARCHAR(MAX) '$.hasMember'
    ) AS rowset
    CROSS APPLY openjson (rowset.[hasMember.JSON]) with (
        [hasMember.id]                 NVARCHAR(100)       '$.id',
        [hasMember.extension]          NVARCHAR(MAX)       '$.extension',
        [hasMember.reference]          NVARCHAR(4000)      '$.reference',
        [hasMember.type]               VARCHAR(256)        '$.type',
        [hasMember.identifier.id]      NVARCHAR(100)       '$.identifier.id',
        [hasMember.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [hasMember.identifier.use]     NVARCHAR(64)        '$.identifier.use',
        [hasMember.identifier.type]    NVARCHAR(MAX)       '$.identifier.type',
        [hasMember.identifier.system]  VARCHAR(256)        '$.identifier.system',
        [hasMember.identifier.value]   NVARCHAR(4000)      '$.identifier.value',
        [hasMember.identifier.period]  NVARCHAR(MAX)       '$.identifier.period',
        [hasMember.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [hasMember.display]            NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ObservationDefinitionComponent AS
SELECT
    [id],
    [component.JSON],
    [component.id],
    [component.extension],
    [component.modifierExtension],
    [component.code.id],
    [component.code.extension],
    [component.code.coding],
    [component.code.text],
    [component.permittedDataType],
    [component.quantitativeDetails.id],
    [component.quantitativeDetails.extension],
    [component.quantitativeDetails.modifierExtension],
    [component.quantitativeDetails.unit],
    [component.quantitativeDetails.customaryUnit],
    [component.quantitativeDetails.conversionFactor],
    [component.quantitativeDetails.decimalPrecision],
    [component.qualifiedValue]
FROM openrowset (
        BULK 'ObservationDefinition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [component.JSON]  VARCHAR(MAX) '$.component'
    ) AS rowset
    CROSS APPLY openjson (rowset.[component.JSON]) with (
        [component.id]                 NVARCHAR(100)       '$.id',
        [component.extension]          NVARCHAR(MAX)       '$.extension',
        [component.modifierExtension]  NVARCHAR(MAX)       '$.modifierExtension',
        [component.code.id]            NVARCHAR(100)       '$.code.id',
        [component.code.extension]     NVARCHAR(MAX)       '$.code.extension',
        [component.code.coding]        NVARCHAR(MAX)       '$.code.coding',
        [component.code.text]          NVARCHAR(4000)      '$.code.text',
        [component.permittedDataType]  NVARCHAR(MAX)       '$.permittedDataType' AS JSON,
        [component.quantitativeDetails.id] NVARCHAR(100)       '$.quantitativeDetails.id',
        [component.quantitativeDetails.extension] NVARCHAR(MAX)       '$.quantitativeDetails.extension',
        [component.quantitativeDetails.modifierExtension] NVARCHAR(MAX)       '$.quantitativeDetails.modifierExtension',
        [component.quantitativeDetails.unit] NVARCHAR(MAX)       '$.quantitativeDetails.unit',
        [component.quantitativeDetails.customaryUnit] NVARCHAR(MAX)       '$.quantitativeDetails.customaryUnit',
        [component.quantitativeDetails.conversionFactor] float               '$.quantitativeDetails.conversionFactor',
        [component.quantitativeDetails.decimalPrecision] bigint              '$.quantitativeDetails.decimalPrecision',
        [component.qualifiedValue]     NVARCHAR(MAX)       '$.qualifiedValue' AS JSON
    ) j
