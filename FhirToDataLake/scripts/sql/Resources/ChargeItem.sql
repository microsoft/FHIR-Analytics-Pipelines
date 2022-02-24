CREATE EXTERNAL TABLE [fhir].[ChargeItem] (
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
    [identifier] VARCHAR(MAX),
    [definitionUri] VARCHAR(MAX),
    [definitionCanonical] VARCHAR(MAX),
    [status] NVARCHAR(64),
    [partOf] VARCHAR(MAX),
    [code.id] NVARCHAR(100),
    [code.extension] NVARCHAR(MAX),
    [code.coding] VARCHAR(MAX),
    [code.text] NVARCHAR(4000),
    [subject.id] NVARCHAR(100),
    [subject.extension] NVARCHAR(MAX),
    [subject.reference] NVARCHAR(4000),
    [subject.type] VARCHAR(256),
    [subject.identifier.id] NVARCHAR(100),
    [subject.identifier.extension] NVARCHAR(MAX),
    [subject.identifier.use] NVARCHAR(64),
    [subject.identifier.type] NVARCHAR(MAX),
    [subject.identifier.system] VARCHAR(256),
    [subject.identifier.value] NVARCHAR(4000),
    [subject.identifier.period] NVARCHAR(MAX),
    [subject.identifier.assigner] NVARCHAR(MAX),
    [subject.display] NVARCHAR(4000),
    [context.id] NVARCHAR(100),
    [context.extension] NVARCHAR(MAX),
    [context.reference] NVARCHAR(4000),
    [context.type] VARCHAR(256),
    [context.identifier.id] NVARCHAR(100),
    [context.identifier.extension] NVARCHAR(MAX),
    [context.identifier.use] NVARCHAR(64),
    [context.identifier.type] NVARCHAR(MAX),
    [context.identifier.system] VARCHAR(256),
    [context.identifier.value] NVARCHAR(4000),
    [context.identifier.period] NVARCHAR(MAX),
    [context.identifier.assigner] NVARCHAR(MAX),
    [context.display] NVARCHAR(4000),
    [performer] VARCHAR(MAX),
    [performingOrganization.id] NVARCHAR(100),
    [performingOrganization.extension] NVARCHAR(MAX),
    [performingOrganization.reference] NVARCHAR(4000),
    [performingOrganization.type] VARCHAR(256),
    [performingOrganization.identifier.id] NVARCHAR(100),
    [performingOrganization.identifier.extension] NVARCHAR(MAX),
    [performingOrganization.identifier.use] NVARCHAR(64),
    [performingOrganization.identifier.type] NVARCHAR(MAX),
    [performingOrganization.identifier.system] VARCHAR(256),
    [performingOrganization.identifier.value] NVARCHAR(4000),
    [performingOrganization.identifier.period] NVARCHAR(MAX),
    [performingOrganization.identifier.assigner] NVARCHAR(MAX),
    [performingOrganization.display] NVARCHAR(4000),
    [requestingOrganization.id] NVARCHAR(100),
    [requestingOrganization.extension] NVARCHAR(MAX),
    [requestingOrganization.reference] NVARCHAR(4000),
    [requestingOrganization.type] VARCHAR(256),
    [requestingOrganization.identifier.id] NVARCHAR(100),
    [requestingOrganization.identifier.extension] NVARCHAR(MAX),
    [requestingOrganization.identifier.use] NVARCHAR(64),
    [requestingOrganization.identifier.type] NVARCHAR(MAX),
    [requestingOrganization.identifier.system] VARCHAR(256),
    [requestingOrganization.identifier.value] NVARCHAR(4000),
    [requestingOrganization.identifier.period] NVARCHAR(MAX),
    [requestingOrganization.identifier.assigner] NVARCHAR(MAX),
    [requestingOrganization.display] NVARCHAR(4000),
    [costCenter.id] NVARCHAR(100),
    [costCenter.extension] NVARCHAR(MAX),
    [costCenter.reference] NVARCHAR(4000),
    [costCenter.type] VARCHAR(256),
    [costCenter.identifier.id] NVARCHAR(100),
    [costCenter.identifier.extension] NVARCHAR(MAX),
    [costCenter.identifier.use] NVARCHAR(64),
    [costCenter.identifier.type] NVARCHAR(MAX),
    [costCenter.identifier.system] VARCHAR(256),
    [costCenter.identifier.value] NVARCHAR(4000),
    [costCenter.identifier.period] NVARCHAR(MAX),
    [costCenter.identifier.assigner] NVARCHAR(MAX),
    [costCenter.display] NVARCHAR(4000),
    [quantity.id] NVARCHAR(100),
    [quantity.extension] NVARCHAR(MAX),
    [quantity.value] float,
    [quantity.comparator] NVARCHAR(64),
    [quantity.unit] NVARCHAR(100),
    [quantity.system] VARCHAR(256),
    [quantity.code] NVARCHAR(4000),
    [bodysite] VARCHAR(MAX),
    [factorOverride] float,
    [priceOverride.id] NVARCHAR(100),
    [priceOverride.extension] NVARCHAR(MAX),
    [priceOverride.value] float,
    [priceOverride.currency] NVARCHAR(100),
    [overrideReason] NVARCHAR(4000),
    [enterer.id] NVARCHAR(100),
    [enterer.extension] NVARCHAR(MAX),
    [enterer.reference] NVARCHAR(4000),
    [enterer.type] VARCHAR(256),
    [enterer.identifier.id] NVARCHAR(100),
    [enterer.identifier.extension] NVARCHAR(MAX),
    [enterer.identifier.use] NVARCHAR(64),
    [enterer.identifier.type] NVARCHAR(MAX),
    [enterer.identifier.system] VARCHAR(256),
    [enterer.identifier.value] NVARCHAR(4000),
    [enterer.identifier.period] NVARCHAR(MAX),
    [enterer.identifier.assigner] NVARCHAR(MAX),
    [enterer.display] NVARCHAR(4000),
    [enteredDate] VARCHAR(64),
    [reason] VARCHAR(MAX),
    [service] VARCHAR(MAX),
    [account] VARCHAR(MAX),
    [note] VARCHAR(MAX),
    [supportingInformation] VARCHAR(MAX),
    [occurrence.dateTime] VARCHAR(64),
    [occurrence.period.id] NVARCHAR(100),
    [occurrence.period.extension] NVARCHAR(MAX),
    [occurrence.period.start] VARCHAR(64),
    [occurrence.period.end] VARCHAR(64),
    [occurrence.timing.id] NVARCHAR(100),
    [occurrence.timing.extension] NVARCHAR(MAX),
    [occurrence.timing.modifierExtension] NVARCHAR(MAX),
    [occurrence.timing.event] VARCHAR(MAX),
    [occurrence.timing.repeat.id] NVARCHAR(100),
    [occurrence.timing.repeat.extension] NVARCHAR(MAX),
    [occurrence.timing.repeat.modifierExtension] NVARCHAR(MAX),
    [occurrence.timing.repeat.count] bigint,
    [occurrence.timing.repeat.countMax] bigint,
    [occurrence.timing.repeat.duration] float,
    [occurrence.timing.repeat.durationMax] float,
    [occurrence.timing.repeat.durationUnit] NVARCHAR(64),
    [occurrence.timing.repeat.frequency] bigint,
    [occurrence.timing.repeat.frequencyMax] bigint,
    [occurrence.timing.repeat.period] float,
    [occurrence.timing.repeat.periodMax] float,
    [occurrence.timing.repeat.periodUnit] NVARCHAR(64),
    [occurrence.timing.repeat.dayOfWeek] NVARCHAR(MAX),
    [occurrence.timing.repeat.timeOfDay] NVARCHAR(MAX),
    [occurrence.timing.repeat.when] NVARCHAR(MAX),
    [occurrence.timing.repeat.offset] bigint,
    [occurrence.timing.repeat.bounds.duration] NVARCHAR(MAX),
    [occurrence.timing.repeat.bounds.range] NVARCHAR(MAX),
    [occurrence.timing.repeat.bounds.period] NVARCHAR(MAX),
    [occurrence.timing.code.id] NVARCHAR(100),
    [occurrence.timing.code.extension] NVARCHAR(MAX),
    [occurrence.timing.code.coding] NVARCHAR(MAX),
    [occurrence.timing.code.text] NVARCHAR(4000),
    [product.reference.id] NVARCHAR(100),
    [product.reference.extension] NVARCHAR(MAX),
    [product.reference.reference] NVARCHAR(4000),
    [product.reference.type] VARCHAR(256),
    [product.reference.identifier.id] NVARCHAR(100),
    [product.reference.identifier.extension] NVARCHAR(MAX),
    [product.reference.identifier.use] NVARCHAR(64),
    [product.reference.identifier.type] NVARCHAR(MAX),
    [product.reference.identifier.system] VARCHAR(256),
    [product.reference.identifier.value] NVARCHAR(4000),
    [product.reference.identifier.period] NVARCHAR(MAX),
    [product.reference.identifier.assigner] NVARCHAR(MAX),
    [product.reference.display] NVARCHAR(4000),
    [product.codeableConcept.id] NVARCHAR(100),
    [product.codeableConcept.extension] NVARCHAR(MAX),
    [product.codeableConcept.coding] VARCHAR(MAX),
    [product.codeableConcept.text] NVARCHAR(4000),
) WITH (
    LOCATION='/ChargeItem/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.ChargeItemIdentifier AS
SELECT
    [id],
    [identifier.JSON],
    [identifier.id],
    [identifier.extension],
    [identifier.use],
    [identifier.type.id],
    [identifier.type.extension],
    [identifier.type.coding],
    [identifier.type.text],
    [identifier.system],
    [identifier.value],
    [identifier.period.id],
    [identifier.period.extension],
    [identifier.period.start],
    [identifier.period.end],
    [identifier.assigner.id],
    [identifier.assigner.extension],
    [identifier.assigner.reference],
    [identifier.assigner.type],
    [identifier.assigner.identifier],
    [identifier.assigner.display]
FROM openrowset (
        BULK 'ChargeItem/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [identifier.JSON]  VARCHAR(MAX) '$.identifier'
    ) AS rowset
    CROSS APPLY openjson (rowset.[identifier.JSON]) with (
        [identifier.id]                NVARCHAR(100)       '$.id',
        [identifier.extension]         NVARCHAR(MAX)       '$.extension',
        [identifier.use]               NVARCHAR(64)        '$.use',
        [identifier.type.id]           NVARCHAR(100)       '$.type.id',
        [identifier.type.extension]    NVARCHAR(MAX)       '$.type.extension',
        [identifier.type.coding]       NVARCHAR(MAX)       '$.type.coding',
        [identifier.type.text]         NVARCHAR(4000)      '$.type.text',
        [identifier.system]            VARCHAR(256)        '$.system',
        [identifier.value]             NVARCHAR(4000)      '$.value',
        [identifier.period.id]         NVARCHAR(100)       '$.period.id',
        [identifier.period.extension]  NVARCHAR(MAX)       '$.period.extension',
        [identifier.period.start]      VARCHAR(64)         '$.period.start',
        [identifier.period.end]        VARCHAR(64)         '$.period.end',
        [identifier.assigner.id]       NVARCHAR(100)       '$.assigner.id',
        [identifier.assigner.extension] NVARCHAR(MAX)       '$.assigner.extension',
        [identifier.assigner.reference] NVARCHAR(4000)      '$.assigner.reference',
        [identifier.assigner.type]     VARCHAR(256)        '$.assigner.type',
        [identifier.assigner.identifier] NVARCHAR(MAX)       '$.assigner.identifier',
        [identifier.assigner.display]  NVARCHAR(4000)      '$.assigner.display'
    ) j

GO

CREATE VIEW fhir.ChargeItemDefinitionUri AS
SELECT
    [id],
    [definitionUri.JSON],
    [definitionUri]
FROM openrowset (
        BULK 'ChargeItem/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [definitionUri.JSON]  VARCHAR(MAX) '$.definitionUri'
    ) AS rowset
    CROSS APPLY openjson (rowset.[definitionUri.JSON]) with (
        [definitionUri]                NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.ChargeItemDefinitionCanonical AS
SELECT
    [id],
    [definitionCanonical.JSON],
    [definitionCanonical]
FROM openrowset (
        BULK 'ChargeItem/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [definitionCanonical.JSON]  VARCHAR(MAX) '$.definitionCanonical'
    ) AS rowset
    CROSS APPLY openjson (rowset.[definitionCanonical.JSON]) with (
        [definitionCanonical]          NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.ChargeItemPartOf AS
SELECT
    [id],
    [partOf.JSON],
    [partOf.id],
    [partOf.extension],
    [partOf.reference],
    [partOf.type],
    [partOf.identifier.id],
    [partOf.identifier.extension],
    [partOf.identifier.use],
    [partOf.identifier.type],
    [partOf.identifier.system],
    [partOf.identifier.value],
    [partOf.identifier.period],
    [partOf.identifier.assigner],
    [partOf.display]
FROM openrowset (
        BULK 'ChargeItem/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [partOf.JSON]  VARCHAR(MAX) '$.partOf'
    ) AS rowset
    CROSS APPLY openjson (rowset.[partOf.JSON]) with (
        [partOf.id]                    NVARCHAR(100)       '$.id',
        [partOf.extension]             NVARCHAR(MAX)       '$.extension',
        [partOf.reference]             NVARCHAR(4000)      '$.reference',
        [partOf.type]                  VARCHAR(256)        '$.type',
        [partOf.identifier.id]         NVARCHAR(100)       '$.identifier.id',
        [partOf.identifier.extension]  NVARCHAR(MAX)       '$.identifier.extension',
        [partOf.identifier.use]        NVARCHAR(64)        '$.identifier.use',
        [partOf.identifier.type]       NVARCHAR(MAX)       '$.identifier.type',
        [partOf.identifier.system]     VARCHAR(256)        '$.identifier.system',
        [partOf.identifier.value]      NVARCHAR(4000)      '$.identifier.value',
        [partOf.identifier.period]     NVARCHAR(MAX)       '$.identifier.period',
        [partOf.identifier.assigner]   NVARCHAR(MAX)       '$.identifier.assigner',
        [partOf.display]               NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ChargeItemPerformer AS
SELECT
    [id],
    [performer.JSON],
    [performer.id],
    [performer.extension],
    [performer.modifierExtension],
    [performer.function.id],
    [performer.function.extension],
    [performer.function.coding],
    [performer.function.text],
    [performer.actor.id],
    [performer.actor.extension],
    [performer.actor.reference],
    [performer.actor.type],
    [performer.actor.identifier],
    [performer.actor.display]
FROM openrowset (
        BULK 'ChargeItem/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [performer.JSON]  VARCHAR(MAX) '$.performer'
    ) AS rowset
    CROSS APPLY openjson (rowset.[performer.JSON]) with (
        [performer.id]                 NVARCHAR(100)       '$.id',
        [performer.extension]          NVARCHAR(MAX)       '$.extension',
        [performer.modifierExtension]  NVARCHAR(MAX)       '$.modifierExtension',
        [performer.function.id]        NVARCHAR(100)       '$.function.id',
        [performer.function.extension] NVARCHAR(MAX)       '$.function.extension',
        [performer.function.coding]    NVARCHAR(MAX)       '$.function.coding',
        [performer.function.text]      NVARCHAR(4000)      '$.function.text',
        [performer.actor.id]           NVARCHAR(100)       '$.actor.id',
        [performer.actor.extension]    NVARCHAR(MAX)       '$.actor.extension',
        [performer.actor.reference]    NVARCHAR(4000)      '$.actor.reference',
        [performer.actor.type]         VARCHAR(256)        '$.actor.type',
        [performer.actor.identifier]   NVARCHAR(MAX)       '$.actor.identifier',
        [performer.actor.display]      NVARCHAR(4000)      '$.actor.display'
    ) j

GO

CREATE VIEW fhir.ChargeItemBodysite AS
SELECT
    [id],
    [bodysite.JSON],
    [bodysite.id],
    [bodysite.extension],
    [bodysite.coding],
    [bodysite.text]
FROM openrowset (
        BULK 'ChargeItem/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [bodysite.JSON]  VARCHAR(MAX) '$.bodysite'
    ) AS rowset
    CROSS APPLY openjson (rowset.[bodysite.JSON]) with (
        [bodysite.id]                  NVARCHAR(100)       '$.id',
        [bodysite.extension]           NVARCHAR(MAX)       '$.extension',
        [bodysite.coding]              NVARCHAR(MAX)       '$.coding' AS JSON,
        [bodysite.text]                NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.ChargeItemReason AS
SELECT
    [id],
    [reason.JSON],
    [reason.id],
    [reason.extension],
    [reason.coding],
    [reason.text]
FROM openrowset (
        BULK 'ChargeItem/**',
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

CREATE VIEW fhir.ChargeItemService AS
SELECT
    [id],
    [service.JSON],
    [service.id],
    [service.extension],
    [service.reference],
    [service.type],
    [service.identifier.id],
    [service.identifier.extension],
    [service.identifier.use],
    [service.identifier.type],
    [service.identifier.system],
    [service.identifier.value],
    [service.identifier.period],
    [service.identifier.assigner],
    [service.display]
FROM openrowset (
        BULK 'ChargeItem/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [service.JSON]  VARCHAR(MAX) '$.service'
    ) AS rowset
    CROSS APPLY openjson (rowset.[service.JSON]) with (
        [service.id]                   NVARCHAR(100)       '$.id',
        [service.extension]            NVARCHAR(MAX)       '$.extension',
        [service.reference]            NVARCHAR(4000)      '$.reference',
        [service.type]                 VARCHAR(256)        '$.type',
        [service.identifier.id]        NVARCHAR(100)       '$.identifier.id',
        [service.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [service.identifier.use]       NVARCHAR(64)        '$.identifier.use',
        [service.identifier.type]      NVARCHAR(MAX)       '$.identifier.type',
        [service.identifier.system]    VARCHAR(256)        '$.identifier.system',
        [service.identifier.value]     NVARCHAR(4000)      '$.identifier.value',
        [service.identifier.period]    NVARCHAR(MAX)       '$.identifier.period',
        [service.identifier.assigner]  NVARCHAR(MAX)       '$.identifier.assigner',
        [service.display]              NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ChargeItemAccount AS
SELECT
    [id],
    [account.JSON],
    [account.id],
    [account.extension],
    [account.reference],
    [account.type],
    [account.identifier.id],
    [account.identifier.extension],
    [account.identifier.use],
    [account.identifier.type],
    [account.identifier.system],
    [account.identifier.value],
    [account.identifier.period],
    [account.identifier.assigner],
    [account.display]
FROM openrowset (
        BULK 'ChargeItem/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [account.JSON]  VARCHAR(MAX) '$.account'
    ) AS rowset
    CROSS APPLY openjson (rowset.[account.JSON]) with (
        [account.id]                   NVARCHAR(100)       '$.id',
        [account.extension]            NVARCHAR(MAX)       '$.extension',
        [account.reference]            NVARCHAR(4000)      '$.reference',
        [account.type]                 VARCHAR(256)        '$.type',
        [account.identifier.id]        NVARCHAR(100)       '$.identifier.id',
        [account.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [account.identifier.use]       NVARCHAR(64)        '$.identifier.use',
        [account.identifier.type]      NVARCHAR(MAX)       '$.identifier.type',
        [account.identifier.system]    VARCHAR(256)        '$.identifier.system',
        [account.identifier.value]     NVARCHAR(4000)      '$.identifier.value',
        [account.identifier.period]    NVARCHAR(MAX)       '$.identifier.period',
        [account.identifier.assigner]  NVARCHAR(MAX)       '$.identifier.assigner',
        [account.display]              NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ChargeItemNote AS
SELECT
    [id],
    [note.JSON],
    [note.id],
    [note.extension],
    [note.time],
    [note.text],
    [note.author.reference.id],
    [note.author.reference.extension],
    [note.author.reference.reference],
    [note.author.reference.type],
    [note.author.reference.identifier],
    [note.author.reference.display],
    [note.author.string]
FROM openrowset (
        BULK 'ChargeItem/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [note.JSON]  VARCHAR(MAX) '$.note'
    ) AS rowset
    CROSS APPLY openjson (rowset.[note.JSON]) with (
        [note.id]                      NVARCHAR(100)       '$.id',
        [note.extension]               NVARCHAR(MAX)       '$.extension',
        [note.time]                    VARCHAR(64)         '$.time',
        [note.text]                    NVARCHAR(MAX)       '$.text',
        [note.author.reference.id]     NVARCHAR(100)       '$.author.reference.id',
        [note.author.reference.extension] NVARCHAR(MAX)       '$.author.reference.extension',
        [note.author.reference.reference] NVARCHAR(4000)      '$.author.reference.reference',
        [note.author.reference.type]   VARCHAR(256)        '$.author.reference.type',
        [note.author.reference.identifier] NVARCHAR(MAX)       '$.author.reference.identifier',
        [note.author.reference.display] NVARCHAR(4000)      '$.author.reference.display',
        [note.author.string]           NVARCHAR(4000)      '$.author.string'
    ) j

GO

CREATE VIEW fhir.ChargeItemSupportingInformation AS
SELECT
    [id],
    [supportingInformation.JSON],
    [supportingInformation.id],
    [supportingInformation.extension],
    [supportingInformation.reference],
    [supportingInformation.type],
    [supportingInformation.identifier.id],
    [supportingInformation.identifier.extension],
    [supportingInformation.identifier.use],
    [supportingInformation.identifier.type],
    [supportingInformation.identifier.system],
    [supportingInformation.identifier.value],
    [supportingInformation.identifier.period],
    [supportingInformation.identifier.assigner],
    [supportingInformation.display]
FROM openrowset (
        BULK 'ChargeItem/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [supportingInformation.JSON]  VARCHAR(MAX) '$.supportingInformation'
    ) AS rowset
    CROSS APPLY openjson (rowset.[supportingInformation.JSON]) with (
        [supportingInformation.id]     NVARCHAR(100)       '$.id',
        [supportingInformation.extension] NVARCHAR(MAX)       '$.extension',
        [supportingInformation.reference] NVARCHAR(4000)      '$.reference',
        [supportingInformation.type]   VARCHAR(256)        '$.type',
        [supportingInformation.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [supportingInformation.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [supportingInformation.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [supportingInformation.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [supportingInformation.identifier.system] VARCHAR(256)        '$.identifier.system',
        [supportingInformation.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [supportingInformation.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [supportingInformation.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [supportingInformation.display] NVARCHAR(4000)      '$.display'
    ) j
