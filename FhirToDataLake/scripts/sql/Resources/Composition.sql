CREATE EXTERNAL TABLE [fhir].[Composition] (
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
    [status] NVARCHAR(64),
    [type.id] NVARCHAR(100),
    [type.extension] NVARCHAR(MAX),
    [type.coding] VARCHAR(MAX),
    [type.text] NVARCHAR(4000),
    [category] VARCHAR(MAX),
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
    [encounter.id] NVARCHAR(100),
    [encounter.extension] NVARCHAR(MAX),
    [encounter.reference] NVARCHAR(4000),
    [encounter.type] VARCHAR(256),
    [encounter.identifier.id] NVARCHAR(100),
    [encounter.identifier.extension] NVARCHAR(MAX),
    [encounter.identifier.use] NVARCHAR(64),
    [encounter.identifier.type] NVARCHAR(MAX),
    [encounter.identifier.system] VARCHAR(256),
    [encounter.identifier.value] NVARCHAR(4000),
    [encounter.identifier.period] NVARCHAR(MAX),
    [encounter.identifier.assigner] NVARCHAR(MAX),
    [encounter.display] NVARCHAR(4000),
    [date] VARCHAR(64),
    [author] VARCHAR(MAX),
    [title] NVARCHAR(4000),
    [confidentiality] NVARCHAR(100),
    [attester] VARCHAR(MAX),
    [custodian.id] NVARCHAR(100),
    [custodian.extension] NVARCHAR(MAX),
    [custodian.reference] NVARCHAR(4000),
    [custodian.type] VARCHAR(256),
    [custodian.identifier.id] NVARCHAR(100),
    [custodian.identifier.extension] NVARCHAR(MAX),
    [custodian.identifier.use] NVARCHAR(64),
    [custodian.identifier.type] NVARCHAR(MAX),
    [custodian.identifier.system] VARCHAR(256),
    [custodian.identifier.value] NVARCHAR(4000),
    [custodian.identifier.period] NVARCHAR(MAX),
    [custodian.identifier.assigner] NVARCHAR(MAX),
    [custodian.display] NVARCHAR(4000),
    [relatesTo] VARCHAR(MAX),
    [event] VARCHAR(MAX),
    [section] VARCHAR(MAX),
) WITH (
    LOCATION='/Composition/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.CompositionCategory AS
SELECT
    [id],
    [category.JSON],
    [category.id],
    [category.extension],
    [category.coding],
    [category.text]
FROM openrowset (
        BULK 'Composition/**',
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

CREATE VIEW fhir.CompositionAuthor AS
SELECT
    [id],
    [author.JSON],
    [author.id],
    [author.extension],
    [author.reference],
    [author.type],
    [author.identifier.id],
    [author.identifier.extension],
    [author.identifier.use],
    [author.identifier.type],
    [author.identifier.system],
    [author.identifier.value],
    [author.identifier.period],
    [author.identifier.assigner],
    [author.display]
FROM openrowset (
        BULK 'Composition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [author.JSON]  VARCHAR(MAX) '$.author'
    ) AS rowset
    CROSS APPLY openjson (rowset.[author.JSON]) with (
        [author.id]                    NVARCHAR(100)       '$.id',
        [author.extension]             NVARCHAR(MAX)       '$.extension',
        [author.reference]             NVARCHAR(4000)      '$.reference',
        [author.type]                  VARCHAR(256)        '$.type',
        [author.identifier.id]         NVARCHAR(100)       '$.identifier.id',
        [author.identifier.extension]  NVARCHAR(MAX)       '$.identifier.extension',
        [author.identifier.use]        NVARCHAR(64)        '$.identifier.use',
        [author.identifier.type]       NVARCHAR(MAX)       '$.identifier.type',
        [author.identifier.system]     VARCHAR(256)        '$.identifier.system',
        [author.identifier.value]      NVARCHAR(4000)      '$.identifier.value',
        [author.identifier.period]     NVARCHAR(MAX)       '$.identifier.period',
        [author.identifier.assigner]   NVARCHAR(MAX)       '$.identifier.assigner',
        [author.display]               NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.CompositionAttester AS
SELECT
    [id],
    [attester.JSON],
    [attester.id],
    [attester.extension],
    [attester.modifierExtension],
    [attester.mode],
    [attester.time],
    [attester.party.id],
    [attester.party.extension],
    [attester.party.reference],
    [attester.party.type],
    [attester.party.identifier],
    [attester.party.display]
FROM openrowset (
        BULK 'Composition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [attester.JSON]  VARCHAR(MAX) '$.attester'
    ) AS rowset
    CROSS APPLY openjson (rowset.[attester.JSON]) with (
        [attester.id]                  NVARCHAR(100)       '$.id',
        [attester.extension]           NVARCHAR(MAX)       '$.extension',
        [attester.modifierExtension]   NVARCHAR(MAX)       '$.modifierExtension',
        [attester.mode]                NVARCHAR(64)        '$.mode',
        [attester.time]                VARCHAR(64)         '$.time',
        [attester.party.id]            NVARCHAR(100)       '$.party.id',
        [attester.party.extension]     NVARCHAR(MAX)       '$.party.extension',
        [attester.party.reference]     NVARCHAR(4000)      '$.party.reference',
        [attester.party.type]          VARCHAR(256)        '$.party.type',
        [attester.party.identifier]    NVARCHAR(MAX)       '$.party.identifier',
        [attester.party.display]       NVARCHAR(4000)      '$.party.display'
    ) j

GO

CREATE VIEW fhir.CompositionRelatesTo AS
SELECT
    [id],
    [relatesTo.JSON],
    [relatesTo.id],
    [relatesTo.extension],
    [relatesTo.modifierExtension],
    [relatesTo.code],
    [relatesTo.target.identifier.id],
    [relatesTo.target.identifier.extension],
    [relatesTo.target.identifier.use],
    [relatesTo.target.identifier.type],
    [relatesTo.target.identifier.system],
    [relatesTo.target.identifier.value],
    [relatesTo.target.identifier.period],
    [relatesTo.target.identifier.assigner],
    [relatesTo.target.reference.id],
    [relatesTo.target.reference.extension],
    [relatesTo.target.reference.reference],
    [relatesTo.target.reference.type],
    [relatesTo.target.reference.identifier],
    [relatesTo.target.reference.display]
FROM openrowset (
        BULK 'Composition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [relatesTo.JSON]  VARCHAR(MAX) '$.relatesTo'
    ) AS rowset
    CROSS APPLY openjson (rowset.[relatesTo.JSON]) with (
        [relatesTo.id]                 NVARCHAR(100)       '$.id',
        [relatesTo.extension]          NVARCHAR(MAX)       '$.extension',
        [relatesTo.modifierExtension]  NVARCHAR(MAX)       '$.modifierExtension',
        [relatesTo.code]               NVARCHAR(4000)      '$.code',
        [relatesTo.target.identifier.id] NVARCHAR(100)       '$.target.identifier.id',
        [relatesTo.target.identifier.extension] NVARCHAR(MAX)       '$.target.identifier.extension',
        [relatesTo.target.identifier.use] NVARCHAR(64)        '$.target.identifier.use',
        [relatesTo.target.identifier.type] NVARCHAR(MAX)       '$.target.identifier.type',
        [relatesTo.target.identifier.system] VARCHAR(256)        '$.target.identifier.system',
        [relatesTo.target.identifier.value] NVARCHAR(4000)      '$.target.identifier.value',
        [relatesTo.target.identifier.period] NVARCHAR(MAX)       '$.target.identifier.period',
        [relatesTo.target.identifier.assigner] NVARCHAR(MAX)       '$.target.identifier.assigner',
        [relatesTo.target.reference.id] NVARCHAR(100)       '$.target.reference.id',
        [relatesTo.target.reference.extension] NVARCHAR(MAX)       '$.target.reference.extension',
        [relatesTo.target.reference.reference] NVARCHAR(4000)      '$.target.reference.reference',
        [relatesTo.target.reference.type] VARCHAR(256)        '$.target.reference.type',
        [relatesTo.target.reference.identifier] NVARCHAR(MAX)       '$.target.reference.identifier',
        [relatesTo.target.reference.display] NVARCHAR(4000)      '$.target.reference.display'
    ) j

GO

CREATE VIEW fhir.CompositionEvent AS
SELECT
    [id],
    [event.JSON],
    [event.id],
    [event.extension],
    [event.modifierExtension],
    [event.code],
    [event.period.id],
    [event.period.extension],
    [event.period.start],
    [event.period.end],
    [event.detail]
FROM openrowset (
        BULK 'Composition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [event.JSON]  VARCHAR(MAX) '$.event'
    ) AS rowset
    CROSS APPLY openjson (rowset.[event.JSON]) with (
        [event.id]                     NVARCHAR(100)       '$.id',
        [event.extension]              NVARCHAR(MAX)       '$.extension',
        [event.modifierExtension]      NVARCHAR(MAX)       '$.modifierExtension',
        [event.code]                   NVARCHAR(MAX)       '$.code' AS JSON,
        [event.period.id]              NVARCHAR(100)       '$.period.id',
        [event.period.extension]       NVARCHAR(MAX)       '$.period.extension',
        [event.period.start]           VARCHAR(64)         '$.period.start',
        [event.period.end]             VARCHAR(64)         '$.period.end',
        [event.detail]                 NVARCHAR(MAX)       '$.detail' AS JSON
    ) j

GO

CREATE VIEW fhir.CompositionSection AS
SELECT
    [id],
    [section.JSON],
    [section.id],
    [section.extension],
    [section.modifierExtension],
    [section.title],
    [section.code.id],
    [section.code.extension],
    [section.code.coding],
    [section.code.text],
    [section.author],
    [section.focus.id],
    [section.focus.extension],
    [section.focus.reference],
    [section.focus.type],
    [section.focus.identifier],
    [section.focus.display],
    [section.text.id],
    [section.text.extension],
    [section.text.status],
    [section.text.div],
    [section.mode],
    [section.orderedBy.id],
    [section.orderedBy.extension],
    [section.orderedBy.coding],
    [section.orderedBy.text],
    [section.entry],
    [section.emptyReason.id],
    [section.emptyReason.extension],
    [section.emptyReason.coding],
    [section.emptyReason.text],
    [section.section]
FROM openrowset (
        BULK 'Composition/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [section.JSON]  VARCHAR(MAX) '$.section'
    ) AS rowset
    CROSS APPLY openjson (rowset.[section.JSON]) with (
        [section.id]                   NVARCHAR(100)       '$.id',
        [section.extension]            NVARCHAR(MAX)       '$.extension',
        [section.modifierExtension]    NVARCHAR(MAX)       '$.modifierExtension',
        [section.title]                NVARCHAR(4000)      '$.title',
        [section.code.id]              NVARCHAR(100)       '$.code.id',
        [section.code.extension]       NVARCHAR(MAX)       '$.code.extension',
        [section.code.coding]          NVARCHAR(MAX)       '$.code.coding',
        [section.code.text]            NVARCHAR(4000)      '$.code.text',
        [section.author]               NVARCHAR(MAX)       '$.author' AS JSON,
        [section.focus.id]             NVARCHAR(100)       '$.focus.id',
        [section.focus.extension]      NVARCHAR(MAX)       '$.focus.extension',
        [section.focus.reference]      NVARCHAR(4000)      '$.focus.reference',
        [section.focus.type]           VARCHAR(256)        '$.focus.type',
        [section.focus.identifier]     NVARCHAR(MAX)       '$.focus.identifier',
        [section.focus.display]        NVARCHAR(4000)      '$.focus.display',
        [section.text.id]              NVARCHAR(100)       '$.text.id',
        [section.text.extension]       NVARCHAR(MAX)       '$.text.extension',
        [section.text.status]          NVARCHAR(64)        '$.text.status',
        [section.text.div]             NVARCHAR(MAX)       '$.text.div',
        [section.mode]                 NVARCHAR(100)       '$.mode',
        [section.orderedBy.id]         NVARCHAR(100)       '$.orderedBy.id',
        [section.orderedBy.extension]  NVARCHAR(MAX)       '$.orderedBy.extension',
        [section.orderedBy.coding]     NVARCHAR(MAX)       '$.orderedBy.coding',
        [section.orderedBy.text]       NVARCHAR(4000)      '$.orderedBy.text',
        [section.entry]                NVARCHAR(MAX)       '$.entry' AS JSON,
        [section.emptyReason.id]       NVARCHAR(100)       '$.emptyReason.id',
        [section.emptyReason.extension] NVARCHAR(MAX)       '$.emptyReason.extension',
        [section.emptyReason.coding]   NVARCHAR(MAX)       '$.emptyReason.coding',
        [section.emptyReason.text]     NVARCHAR(4000)      '$.emptyReason.text',
        [section.section]              NVARCHAR(MAX)       '$.section' AS JSON
    ) j
