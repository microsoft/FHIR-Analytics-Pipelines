CREATE EXTERNAL TABLE [fhir].[ImagingStudy] (
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
    [status] NVARCHAR(64),
    [modality] VARCHAR(MAX),
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
    [started] VARCHAR(64),
    [basedOn] VARCHAR(MAX),
    [referrer.id] NVARCHAR(100),
    [referrer.extension] NVARCHAR(MAX),
    [referrer.reference] NVARCHAR(4000),
    [referrer.type] VARCHAR(256),
    [referrer.identifier.id] NVARCHAR(100),
    [referrer.identifier.extension] NVARCHAR(MAX),
    [referrer.identifier.use] NVARCHAR(64),
    [referrer.identifier.type] NVARCHAR(MAX),
    [referrer.identifier.system] VARCHAR(256),
    [referrer.identifier.value] NVARCHAR(4000),
    [referrer.identifier.period] NVARCHAR(MAX),
    [referrer.identifier.assigner] NVARCHAR(MAX),
    [referrer.display] NVARCHAR(4000),
    [interpreter] VARCHAR(MAX),
    [endpoint] VARCHAR(MAX),
    [numberOfSeries] bigint,
    [numberOfInstances] bigint,
    [procedureReference.id] NVARCHAR(100),
    [procedureReference.extension] NVARCHAR(MAX),
    [procedureReference.reference] NVARCHAR(4000),
    [procedureReference.type] VARCHAR(256),
    [procedureReference.identifier.id] NVARCHAR(100),
    [procedureReference.identifier.extension] NVARCHAR(MAX),
    [procedureReference.identifier.use] NVARCHAR(64),
    [procedureReference.identifier.type] NVARCHAR(MAX),
    [procedureReference.identifier.system] VARCHAR(256),
    [procedureReference.identifier.value] NVARCHAR(4000),
    [procedureReference.identifier.period] NVARCHAR(MAX),
    [procedureReference.identifier.assigner] NVARCHAR(MAX),
    [procedureReference.display] NVARCHAR(4000),
    [procedureCode] VARCHAR(MAX),
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
    [reasonCode] VARCHAR(MAX),
    [reasonReference] VARCHAR(MAX),
    [note] VARCHAR(MAX),
    [description] NVARCHAR(4000),
    [series] VARCHAR(MAX),
) WITH (
    LOCATION='/ImagingStudy/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.ImagingStudyIdentifier AS
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
        BULK 'ImagingStudy/**',
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

CREATE VIEW fhir.ImagingStudyModality AS
SELECT
    [id],
    [modality.JSON],
    [modality.id],
    [modality.extension],
    [modality.system],
    [modality.version],
    [modality.code],
    [modality.display],
    [modality.userSelected]
FROM openrowset (
        BULK 'ImagingStudy/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [modality.JSON]  VARCHAR(MAX) '$.modality'
    ) AS rowset
    CROSS APPLY openjson (rowset.[modality.JSON]) with (
        [modality.id]                  NVARCHAR(100)       '$.id',
        [modality.extension]           NVARCHAR(MAX)       '$.extension',
        [modality.system]              VARCHAR(256)        '$.system',
        [modality.version]             NVARCHAR(100)       '$.version',
        [modality.code]                NVARCHAR(4000)      '$.code',
        [modality.display]             NVARCHAR(4000)      '$.display',
        [modality.userSelected]        bit                 '$.userSelected'
    ) j

GO

CREATE VIEW fhir.ImagingStudyBasedOn AS
SELECT
    [id],
    [basedOn.JSON],
    [basedOn.id],
    [basedOn.extension],
    [basedOn.reference],
    [basedOn.type],
    [basedOn.identifier.id],
    [basedOn.identifier.extension],
    [basedOn.identifier.use],
    [basedOn.identifier.type],
    [basedOn.identifier.system],
    [basedOn.identifier.value],
    [basedOn.identifier.period],
    [basedOn.identifier.assigner],
    [basedOn.display]
FROM openrowset (
        BULK 'ImagingStudy/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [basedOn.JSON]  VARCHAR(MAX) '$.basedOn'
    ) AS rowset
    CROSS APPLY openjson (rowset.[basedOn.JSON]) with (
        [basedOn.id]                   NVARCHAR(100)       '$.id',
        [basedOn.extension]            NVARCHAR(MAX)       '$.extension',
        [basedOn.reference]            NVARCHAR(4000)      '$.reference',
        [basedOn.type]                 VARCHAR(256)        '$.type',
        [basedOn.identifier.id]        NVARCHAR(100)       '$.identifier.id',
        [basedOn.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [basedOn.identifier.use]       NVARCHAR(64)        '$.identifier.use',
        [basedOn.identifier.type]      NVARCHAR(MAX)       '$.identifier.type',
        [basedOn.identifier.system]    VARCHAR(256)        '$.identifier.system',
        [basedOn.identifier.value]     NVARCHAR(4000)      '$.identifier.value',
        [basedOn.identifier.period]    NVARCHAR(MAX)       '$.identifier.period',
        [basedOn.identifier.assigner]  NVARCHAR(MAX)       '$.identifier.assigner',
        [basedOn.display]              NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ImagingStudyInterpreter AS
SELECT
    [id],
    [interpreter.JSON],
    [interpreter.id],
    [interpreter.extension],
    [interpreter.reference],
    [interpreter.type],
    [interpreter.identifier.id],
    [interpreter.identifier.extension],
    [interpreter.identifier.use],
    [interpreter.identifier.type],
    [interpreter.identifier.system],
    [interpreter.identifier.value],
    [interpreter.identifier.period],
    [interpreter.identifier.assigner],
    [interpreter.display]
FROM openrowset (
        BULK 'ImagingStudy/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [interpreter.JSON]  VARCHAR(MAX) '$.interpreter'
    ) AS rowset
    CROSS APPLY openjson (rowset.[interpreter.JSON]) with (
        [interpreter.id]               NVARCHAR(100)       '$.id',
        [interpreter.extension]        NVARCHAR(MAX)       '$.extension',
        [interpreter.reference]        NVARCHAR(4000)      '$.reference',
        [interpreter.type]             VARCHAR(256)        '$.type',
        [interpreter.identifier.id]    NVARCHAR(100)       '$.identifier.id',
        [interpreter.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [interpreter.identifier.use]   NVARCHAR(64)        '$.identifier.use',
        [interpreter.identifier.type]  NVARCHAR(MAX)       '$.identifier.type',
        [interpreter.identifier.system] VARCHAR(256)        '$.identifier.system',
        [interpreter.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [interpreter.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [interpreter.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [interpreter.display]          NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ImagingStudyEndpoint AS
SELECT
    [id],
    [endpoint.JSON],
    [endpoint.id],
    [endpoint.extension],
    [endpoint.reference],
    [endpoint.type],
    [endpoint.identifier.id],
    [endpoint.identifier.extension],
    [endpoint.identifier.use],
    [endpoint.identifier.type],
    [endpoint.identifier.system],
    [endpoint.identifier.value],
    [endpoint.identifier.period],
    [endpoint.identifier.assigner],
    [endpoint.display]
FROM openrowset (
        BULK 'ImagingStudy/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [endpoint.JSON]  VARCHAR(MAX) '$.endpoint'
    ) AS rowset
    CROSS APPLY openjson (rowset.[endpoint.JSON]) with (
        [endpoint.id]                  NVARCHAR(100)       '$.id',
        [endpoint.extension]           NVARCHAR(MAX)       '$.extension',
        [endpoint.reference]           NVARCHAR(4000)      '$.reference',
        [endpoint.type]                VARCHAR(256)        '$.type',
        [endpoint.identifier.id]       NVARCHAR(100)       '$.identifier.id',
        [endpoint.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [endpoint.identifier.use]      NVARCHAR(64)        '$.identifier.use',
        [endpoint.identifier.type]     NVARCHAR(MAX)       '$.identifier.type',
        [endpoint.identifier.system]   VARCHAR(256)        '$.identifier.system',
        [endpoint.identifier.value]    NVARCHAR(4000)      '$.identifier.value',
        [endpoint.identifier.period]   NVARCHAR(MAX)       '$.identifier.period',
        [endpoint.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [endpoint.display]             NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ImagingStudyProcedureCode AS
SELECT
    [id],
    [procedureCode.JSON],
    [procedureCode.id],
    [procedureCode.extension],
    [procedureCode.coding],
    [procedureCode.text]
FROM openrowset (
        BULK 'ImagingStudy/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [procedureCode.JSON]  VARCHAR(MAX) '$.procedureCode'
    ) AS rowset
    CROSS APPLY openjson (rowset.[procedureCode.JSON]) with (
        [procedureCode.id]             NVARCHAR(100)       '$.id',
        [procedureCode.extension]      NVARCHAR(MAX)       '$.extension',
        [procedureCode.coding]         NVARCHAR(MAX)       '$.coding' AS JSON,
        [procedureCode.text]           NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.ImagingStudyReasonCode AS
SELECT
    [id],
    [reasonCode.JSON],
    [reasonCode.id],
    [reasonCode.extension],
    [reasonCode.coding],
    [reasonCode.text]
FROM openrowset (
        BULK 'ImagingStudy/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [reasonCode.JSON]  VARCHAR(MAX) '$.reasonCode'
    ) AS rowset
    CROSS APPLY openjson (rowset.[reasonCode.JSON]) with (
        [reasonCode.id]                NVARCHAR(100)       '$.id',
        [reasonCode.extension]         NVARCHAR(MAX)       '$.extension',
        [reasonCode.coding]            NVARCHAR(MAX)       '$.coding' AS JSON,
        [reasonCode.text]              NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.ImagingStudyReasonReference AS
SELECT
    [id],
    [reasonReference.JSON],
    [reasonReference.id],
    [reasonReference.extension],
    [reasonReference.reference],
    [reasonReference.type],
    [reasonReference.identifier.id],
    [reasonReference.identifier.extension],
    [reasonReference.identifier.use],
    [reasonReference.identifier.type],
    [reasonReference.identifier.system],
    [reasonReference.identifier.value],
    [reasonReference.identifier.period],
    [reasonReference.identifier.assigner],
    [reasonReference.display]
FROM openrowset (
        BULK 'ImagingStudy/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [reasonReference.JSON]  VARCHAR(MAX) '$.reasonReference'
    ) AS rowset
    CROSS APPLY openjson (rowset.[reasonReference.JSON]) with (
        [reasonReference.id]           NVARCHAR(100)       '$.id',
        [reasonReference.extension]    NVARCHAR(MAX)       '$.extension',
        [reasonReference.reference]    NVARCHAR(4000)      '$.reference',
        [reasonReference.type]         VARCHAR(256)        '$.type',
        [reasonReference.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [reasonReference.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [reasonReference.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [reasonReference.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [reasonReference.identifier.system] VARCHAR(256)        '$.identifier.system',
        [reasonReference.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [reasonReference.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [reasonReference.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [reasonReference.display]      NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ImagingStudyNote AS
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
        BULK 'ImagingStudy/**',
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

CREATE VIEW fhir.ImagingStudySeries AS
SELECT
    [id],
    [series.JSON],
    [series.id],
    [series.extension],
    [series.modifierExtension],
    [series.uid],
    [series.number],
    [series.modality.id],
    [series.modality.extension],
    [series.modality.system],
    [series.modality.version],
    [series.modality.code],
    [series.modality.display],
    [series.modality.userSelected],
    [series.description],
    [series.numberOfInstances],
    [series.endpoint],
    [series.bodySite.id],
    [series.bodySite.extension],
    [series.bodySite.system],
    [series.bodySite.version],
    [series.bodySite.code],
    [series.bodySite.display],
    [series.bodySite.userSelected],
    [series.laterality.id],
    [series.laterality.extension],
    [series.laterality.system],
    [series.laterality.version],
    [series.laterality.code],
    [series.laterality.display],
    [series.laterality.userSelected],
    [series.specimen],
    [series.started],
    [series.performer],
    [series.instance]
FROM openrowset (
        BULK 'ImagingStudy/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [series.JSON]  VARCHAR(MAX) '$.series'
    ) AS rowset
    CROSS APPLY openjson (rowset.[series.JSON]) with (
        [series.id]                    NVARCHAR(100)       '$.id',
        [series.extension]             NVARCHAR(MAX)       '$.extension',
        [series.modifierExtension]     NVARCHAR(MAX)       '$.modifierExtension',
        [series.uid]                   VARCHAR(64)         '$.uid',
        [series.number]                bigint              '$.number',
        [series.modality.id]           NVARCHAR(100)       '$.modality.id',
        [series.modality.extension]    NVARCHAR(MAX)       '$.modality.extension',
        [series.modality.system]       VARCHAR(256)        '$.modality.system',
        [series.modality.version]      NVARCHAR(100)       '$.modality.version',
        [series.modality.code]         NVARCHAR(4000)      '$.modality.code',
        [series.modality.display]      NVARCHAR(4000)      '$.modality.display',
        [series.modality.userSelected] bit                 '$.modality.userSelected',
        [series.description]           NVARCHAR(4000)      '$.description',
        [series.numberOfInstances]     bigint              '$.numberOfInstances',
        [series.endpoint]              NVARCHAR(MAX)       '$.endpoint' AS JSON,
        [series.bodySite.id]           NVARCHAR(100)       '$.bodySite.id',
        [series.bodySite.extension]    NVARCHAR(MAX)       '$.bodySite.extension',
        [series.bodySite.system]       VARCHAR(256)        '$.bodySite.system',
        [series.bodySite.version]      NVARCHAR(100)       '$.bodySite.version',
        [series.bodySite.code]         NVARCHAR(4000)      '$.bodySite.code',
        [series.bodySite.display]      NVARCHAR(4000)      '$.bodySite.display',
        [series.bodySite.userSelected] bit                 '$.bodySite.userSelected',
        [series.laterality.id]         NVARCHAR(100)       '$.laterality.id',
        [series.laterality.extension]  NVARCHAR(MAX)       '$.laterality.extension',
        [series.laterality.system]     VARCHAR(256)        '$.laterality.system',
        [series.laterality.version]    NVARCHAR(100)       '$.laterality.version',
        [series.laterality.code]       NVARCHAR(4000)      '$.laterality.code',
        [series.laterality.display]    NVARCHAR(4000)      '$.laterality.display',
        [series.laterality.userSelected] bit                 '$.laterality.userSelected',
        [series.specimen]              NVARCHAR(MAX)       '$.specimen' AS JSON,
        [series.started]               VARCHAR(64)         '$.started',
        [series.performer]             NVARCHAR(MAX)       '$.performer' AS JSON,
        [series.instance]              NVARCHAR(MAX)       '$.instance' AS JSON
    ) j
