CREATE EXTERNAL TABLE [fhir].[PaymentNotice] (
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
    [language] NVARCHAR(12),
    [text.id] NVARCHAR(100),
    [text.extension] NVARCHAR(MAX),
    [text.status] NVARCHAR(64),
    [text.div] NVARCHAR(MAX),
    [extension] NVARCHAR(MAX),
    [modifierExtension] NVARCHAR(MAX),
    [identifier] VARCHAR(MAX),
    [status] NVARCHAR(64),
    [request.id] NVARCHAR(100),
    [request.extension] NVARCHAR(MAX),
    [request.reference] NVARCHAR(4000),
    [request.type] VARCHAR(256),
    [request.identifier.id] NVARCHAR(100),
    [request.identifier.extension] NVARCHAR(MAX),
    [request.identifier.use] NVARCHAR(64),
    [request.identifier.type] NVARCHAR(MAX),
    [request.identifier.system] VARCHAR(256),
    [request.identifier.value] NVARCHAR(4000),
    [request.identifier.period] NVARCHAR(MAX),
    [request.identifier.assigner] NVARCHAR(MAX),
    [request.display] NVARCHAR(4000),
    [response.id] NVARCHAR(100),
    [response.extension] NVARCHAR(MAX),
    [response.reference] NVARCHAR(4000),
    [response.type] VARCHAR(256),
    [response.identifier.id] NVARCHAR(100),
    [response.identifier.extension] NVARCHAR(MAX),
    [response.identifier.use] NVARCHAR(64),
    [response.identifier.type] NVARCHAR(MAX),
    [response.identifier.system] VARCHAR(256),
    [response.identifier.value] NVARCHAR(4000),
    [response.identifier.period] NVARCHAR(MAX),
    [response.identifier.assigner] NVARCHAR(MAX),
    [response.display] NVARCHAR(4000),
    [created] VARCHAR(64),
    [provider.id] NVARCHAR(100),
    [provider.extension] NVARCHAR(MAX),
    [provider.reference] NVARCHAR(4000),
    [provider.type] VARCHAR(256),
    [provider.identifier.id] NVARCHAR(100),
    [provider.identifier.extension] NVARCHAR(MAX),
    [provider.identifier.use] NVARCHAR(64),
    [provider.identifier.type] NVARCHAR(MAX),
    [provider.identifier.system] VARCHAR(256),
    [provider.identifier.value] NVARCHAR(4000),
    [provider.identifier.period] NVARCHAR(MAX),
    [provider.identifier.assigner] NVARCHAR(MAX),
    [provider.display] NVARCHAR(4000),
    [payment.id] NVARCHAR(100),
    [payment.extension] NVARCHAR(MAX),
    [payment.reference] NVARCHAR(4000),
    [payment.type] VARCHAR(256),
    [payment.identifier.id] NVARCHAR(100),
    [payment.identifier.extension] NVARCHAR(MAX),
    [payment.identifier.use] NVARCHAR(64),
    [payment.identifier.type] NVARCHAR(MAX),
    [payment.identifier.system] VARCHAR(256),
    [payment.identifier.value] NVARCHAR(4000),
    [payment.identifier.period] NVARCHAR(MAX),
    [payment.identifier.assigner] NVARCHAR(MAX),
    [payment.display] NVARCHAR(4000),
    [paymentDate] VARCHAR(32),
    [payee.id] NVARCHAR(100),
    [payee.extension] NVARCHAR(MAX),
    [payee.reference] NVARCHAR(4000),
    [payee.type] VARCHAR(256),
    [payee.identifier.id] NVARCHAR(100),
    [payee.identifier.extension] NVARCHAR(MAX),
    [payee.identifier.use] NVARCHAR(64),
    [payee.identifier.type] NVARCHAR(MAX),
    [payee.identifier.system] VARCHAR(256),
    [payee.identifier.value] NVARCHAR(4000),
    [payee.identifier.period] NVARCHAR(MAX),
    [payee.identifier.assigner] NVARCHAR(MAX),
    [payee.display] NVARCHAR(4000),
    [recipient.id] NVARCHAR(100),
    [recipient.extension] NVARCHAR(MAX),
    [recipient.reference] NVARCHAR(4000),
    [recipient.type] VARCHAR(256),
    [recipient.identifier.id] NVARCHAR(100),
    [recipient.identifier.extension] NVARCHAR(MAX),
    [recipient.identifier.use] NVARCHAR(64),
    [recipient.identifier.type] NVARCHAR(MAX),
    [recipient.identifier.system] VARCHAR(256),
    [recipient.identifier.value] NVARCHAR(4000),
    [recipient.identifier.period] NVARCHAR(MAX),
    [recipient.identifier.assigner] NVARCHAR(MAX),
    [recipient.display] NVARCHAR(4000),
    [amount.id] NVARCHAR(100),
    [amount.extension] NVARCHAR(MAX),
    [amount.value] float,
    [amount.currency] NVARCHAR(8),
    [paymentStatus.id] NVARCHAR(100),
    [paymentStatus.extension] NVARCHAR(MAX),
    [paymentStatus.coding] VARCHAR(MAX),
    [paymentStatus.text] NVARCHAR(4000),
) WITH (
    LOCATION='/PaymentNotice/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.PaymentNoticeIdentifier AS
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
        BULK 'PaymentNotice/**',
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
