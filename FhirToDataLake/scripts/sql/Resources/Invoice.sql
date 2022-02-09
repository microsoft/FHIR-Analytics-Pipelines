CREATE EXTERNAL TABLE [fhir].[Invoice] (
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
    [cancelledReason] NVARCHAR(4000),
    [type.id] NVARCHAR(100),
    [type.extension] NVARCHAR(MAX),
    [type.coding] VARCHAR(MAX),
    [type.text] NVARCHAR(4000),
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
    [date] VARCHAR(64),
    [participant] VARCHAR(MAX),
    [issuer.id] NVARCHAR(100),
    [issuer.extension] NVARCHAR(MAX),
    [issuer.reference] NVARCHAR(4000),
    [issuer.type] VARCHAR(256),
    [issuer.identifier.id] NVARCHAR(100),
    [issuer.identifier.extension] NVARCHAR(MAX),
    [issuer.identifier.use] NVARCHAR(64),
    [issuer.identifier.type] NVARCHAR(MAX),
    [issuer.identifier.system] VARCHAR(256),
    [issuer.identifier.value] NVARCHAR(4000),
    [issuer.identifier.period] NVARCHAR(MAX),
    [issuer.identifier.assigner] NVARCHAR(MAX),
    [issuer.display] NVARCHAR(4000),
    [account.id] NVARCHAR(100),
    [account.extension] NVARCHAR(MAX),
    [account.reference] NVARCHAR(4000),
    [account.type] VARCHAR(256),
    [account.identifier.id] NVARCHAR(100),
    [account.identifier.extension] NVARCHAR(MAX),
    [account.identifier.use] NVARCHAR(64),
    [account.identifier.type] NVARCHAR(MAX),
    [account.identifier.system] VARCHAR(256),
    [account.identifier.value] NVARCHAR(4000),
    [account.identifier.period] NVARCHAR(MAX),
    [account.identifier.assigner] NVARCHAR(MAX),
    [account.display] NVARCHAR(4000),
    [lineItem] VARCHAR(MAX),
    [totalPriceComponent] VARCHAR(MAX),
    [totalNet.id] NVARCHAR(100),
    [totalNet.extension] NVARCHAR(MAX),
    [totalNet.value] float,
    [totalNet.currency] NVARCHAR(100),
    [totalGross.id] NVARCHAR(100),
    [totalGross.extension] NVARCHAR(MAX),
    [totalGross.value] float,
    [totalGross.currency] NVARCHAR(100),
    [paymentTerms] NVARCHAR(MAX),
    [note] VARCHAR(MAX),
) WITH (
    LOCATION='/Invoice/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.InvoiceIdentifier AS
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
        BULK 'Invoice/**',
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

CREATE VIEW fhir.InvoiceParticipant AS
SELECT
    [id],
    [participant.JSON],
    [participant.id],
    [participant.extension],
    [participant.modifierExtension],
    [participant.role.id],
    [participant.role.extension],
    [participant.role.coding],
    [participant.role.text],
    [participant.actor.id],
    [participant.actor.extension],
    [participant.actor.reference],
    [participant.actor.type],
    [participant.actor.identifier],
    [participant.actor.display]
FROM openrowset (
        BULK 'Invoice/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [participant.JSON]  VARCHAR(MAX) '$.participant'
    ) AS rowset
    CROSS APPLY openjson (rowset.[participant.JSON]) with (
        [participant.id]               NVARCHAR(100)       '$.id',
        [participant.extension]        NVARCHAR(MAX)       '$.extension',
        [participant.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [participant.role.id]          NVARCHAR(100)       '$.role.id',
        [participant.role.extension]   NVARCHAR(MAX)       '$.role.extension',
        [participant.role.coding]      NVARCHAR(MAX)       '$.role.coding',
        [participant.role.text]        NVARCHAR(4000)      '$.role.text',
        [participant.actor.id]         NVARCHAR(100)       '$.actor.id',
        [participant.actor.extension]  NVARCHAR(MAX)       '$.actor.extension',
        [participant.actor.reference]  NVARCHAR(4000)      '$.actor.reference',
        [participant.actor.type]       VARCHAR(256)        '$.actor.type',
        [participant.actor.identifier] NVARCHAR(MAX)       '$.actor.identifier',
        [participant.actor.display]    NVARCHAR(4000)      '$.actor.display'
    ) j

GO

CREATE VIEW fhir.InvoiceLineItem AS
SELECT
    [id],
    [lineItem.JSON],
    [lineItem.id],
    [lineItem.extension],
    [lineItem.modifierExtension],
    [lineItem.sequence],
    [lineItem.priceComponent],
    [lineItem.chargeItem.reference.id],
    [lineItem.chargeItem.reference.extension],
    [lineItem.chargeItem.reference.reference],
    [lineItem.chargeItem.reference.type],
    [lineItem.chargeItem.reference.identifier],
    [lineItem.chargeItem.reference.display],
    [lineItem.chargeItem.codeableConcept.id],
    [lineItem.chargeItem.codeableConcept.extension],
    [lineItem.chargeItem.codeableConcept.coding],
    [lineItem.chargeItem.codeableConcept.text]
FROM openrowset (
        BULK 'Invoice/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [lineItem.JSON]  VARCHAR(MAX) '$.lineItem'
    ) AS rowset
    CROSS APPLY openjson (rowset.[lineItem.JSON]) with (
        [lineItem.id]                  NVARCHAR(100)       '$.id',
        [lineItem.extension]           NVARCHAR(MAX)       '$.extension',
        [lineItem.modifierExtension]   NVARCHAR(MAX)       '$.modifierExtension',
        [lineItem.sequence]            bigint              '$.sequence',
        [lineItem.priceComponent]      NVARCHAR(MAX)       '$.priceComponent' AS JSON,
        [lineItem.chargeItem.reference.id] NVARCHAR(100)       '$.chargeItem.reference.id',
        [lineItem.chargeItem.reference.extension] NVARCHAR(MAX)       '$.chargeItem.reference.extension',
        [lineItem.chargeItem.reference.reference] NVARCHAR(4000)      '$.chargeItem.reference.reference',
        [lineItem.chargeItem.reference.type] VARCHAR(256)        '$.chargeItem.reference.type',
        [lineItem.chargeItem.reference.identifier] NVARCHAR(MAX)       '$.chargeItem.reference.identifier',
        [lineItem.chargeItem.reference.display] NVARCHAR(4000)      '$.chargeItem.reference.display',
        [lineItem.chargeItem.codeableConcept.id] NVARCHAR(100)       '$.chargeItem.codeableConcept.id',
        [lineItem.chargeItem.codeableConcept.extension] NVARCHAR(MAX)       '$.chargeItem.codeableConcept.extension',
        [lineItem.chargeItem.codeableConcept.coding] NVARCHAR(MAX)       '$.chargeItem.codeableConcept.coding',
        [lineItem.chargeItem.codeableConcept.text] NVARCHAR(4000)      '$.chargeItem.codeableConcept.text'
    ) j

GO

CREATE VIEW fhir.InvoiceTotalPriceComponent AS
SELECT
    [id],
    [totalPriceComponent.JSON],
    [totalPriceComponent.id],
    [totalPriceComponent.extension],
    [totalPriceComponent.modifierExtension],
    [totalPriceComponent.type],
    [totalPriceComponent.code.id],
    [totalPriceComponent.code.extension],
    [totalPriceComponent.code.coding],
    [totalPriceComponent.code.text],
    [totalPriceComponent.factor],
    [totalPriceComponent.amount.id],
    [totalPriceComponent.amount.extension],
    [totalPriceComponent.amount.value],
    [totalPriceComponent.amount.currency]
FROM openrowset (
        BULK 'Invoice/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [totalPriceComponent.JSON]  VARCHAR(MAX) '$.totalPriceComponent'
    ) AS rowset
    CROSS APPLY openjson (rowset.[totalPriceComponent.JSON]) with (
        [totalPriceComponent.id]       NVARCHAR(100)       '$.id',
        [totalPriceComponent.extension] NVARCHAR(MAX)       '$.extension',
        [totalPriceComponent.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [totalPriceComponent.type]     NVARCHAR(64)        '$.type',
        [totalPriceComponent.code.id]  NVARCHAR(100)       '$.code.id',
        [totalPriceComponent.code.extension] NVARCHAR(MAX)       '$.code.extension',
        [totalPriceComponent.code.coding] NVARCHAR(MAX)       '$.code.coding',
        [totalPriceComponent.code.text] NVARCHAR(4000)      '$.code.text',
        [totalPriceComponent.factor]   float               '$.factor',
        [totalPriceComponent.amount.id] NVARCHAR(100)       '$.amount.id',
        [totalPriceComponent.amount.extension] NVARCHAR(MAX)       '$.amount.extension',
        [totalPriceComponent.amount.value] float               '$.amount.value',
        [totalPriceComponent.amount.currency] NVARCHAR(100)       '$.amount.currency'
    ) j

GO

CREATE VIEW fhir.InvoiceNote AS
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
        BULK 'Invoice/**',
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
