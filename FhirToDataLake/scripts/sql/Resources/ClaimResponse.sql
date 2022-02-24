CREATE EXTERNAL TABLE [fhir].[ClaimResponse] (
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
    [status] NVARCHAR(100),
    [type.id] NVARCHAR(100),
    [type.extension] NVARCHAR(MAX),
    [type.coding] VARCHAR(MAX),
    [type.text] NVARCHAR(4000),
    [subType.id] NVARCHAR(100),
    [subType.extension] NVARCHAR(MAX),
    [subType.coding] VARCHAR(MAX),
    [subType.text] NVARCHAR(4000),
    [use] NVARCHAR(100),
    [patient.id] NVARCHAR(100),
    [patient.extension] NVARCHAR(MAX),
    [patient.reference] NVARCHAR(4000),
    [patient.type] VARCHAR(256),
    [patient.identifier.id] NVARCHAR(100),
    [patient.identifier.extension] NVARCHAR(MAX),
    [patient.identifier.use] NVARCHAR(64),
    [patient.identifier.type] NVARCHAR(MAX),
    [patient.identifier.system] VARCHAR(256),
    [patient.identifier.value] NVARCHAR(4000),
    [patient.identifier.period] NVARCHAR(MAX),
    [patient.identifier.assigner] NVARCHAR(MAX),
    [patient.display] NVARCHAR(4000),
    [created] VARCHAR(64),
    [insurer.id] NVARCHAR(100),
    [insurer.extension] NVARCHAR(MAX),
    [insurer.reference] NVARCHAR(4000),
    [insurer.type] VARCHAR(256),
    [insurer.identifier.id] NVARCHAR(100),
    [insurer.identifier.extension] NVARCHAR(MAX),
    [insurer.identifier.use] NVARCHAR(64),
    [insurer.identifier.type] NVARCHAR(MAX),
    [insurer.identifier.system] VARCHAR(256),
    [insurer.identifier.value] NVARCHAR(4000),
    [insurer.identifier.period] NVARCHAR(MAX),
    [insurer.identifier.assigner] NVARCHAR(MAX),
    [insurer.display] NVARCHAR(4000),
    [requestor.id] NVARCHAR(100),
    [requestor.extension] NVARCHAR(MAX),
    [requestor.reference] NVARCHAR(4000),
    [requestor.type] VARCHAR(256),
    [requestor.identifier.id] NVARCHAR(100),
    [requestor.identifier.extension] NVARCHAR(MAX),
    [requestor.identifier.use] NVARCHAR(64),
    [requestor.identifier.type] NVARCHAR(MAX),
    [requestor.identifier.system] VARCHAR(256),
    [requestor.identifier.value] NVARCHAR(4000),
    [requestor.identifier.period] NVARCHAR(MAX),
    [requestor.identifier.assigner] NVARCHAR(MAX),
    [requestor.display] NVARCHAR(4000),
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
    [outcome] NVARCHAR(100),
    [disposition] NVARCHAR(4000),
    [preAuthRef] NVARCHAR(4000),
    [preAuthPeriod.id] NVARCHAR(100),
    [preAuthPeriod.extension] NVARCHAR(MAX),
    [preAuthPeriod.start] VARCHAR(64),
    [preAuthPeriod.end] VARCHAR(64),
    [payeeType.id] NVARCHAR(100),
    [payeeType.extension] NVARCHAR(MAX),
    [payeeType.coding] VARCHAR(MAX),
    [payeeType.text] NVARCHAR(4000),
    [item] VARCHAR(MAX),
    [addItem] VARCHAR(MAX),
    [adjudication] VARCHAR(MAX),
    [total] VARCHAR(MAX),
    [payment.id] NVARCHAR(100),
    [payment.extension] NVARCHAR(MAX),
    [payment.modifierExtension] NVARCHAR(MAX),
    [payment.type.id] NVARCHAR(100),
    [payment.type.extension] NVARCHAR(MAX),
    [payment.type.coding] NVARCHAR(MAX),
    [payment.type.text] NVARCHAR(4000),
    [payment.adjustment.id] NVARCHAR(100),
    [payment.adjustment.extension] NVARCHAR(MAX),
    [payment.adjustment.value] float,
    [payment.adjustment.currency] NVARCHAR(100),
    [payment.adjustmentReason.id] NVARCHAR(100),
    [payment.adjustmentReason.extension] NVARCHAR(MAX),
    [payment.adjustmentReason.coding] NVARCHAR(MAX),
    [payment.adjustmentReason.text] NVARCHAR(4000),
    [payment.date] VARCHAR(64),
    [payment.amount.id] NVARCHAR(100),
    [payment.amount.extension] NVARCHAR(MAX),
    [payment.amount.value] float,
    [payment.amount.currency] NVARCHAR(100),
    [payment.identifier.id] NVARCHAR(100),
    [payment.identifier.extension] NVARCHAR(MAX),
    [payment.identifier.use] NVARCHAR(64),
    [payment.identifier.type] NVARCHAR(MAX),
    [payment.identifier.system] VARCHAR(256),
    [payment.identifier.value] NVARCHAR(4000),
    [payment.identifier.period] NVARCHAR(MAX),
    [payment.identifier.assigner] NVARCHAR(MAX),
    [fundsReserve.id] NVARCHAR(100),
    [fundsReserve.extension] NVARCHAR(MAX),
    [fundsReserve.coding] VARCHAR(MAX),
    [fundsReserve.text] NVARCHAR(4000),
    [formCode.id] NVARCHAR(100),
    [formCode.extension] NVARCHAR(MAX),
    [formCode.coding] VARCHAR(MAX),
    [formCode.text] NVARCHAR(4000),
    [form.id] NVARCHAR(100),
    [form.extension] NVARCHAR(MAX),
    [form.contentType] NVARCHAR(100),
    [form.language] NVARCHAR(100),
    [form.data] NVARCHAR(MAX),
    [form.url] VARCHAR(256),
    [form.size] bigint,
    [form.hash] NVARCHAR(MAX),
    [form.title] NVARCHAR(4000),
    [form.creation] VARCHAR(64),
    [processNote] VARCHAR(MAX),
    [communicationRequest] VARCHAR(MAX),
    [insurance] VARCHAR(MAX),
    [error] VARCHAR(MAX),
) WITH (
    LOCATION='/ClaimResponse/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.ClaimResponseIdentifier AS
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
        BULK 'ClaimResponse/**',
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

CREATE VIEW fhir.ClaimResponseItem AS
SELECT
    [id],
    [item.JSON],
    [item.id],
    [item.extension],
    [item.modifierExtension],
    [item.itemSequence],
    [item.noteNumber],
    [item.adjudication],
    [item.detail]
FROM openrowset (
        BULK 'ClaimResponse/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [item.JSON]  VARCHAR(MAX) '$.item'
    ) AS rowset
    CROSS APPLY openjson (rowset.[item.JSON]) with (
        [item.id]                      NVARCHAR(100)       '$.id',
        [item.extension]               NVARCHAR(MAX)       '$.extension',
        [item.modifierExtension]       NVARCHAR(MAX)       '$.modifierExtension',
        [item.itemSequence]            bigint              '$.itemSequence',
        [item.noteNumber]              NVARCHAR(MAX)       '$.noteNumber' AS JSON,
        [item.adjudication]            NVARCHAR(MAX)       '$.adjudication' AS JSON,
        [item.detail]                  NVARCHAR(MAX)       '$.detail' AS JSON
    ) j

GO

CREATE VIEW fhir.ClaimResponseAddItem AS
SELECT
    [id],
    [addItem.JSON],
    [addItem.id],
    [addItem.extension],
    [addItem.modifierExtension],
    [addItem.itemSequence],
    [addItem.detailSequence],
    [addItem.subdetailSequence],
    [addItem.provider],
    [addItem.productOrService.id],
    [addItem.productOrService.extension],
    [addItem.productOrService.coding],
    [addItem.productOrService.text],
    [addItem.modifier],
    [addItem.programCode],
    [addItem.quantity.id],
    [addItem.quantity.extension],
    [addItem.quantity.value],
    [addItem.quantity.comparator],
    [addItem.quantity.unit],
    [addItem.quantity.system],
    [addItem.quantity.code],
    [addItem.unitPrice.id],
    [addItem.unitPrice.extension],
    [addItem.unitPrice.value],
    [addItem.unitPrice.currency],
    [addItem.factor],
    [addItem.net.id],
    [addItem.net.extension],
    [addItem.net.value],
    [addItem.net.currency],
    [addItem.bodySite.id],
    [addItem.bodySite.extension],
    [addItem.bodySite.coding],
    [addItem.bodySite.text],
    [addItem.subSite],
    [addItem.noteNumber],
    [addItem.adjudication],
    [addItem.detail],
    [addItem.serviced.date],
    [addItem.serviced.period.id],
    [addItem.serviced.period.extension],
    [addItem.serviced.period.start],
    [addItem.serviced.period.end],
    [addItem.location.codeableConcept.id],
    [addItem.location.codeableConcept.extension],
    [addItem.location.codeableConcept.coding],
    [addItem.location.codeableConcept.text],
    [addItem.location.address.id],
    [addItem.location.address.extension],
    [addItem.location.address.use],
    [addItem.location.address.type],
    [addItem.location.address.text],
    [addItem.location.address.line],
    [addItem.location.address.city],
    [addItem.location.address.district],
    [addItem.location.address.state],
    [addItem.location.address.postalCode],
    [addItem.location.address.country],
    [addItem.location.address.period],
    [addItem.location.reference.id],
    [addItem.location.reference.extension],
    [addItem.location.reference.reference],
    [addItem.location.reference.type],
    [addItem.location.reference.identifier],
    [addItem.location.reference.display]
FROM openrowset (
        BULK 'ClaimResponse/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [addItem.JSON]  VARCHAR(MAX) '$.addItem'
    ) AS rowset
    CROSS APPLY openjson (rowset.[addItem.JSON]) with (
        [addItem.id]                   NVARCHAR(100)       '$.id',
        [addItem.extension]            NVARCHAR(MAX)       '$.extension',
        [addItem.modifierExtension]    NVARCHAR(MAX)       '$.modifierExtension',
        [addItem.itemSequence]         NVARCHAR(MAX)       '$.itemSequence' AS JSON,
        [addItem.detailSequence]       NVARCHAR(MAX)       '$.detailSequence' AS JSON,
        [addItem.subdetailSequence]    NVARCHAR(MAX)       '$.subdetailSequence' AS JSON,
        [addItem.provider]             NVARCHAR(MAX)       '$.provider' AS JSON,
        [addItem.productOrService.id]  NVARCHAR(100)       '$.productOrService.id',
        [addItem.productOrService.extension] NVARCHAR(MAX)       '$.productOrService.extension',
        [addItem.productOrService.coding] NVARCHAR(MAX)       '$.productOrService.coding',
        [addItem.productOrService.text] NVARCHAR(4000)      '$.productOrService.text',
        [addItem.modifier]             NVARCHAR(MAX)       '$.modifier' AS JSON,
        [addItem.programCode]          NVARCHAR(MAX)       '$.programCode' AS JSON,
        [addItem.quantity.id]          NVARCHAR(100)       '$.quantity.id',
        [addItem.quantity.extension]   NVARCHAR(MAX)       '$.quantity.extension',
        [addItem.quantity.value]       float               '$.quantity.value',
        [addItem.quantity.comparator]  NVARCHAR(64)        '$.quantity.comparator',
        [addItem.quantity.unit]        NVARCHAR(100)       '$.quantity.unit',
        [addItem.quantity.system]      VARCHAR(256)        '$.quantity.system',
        [addItem.quantity.code]        NVARCHAR(4000)      '$.quantity.code',
        [addItem.unitPrice.id]         NVARCHAR(100)       '$.unitPrice.id',
        [addItem.unitPrice.extension]  NVARCHAR(MAX)       '$.unitPrice.extension',
        [addItem.unitPrice.value]      float               '$.unitPrice.value',
        [addItem.unitPrice.currency]   NVARCHAR(100)       '$.unitPrice.currency',
        [addItem.factor]               float               '$.factor',
        [addItem.net.id]               NVARCHAR(100)       '$.net.id',
        [addItem.net.extension]        NVARCHAR(MAX)       '$.net.extension',
        [addItem.net.value]            float               '$.net.value',
        [addItem.net.currency]         NVARCHAR(100)       '$.net.currency',
        [addItem.bodySite.id]          NVARCHAR(100)       '$.bodySite.id',
        [addItem.bodySite.extension]   NVARCHAR(MAX)       '$.bodySite.extension',
        [addItem.bodySite.coding]      NVARCHAR(MAX)       '$.bodySite.coding',
        [addItem.bodySite.text]        NVARCHAR(4000)      '$.bodySite.text',
        [addItem.subSite]              NVARCHAR(MAX)       '$.subSite' AS JSON,
        [addItem.noteNumber]           NVARCHAR(MAX)       '$.noteNumber' AS JSON,
        [addItem.adjudication]         NVARCHAR(MAX)       '$.adjudication' AS JSON,
        [addItem.detail]               NVARCHAR(MAX)       '$.detail' AS JSON,
        [addItem.serviced.date]        VARCHAR(64)         '$.serviced.date',
        [addItem.serviced.period.id]   NVARCHAR(100)       '$.serviced.period.id',
        [addItem.serviced.period.extension] NVARCHAR(MAX)       '$.serviced.period.extension',
        [addItem.serviced.period.start] VARCHAR(64)         '$.serviced.period.start',
        [addItem.serviced.period.end]  VARCHAR(64)         '$.serviced.period.end',
        [addItem.location.codeableConcept.id] NVARCHAR(100)       '$.location.codeableConcept.id',
        [addItem.location.codeableConcept.extension] NVARCHAR(MAX)       '$.location.codeableConcept.extension',
        [addItem.location.codeableConcept.coding] NVARCHAR(MAX)       '$.location.codeableConcept.coding',
        [addItem.location.codeableConcept.text] NVARCHAR(4000)      '$.location.codeableConcept.text',
        [addItem.location.address.id]  NVARCHAR(100)       '$.location.address.id',
        [addItem.location.address.extension] NVARCHAR(MAX)       '$.location.address.extension',
        [addItem.location.address.use] NVARCHAR(64)        '$.location.address.use',
        [addItem.location.address.type] NVARCHAR(64)        '$.location.address.type',
        [addItem.location.address.text] NVARCHAR(4000)      '$.location.address.text',
        [addItem.location.address.line] NVARCHAR(MAX)       '$.location.address.line',
        [addItem.location.address.city] NVARCHAR(500)       '$.location.address.city',
        [addItem.location.address.district] NVARCHAR(500)       '$.location.address.district',
        [addItem.location.address.state] NVARCHAR(500)       '$.location.address.state',
        [addItem.location.address.postalCode] NVARCHAR(100)       '$.location.address.postalCode',
        [addItem.location.address.country] NVARCHAR(500)       '$.location.address.country',
        [addItem.location.address.period] NVARCHAR(MAX)       '$.location.address.period',
        [addItem.location.reference.id] NVARCHAR(100)       '$.location.reference.id',
        [addItem.location.reference.extension] NVARCHAR(MAX)       '$.location.reference.extension',
        [addItem.location.reference.reference] NVARCHAR(4000)      '$.location.reference.reference',
        [addItem.location.reference.type] VARCHAR(256)        '$.location.reference.type',
        [addItem.location.reference.identifier] NVARCHAR(MAX)       '$.location.reference.identifier',
        [addItem.location.reference.display] NVARCHAR(4000)      '$.location.reference.display'
    ) j

GO

CREATE VIEW fhir.ClaimResponseAdjudication AS
SELECT
    [id],
    [adjudication.JSON],
    [adjudication.id],
    [adjudication.extension],
    [adjudication.modifierExtension],
    [adjudication.category.id],
    [adjudication.category.extension],
    [adjudication.category.coding],
    [adjudication.category.text],
    [adjudication.reason.id],
    [adjudication.reason.extension],
    [adjudication.reason.coding],
    [adjudication.reason.text],
    [adjudication.amount.id],
    [adjudication.amount.extension],
    [adjudication.amount.value],
    [adjudication.amount.currency],
    [adjudication.value]
FROM openrowset (
        BULK 'ClaimResponse/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [adjudication.JSON]  VARCHAR(MAX) '$.adjudication'
    ) AS rowset
    CROSS APPLY openjson (rowset.[adjudication.JSON]) with (
        [adjudication.id]              NVARCHAR(100)       '$.id',
        [adjudication.extension]       NVARCHAR(MAX)       '$.extension',
        [adjudication.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [adjudication.category.id]     NVARCHAR(100)       '$.category.id',
        [adjudication.category.extension] NVARCHAR(MAX)       '$.category.extension',
        [adjudication.category.coding] NVARCHAR(MAX)       '$.category.coding',
        [adjudication.category.text]   NVARCHAR(4000)      '$.category.text',
        [adjudication.reason.id]       NVARCHAR(100)       '$.reason.id',
        [adjudication.reason.extension] NVARCHAR(MAX)       '$.reason.extension',
        [adjudication.reason.coding]   NVARCHAR(MAX)       '$.reason.coding',
        [adjudication.reason.text]     NVARCHAR(4000)      '$.reason.text',
        [adjudication.amount.id]       NVARCHAR(100)       '$.amount.id',
        [adjudication.amount.extension] NVARCHAR(MAX)       '$.amount.extension',
        [adjudication.amount.value]    float               '$.amount.value',
        [adjudication.amount.currency] NVARCHAR(100)       '$.amount.currency',
        [adjudication.value]           float               '$.value'
    ) j

GO

CREATE VIEW fhir.ClaimResponseTotal AS
SELECT
    [id],
    [total.JSON],
    [total.id],
    [total.extension],
    [total.modifierExtension],
    [total.category.id],
    [total.category.extension],
    [total.category.coding],
    [total.category.text],
    [total.amount.id],
    [total.amount.extension],
    [total.amount.value],
    [total.amount.currency]
FROM openrowset (
        BULK 'ClaimResponse/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [total.JSON]  VARCHAR(MAX) '$.total'
    ) AS rowset
    CROSS APPLY openjson (rowset.[total.JSON]) with (
        [total.id]                     NVARCHAR(100)       '$.id',
        [total.extension]              NVARCHAR(MAX)       '$.extension',
        [total.modifierExtension]      NVARCHAR(MAX)       '$.modifierExtension',
        [total.category.id]            NVARCHAR(100)       '$.category.id',
        [total.category.extension]     NVARCHAR(MAX)       '$.category.extension',
        [total.category.coding]        NVARCHAR(MAX)       '$.category.coding',
        [total.category.text]          NVARCHAR(4000)      '$.category.text',
        [total.amount.id]              NVARCHAR(100)       '$.amount.id',
        [total.amount.extension]       NVARCHAR(MAX)       '$.amount.extension',
        [total.amount.value]           float               '$.amount.value',
        [total.amount.currency]        NVARCHAR(100)       '$.amount.currency'
    ) j

GO

CREATE VIEW fhir.ClaimResponseProcessNote AS
SELECT
    [id],
    [processNote.JSON],
    [processNote.id],
    [processNote.extension],
    [processNote.modifierExtension],
    [processNote.number],
    [processNote.type],
    [processNote.text],
    [processNote.language.id],
    [processNote.language.extension],
    [processNote.language.coding],
    [processNote.language.text]
FROM openrowset (
        BULK 'ClaimResponse/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [processNote.JSON]  VARCHAR(MAX) '$.processNote'
    ) AS rowset
    CROSS APPLY openjson (rowset.[processNote.JSON]) with (
        [processNote.id]               NVARCHAR(100)       '$.id',
        [processNote.extension]        NVARCHAR(MAX)       '$.extension',
        [processNote.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [processNote.number]           bigint              '$.number',
        [processNote.type]             NVARCHAR(64)        '$.type',
        [processNote.text]             NVARCHAR(4000)      '$.text',
        [processNote.language.id]      NVARCHAR(100)       '$.language.id',
        [processNote.language.extension] NVARCHAR(MAX)       '$.language.extension',
        [processNote.language.coding]  NVARCHAR(MAX)       '$.language.coding',
        [processNote.language.text]    NVARCHAR(4000)      '$.language.text'
    ) j

GO

CREATE VIEW fhir.ClaimResponseCommunicationRequest AS
SELECT
    [id],
    [communicationRequest.JSON],
    [communicationRequest.id],
    [communicationRequest.extension],
    [communicationRequest.reference],
    [communicationRequest.type],
    [communicationRequest.identifier.id],
    [communicationRequest.identifier.extension],
    [communicationRequest.identifier.use],
    [communicationRequest.identifier.type],
    [communicationRequest.identifier.system],
    [communicationRequest.identifier.value],
    [communicationRequest.identifier.period],
    [communicationRequest.identifier.assigner],
    [communicationRequest.display]
FROM openrowset (
        BULK 'ClaimResponse/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [communicationRequest.JSON]  VARCHAR(MAX) '$.communicationRequest'
    ) AS rowset
    CROSS APPLY openjson (rowset.[communicationRequest.JSON]) with (
        [communicationRequest.id]      NVARCHAR(100)       '$.id',
        [communicationRequest.extension] NVARCHAR(MAX)       '$.extension',
        [communicationRequest.reference] NVARCHAR(4000)      '$.reference',
        [communicationRequest.type]    VARCHAR(256)        '$.type',
        [communicationRequest.identifier.id] NVARCHAR(100)       '$.identifier.id',
        [communicationRequest.identifier.extension] NVARCHAR(MAX)       '$.identifier.extension',
        [communicationRequest.identifier.use] NVARCHAR(64)        '$.identifier.use',
        [communicationRequest.identifier.type] NVARCHAR(MAX)       '$.identifier.type',
        [communicationRequest.identifier.system] VARCHAR(256)        '$.identifier.system',
        [communicationRequest.identifier.value] NVARCHAR(4000)      '$.identifier.value',
        [communicationRequest.identifier.period] NVARCHAR(MAX)       '$.identifier.period',
        [communicationRequest.identifier.assigner] NVARCHAR(MAX)       '$.identifier.assigner',
        [communicationRequest.display] NVARCHAR(4000)      '$.display'
    ) j

GO

CREATE VIEW fhir.ClaimResponseInsurance AS
SELECT
    [id],
    [insurance.JSON],
    [insurance.id],
    [insurance.extension],
    [insurance.modifierExtension],
    [insurance.sequence],
    [insurance.focal],
    [insurance.coverage.id],
    [insurance.coverage.extension],
    [insurance.coverage.reference],
    [insurance.coverage.type],
    [insurance.coverage.identifier],
    [insurance.coverage.display],
    [insurance.businessArrangement],
    [insurance.claimResponse.id],
    [insurance.claimResponse.extension],
    [insurance.claimResponse.reference],
    [insurance.claimResponse.type],
    [insurance.claimResponse.identifier],
    [insurance.claimResponse.display]
FROM openrowset (
        BULK 'ClaimResponse/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [insurance.JSON]  VARCHAR(MAX) '$.insurance'
    ) AS rowset
    CROSS APPLY openjson (rowset.[insurance.JSON]) with (
        [insurance.id]                 NVARCHAR(100)       '$.id',
        [insurance.extension]          NVARCHAR(MAX)       '$.extension',
        [insurance.modifierExtension]  NVARCHAR(MAX)       '$.modifierExtension',
        [insurance.sequence]           bigint              '$.sequence',
        [insurance.focal]              bit                 '$.focal',
        [insurance.coverage.id]        NVARCHAR(100)       '$.coverage.id',
        [insurance.coverage.extension] NVARCHAR(MAX)       '$.coverage.extension',
        [insurance.coverage.reference] NVARCHAR(4000)      '$.coverage.reference',
        [insurance.coverage.type]      VARCHAR(256)        '$.coverage.type',
        [insurance.coverage.identifier] NVARCHAR(MAX)       '$.coverage.identifier',
        [insurance.coverage.display]   NVARCHAR(4000)      '$.coverage.display',
        [insurance.businessArrangement] NVARCHAR(4000)      '$.businessArrangement',
        [insurance.claimResponse.id]   NVARCHAR(100)       '$.claimResponse.id',
        [insurance.claimResponse.extension] NVARCHAR(MAX)       '$.claimResponse.extension',
        [insurance.claimResponse.reference] NVARCHAR(4000)      '$.claimResponse.reference',
        [insurance.claimResponse.type] VARCHAR(256)        '$.claimResponse.type',
        [insurance.claimResponse.identifier] NVARCHAR(MAX)       '$.claimResponse.identifier',
        [insurance.claimResponse.display] NVARCHAR(4000)      '$.claimResponse.display'
    ) j

GO

CREATE VIEW fhir.ClaimResponseError AS
SELECT
    [id],
    [error.JSON],
    [error.id],
    [error.extension],
    [error.modifierExtension],
    [error.itemSequence],
    [error.detailSequence],
    [error.subDetailSequence],
    [error.code.id],
    [error.code.extension],
    [error.code.coding],
    [error.code.text]
FROM openrowset (
        BULK 'ClaimResponse/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [error.JSON]  VARCHAR(MAX) '$.error'
    ) AS rowset
    CROSS APPLY openjson (rowset.[error.JSON]) with (
        [error.id]                     NVARCHAR(100)       '$.id',
        [error.extension]              NVARCHAR(MAX)       '$.extension',
        [error.modifierExtension]      NVARCHAR(MAX)       '$.modifierExtension',
        [error.itemSequence]           bigint              '$.itemSequence',
        [error.detailSequence]         bigint              '$.detailSequence',
        [error.subDetailSequence]      bigint              '$.subDetailSequence',
        [error.code.id]                NVARCHAR(100)       '$.code.id',
        [error.code.extension]         NVARCHAR(MAX)       '$.code.extension',
        [error.code.coding]            NVARCHAR(MAX)       '$.code.coding',
        [error.code.text]              NVARCHAR(4000)      '$.code.text'
    ) j
