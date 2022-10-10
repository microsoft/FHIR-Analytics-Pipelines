CREATE EXTERNAL TABLE [fhir].[Immunization] (
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
    [instantiatesCanonical] VARCHAR(MAX),
    [instantiatesUri] VARCHAR(MAX),
    [basedOn] VARCHAR(MAX),
    [status] NVARCHAR(100),
    [statusReason.id] NVARCHAR(100),
    [statusReason.extension] NVARCHAR(MAX),
    [statusReason.coding] VARCHAR(MAX),
    [statusReason.text] NVARCHAR(4000),
    [vaccineCode.id] NVARCHAR(100),
    [vaccineCode.extension] NVARCHAR(MAX),
    [vaccineCode.coding] VARCHAR(MAX),
    [vaccineCode.text] NVARCHAR(4000),
    [manufacturer.id] NVARCHAR(100),
    [manufacturer.extension] NVARCHAR(MAX),
    [manufacturer.reference] NVARCHAR(4000),
    [manufacturer.type] VARCHAR(256),
    [manufacturer.identifier.id] NVARCHAR(100),
    [manufacturer.identifier.extension] NVARCHAR(MAX),
    [manufacturer.identifier.use] NVARCHAR(64),
    [manufacturer.identifier.type] NVARCHAR(MAX),
    [manufacturer.identifier.system] VARCHAR(256),
    [manufacturer.identifier.value] NVARCHAR(4000),
    [manufacturer.identifier.period] NVARCHAR(MAX),
    [manufacturer.identifier.assigner] NVARCHAR(MAX),
    [manufacturer.display] NVARCHAR(4000),
    [lotNumber] NVARCHAR(100),
    [expirationDate] VARCHAR(64),
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
    [recorded] VARCHAR(64),
    [primarySource] bit,
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
    [site.id] NVARCHAR(100),
    [site.extension] NVARCHAR(MAX),
    [site.coding] VARCHAR(MAX),
    [site.text] NVARCHAR(4000),
    [route.id] NVARCHAR(100),
    [route.extension] NVARCHAR(MAX),
    [route.coding] VARCHAR(MAX),
    [route.text] NVARCHAR(4000),
    [doseQuantity.id] NVARCHAR(100),
    [doseQuantity.extension] NVARCHAR(MAX),
    [doseQuantity.value] float,
    [doseQuantity.comparator] NVARCHAR(64),
    [doseQuantity.unit] NVARCHAR(100),
    [doseQuantity.system] VARCHAR(256),
    [doseQuantity.code] NVARCHAR(4000),
    [performer] VARCHAR(MAX),
    [note] VARCHAR(MAX),
    [reason] VARCHAR(MAX),
    [isSubpotent] bit,
    [subpotentReason] VARCHAR(MAX),
    [education] VARCHAR(MAX),
    [programEligibility] VARCHAR(MAX),
    [fundingSource.id] NVARCHAR(100),
    [fundingSource.extension] NVARCHAR(MAX),
    [fundingSource.coding] VARCHAR(MAX),
    [fundingSource.text] NVARCHAR(4000),
    [reaction] VARCHAR(MAX),
    [protocolApplied] VARCHAR(MAX),
    [occurrence.dateTime] VARCHAR(64),
    [occurrence.string] NVARCHAR(4000),
    [informationSource.codeableConcept.id] NVARCHAR(100),
    [informationSource.codeableConcept.extension] NVARCHAR(MAX),
    [informationSource.codeableConcept.coding] VARCHAR(MAX),
    [informationSource.codeableConcept.text] NVARCHAR(4000),
    [informationSource.reference.id] NVARCHAR(100),
    [informationSource.reference.extension] NVARCHAR(MAX),
    [informationSource.reference.reference] NVARCHAR(4000),
    [informationSource.reference.type] VARCHAR(256),
    [informationSource.reference.identifier.id] NVARCHAR(100),
    [informationSource.reference.identifier.extension] NVARCHAR(MAX),
    [informationSource.reference.identifier.use] NVARCHAR(64),
    [informationSource.reference.identifier.type] NVARCHAR(MAX),
    [informationSource.reference.identifier.system] VARCHAR(256),
    [informationSource.reference.identifier.value] NVARCHAR(4000),
    [informationSource.reference.identifier.period] NVARCHAR(MAX),
    [informationSource.reference.identifier.assigner] NVARCHAR(MAX),
    [informationSource.reference.display] NVARCHAR(4000),
) WITH (
    LOCATION='/Immunization/**',
    DATA_SOURCE = ParquetSource,
    FILE_FORMAT = ParquetFormat
);

GO

CREATE VIEW fhir.ImmunizationIdentifier AS
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
        BULK 'Immunization/**',
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

CREATE VIEW fhir.ImmunizationInstantiatesCanonical AS
SELECT
    [id],
    [instantiatesCanonical.JSON],
    [instantiatesCanonical]
FROM openrowset (
        BULK 'Immunization/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [instantiatesCanonical.JSON]  VARCHAR(MAX) '$.instantiatesCanonical'
    ) AS rowset
    CROSS APPLY openjson (rowset.[instantiatesCanonical.JSON]) with (
        [instantiatesCanonical]        NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.ImmunizationInstantiatesUri AS
SELECT
    [id],
    [instantiatesUri.JSON],
    [instantiatesUri]
FROM openrowset (
        BULK 'Immunization/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [instantiatesUri.JSON]  VARCHAR(MAX) '$.instantiatesUri'
    ) AS rowset
    CROSS APPLY openjson (rowset.[instantiatesUri.JSON]) with (
        [instantiatesUri]              NVARCHAR(MAX)       '$'
    ) j

GO

CREATE VIEW fhir.ImmunizationBasedOn AS
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
        BULK 'Immunization/**',
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

CREATE VIEW fhir.ImmunizationPerformer AS
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
        BULK 'Immunization/**',
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

CREATE VIEW fhir.ImmunizationNote AS
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
        BULK 'Immunization/**',
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

CREATE VIEW fhir.ImmunizationReason AS
SELECT
    [id],
    [reason.JSON],
    [reason.id],
    [reason.extension],
    [reason.concept.id],
    [reason.concept.extension],
    [reason.concept.coding],
    [reason.concept.text],
    [reason.reference.id],
    [reason.reference.extension],
    [reason.reference.reference],
    [reason.reference.type],
    [reason.reference.identifier],
    [reason.reference.display]
FROM openrowset (
        BULK 'Immunization/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [reason.JSON]  VARCHAR(MAX) '$.reason'
    ) AS rowset
    CROSS APPLY openjson (rowset.[reason.JSON]) with (
        [reason.id]                    NVARCHAR(100)       '$.id',
        [reason.extension]             NVARCHAR(MAX)       '$.extension',
        [reason.concept.id]            NVARCHAR(100)       '$.concept.id',
        [reason.concept.extension]     NVARCHAR(MAX)       '$.concept.extension',
        [reason.concept.coding]        NVARCHAR(MAX)       '$.concept.coding',
        [reason.concept.text]          NVARCHAR(4000)      '$.concept.text',
        [reason.reference.id]          NVARCHAR(100)       '$.reference.id',
        [reason.reference.extension]   NVARCHAR(MAX)       '$.reference.extension',
        [reason.reference.reference]   NVARCHAR(4000)      '$.reference.reference',
        [reason.reference.type]        VARCHAR(256)        '$.reference.type',
        [reason.reference.identifier]  NVARCHAR(MAX)       '$.reference.identifier',
        [reason.reference.display]     NVARCHAR(4000)      '$.reference.display'
    ) j

GO

CREATE VIEW fhir.ImmunizationSubpotentReason AS
SELECT
    [id],
    [subpotentReason.JSON],
    [subpotentReason.id],
    [subpotentReason.extension],
    [subpotentReason.coding],
    [subpotentReason.text]
FROM openrowset (
        BULK 'Immunization/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [subpotentReason.JSON]  VARCHAR(MAX) '$.subpotentReason'
    ) AS rowset
    CROSS APPLY openjson (rowset.[subpotentReason.JSON]) with (
        [subpotentReason.id]           NVARCHAR(100)       '$.id',
        [subpotentReason.extension]    NVARCHAR(MAX)       '$.extension',
        [subpotentReason.coding]       NVARCHAR(MAX)       '$.coding' AS JSON,
        [subpotentReason.text]         NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.ImmunizationEducation AS
SELECT
    [id],
    [education.JSON],
    [education.id],
    [education.extension],
    [education.modifierExtension],
    [education.documentType],
    [education.reference],
    [education.publicationDate],
    [education.presentationDate]
FROM openrowset (
        BULK 'Immunization/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [education.JSON]  VARCHAR(MAX) '$.education'
    ) AS rowset
    CROSS APPLY openjson (rowset.[education.JSON]) with (
        [education.id]                 NVARCHAR(100)       '$.id',
        [education.extension]          NVARCHAR(MAX)       '$.extension',
        [education.modifierExtension]  NVARCHAR(MAX)       '$.modifierExtension',
        [education.documentType]       NVARCHAR(100)       '$.documentType',
        [education.reference]          VARCHAR(256)        '$.reference',
        [education.publicationDate]    VARCHAR(64)         '$.publicationDate',
        [education.presentationDate]   VARCHAR(64)         '$.presentationDate'
    ) j

GO

CREATE VIEW fhir.ImmunizationProgramEligibility AS
SELECT
    [id],
    [programEligibility.JSON],
    [programEligibility.id],
    [programEligibility.extension],
    [programEligibility.coding],
    [programEligibility.text]
FROM openrowset (
        BULK 'Immunization/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [programEligibility.JSON]  VARCHAR(MAX) '$.programEligibility'
    ) AS rowset
    CROSS APPLY openjson (rowset.[programEligibility.JSON]) with (
        [programEligibility.id]        NVARCHAR(100)       '$.id',
        [programEligibility.extension] NVARCHAR(MAX)       '$.extension',
        [programEligibility.coding]    NVARCHAR(MAX)       '$.coding' AS JSON,
        [programEligibility.text]      NVARCHAR(4000)      '$.text'
    ) j

GO

CREATE VIEW fhir.ImmunizationReaction AS
SELECT
    [id],
    [reaction.JSON],
    [reaction.id],
    [reaction.extension],
    [reaction.modifierExtension],
    [reaction.date],
    [reaction.detail.id],
    [reaction.detail.extension],
    [reaction.detail.reference],
    [reaction.detail.type],
    [reaction.detail.identifier],
    [reaction.detail.display],
    [reaction.reported]
FROM openrowset (
        BULK 'Immunization/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [reaction.JSON]  VARCHAR(MAX) '$.reaction'
    ) AS rowset
    CROSS APPLY openjson (rowset.[reaction.JSON]) with (
        [reaction.id]                  NVARCHAR(100)       '$.id',
        [reaction.extension]           NVARCHAR(MAX)       '$.extension',
        [reaction.modifierExtension]   NVARCHAR(MAX)       '$.modifierExtension',
        [reaction.date]                VARCHAR(64)         '$.date',
        [reaction.detail.id]           NVARCHAR(100)       '$.detail.id',
        [reaction.detail.extension]    NVARCHAR(MAX)       '$.detail.extension',
        [reaction.detail.reference]    NVARCHAR(4000)      '$.detail.reference',
        [reaction.detail.type]         VARCHAR(256)        '$.detail.type',
        [reaction.detail.identifier]   NVARCHAR(MAX)       '$.detail.identifier',
        [reaction.detail.display]      NVARCHAR(4000)      '$.detail.display',
        [reaction.reported]            bit                 '$.reported'
    ) j

GO

CREATE VIEW fhir.ImmunizationProtocolApplied AS
SELECT
    [id],
    [protocolApplied.JSON],
    [protocolApplied.id],
    [protocolApplied.extension],
    [protocolApplied.modifierExtension],
    [protocolApplied.series],
    [protocolApplied.authority.id],
    [protocolApplied.authority.extension],
    [protocolApplied.authority.reference],
    [protocolApplied.authority.type],
    [protocolApplied.authority.identifier],
    [protocolApplied.authority.display],
    [protocolApplied.targetDisease],
    [protocolApplied.doseNumber],
    [protocolApplied.seriesDoses]
FROM openrowset (
        BULK 'Immunization/**',
        DATA_SOURCE = 'ParquetSource',
        FORMAT = 'PARQUET'
    ) WITH (
        [id]   VARCHAR(64),
       [protocolApplied.JSON]  VARCHAR(MAX) '$.protocolApplied'
    ) AS rowset
    CROSS APPLY openjson (rowset.[protocolApplied.JSON]) with (
        [protocolApplied.id]           NVARCHAR(100)       '$.id',
        [protocolApplied.extension]    NVARCHAR(MAX)       '$.extension',
        [protocolApplied.modifierExtension] NVARCHAR(MAX)       '$.modifierExtension',
        [protocolApplied.series]       NVARCHAR(4000)      '$.series',
        [protocolApplied.authority.id] NVARCHAR(100)       '$.authority.id',
        [protocolApplied.authority.extension] NVARCHAR(MAX)       '$.authority.extension',
        [protocolApplied.authority.reference] NVARCHAR(4000)      '$.authority.reference',
        [protocolApplied.authority.type] VARCHAR(256)        '$.authority.type',
        [protocolApplied.authority.identifier] NVARCHAR(MAX)       '$.authority.identifier',
        [protocolApplied.authority.display] NVARCHAR(4000)      '$.authority.display',
        [protocolApplied.targetDisease] NVARCHAR(MAX)       '$.targetDisease' AS JSON,
        [protocolApplied.doseNumber]   NVARCHAR(4000)      '$.doseNumber',
        [protocolApplied.seriesDoses]  NVARCHAR(4000)      '$.seriesDoses'
    ) j
