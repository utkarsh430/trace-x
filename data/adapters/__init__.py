"""`SourceAdapter` implementations (ADR-0022).

Every inbound dataset is mapped onto `CanonicalTransaction` by an adapter that
declares its `field_coverage`. A feature whose `required_fields` are not covered
evaluates to UNAVAILABLE and propagates as null — never imputed to zero, which
would fabricate signal and make cross-dataset transfer metrics meaningless.

`GeneratorAdapter` (Track A, full coverage) lands in Phase 1; `IeeeCisAdapter`
(Track B, partial coverage) in Phase 4B. Both must pass the one shared
`SourceAdapterConformanceSuite`.
"""
