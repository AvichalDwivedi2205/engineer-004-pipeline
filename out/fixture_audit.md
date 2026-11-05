# Fixture audit: event_sample.jsonl

sha256 `1aeb24b415009e89fcf8acb5a178410faf216dc17b16920d9849ecc8bbb24235`

records_in=25  buckets={'accepted': 21, 'duplicate': 1, 'quarantined': 1, 'control': 1, 'dead_letter': 1}  invariant_balanced=True

## Findings by class (gate output)

| class | event_ids (line) |
|---|---|
| bot_rate | evt-0015 (L16) |
| bot_referrer | evt-0012 (L13), evt-0013 (L14), evt-0014 (L15), evt-0015 (L16) |
| client_clock_ahead | evt-0005 (L6), evt-0006 (L7), evt-0016 (L17) |
| identity_candidate | evt-0016 (L17), evt-0024 (L25) |
| legacy_schema_mapped | evt-0009 (L10) |
| no_state_update | evt-0005 (L6), evt-0006 (L7), evt-0009 (L10), evt-0016 (L17) |
| no_trigger | evt-0005 (L6), evt-0006 (L7), evt-0009 (L10), evt-0016 (L17) |
| partition_time_unverified | evt-0009 (L10) |
| pii_redacted | evt-0007 (L8) |
| privacy_request_pending_verification | evt-0017 (L18) |
| received_at_missing | evt-0009 (L10) |
| same_id_same_content | evt-0002 (L5) |
| tenant_unresolved | evt-0011 (L12) |
| unparseable: JSONDecodeError | evt-0020 (L21) |

## Audit-only cross-checks

- Arrival-order breaks (file order vs received_at): [{'line': 5, 'event_id': 'evt-0002', 'received_at': '2026-06-15T14:02:27.442Z', 'previous_line': 4}]
- Duplicate evt-0002 (L5) re-sent 7428 ms after the first copy
- Transit ms for sane clients: {'n': 17, 'min': 119, 'median': 163, 'max': 206}
- Clock ahead: evt-0005 client-server = 47190 ms (2026-06-15T14:05:00.120Z vs 2026-06-15T14:04:12.930000+00:00)
- Clock ahead: evt-0006 client-server = 3900000 ms (2026-06-15T15:11:03.552Z vs 2026-06-15T14:06:03.552000+00:00)
- Clock ahead: evt-0016 client-server = 31535999788 ms (2027-06-15T14:11:27.310Z vs 2026-06-15T14:11:27.522000+00:00)
- Client aggregate evt-0019 claims viewed_pricing.count_today=3 for u-5511; non-bot raw page views in sample: ['evt-0001']
- /pricing page views by tenant: {'naive_exact_type_match': {'t-042': ['evt-0001', 'evt-0012'], 't-017': ['evt-0021']}, 'after_gate': {'t-042': ['evt-0001', 'evt-0009'], 't-017': ['evt-0021']}, 'rollup_view_filter': {'t-042': ['evt-0001', 'evt-0009'], 't-017': ['evt-0021']}}
- DSR evt-0017 for u-1077: by user_id -> []; only via the request's anon id -> ['evt-0006']; anon link confirmed by an identify event: False
- PII ['contact_email', 'phone'] in evt-0007 becomes attributable to u-7304 after stitching
- Identity links: anon-9f2->u-5511 via identify (evt-0003), anon-c81->u-2209 via identify (evt-0008), anon-3d0->u-7304 via identify (evt-0022)
- Identity candidates (not merged): anon-52d->u-8842 via custom (evt-0016), anon-52d->u-8842 via page_view (evt-0024)
- Bot visitors: ['t-042/anon-8fc']
- DSR tickets: [('evt-0017', 'pending_tenant_verification')]
- Event-id gaps: []  tenant mix: {'t-042': 12, 't-017': 7, 't-088': 4, 'None': 1}
