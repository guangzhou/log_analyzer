# -*- coding: utf-8 -*-
from datetime import datetime
from .db import Database
def merge_to_summary(db: Database):
    now=datetime.utcnow().isoformat()
    rows=db.query('SELECT * FROM REGEX_TEMPLATE')
    for r in rows:
        db.execute('''
        INSERT INTO SUMMARY_REGEX_TEMPLATE(template_id, pattern, sample_log, normalized_sample, version, is_active, semantic_info, aggregated_at, total_match_count, first_seen_global, last_seen_global)
        VALUES(?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(template_id) DO UPDATE SET
          pattern=excluded.pattern,
          sample_log=excluded.sample_log,
          normalized_sample=excluded.normalized_sample,
          version=excluded.version,
          is_active=excluded.is_active,
          semantic_info=excluded.semantic_info,
          aggregated_at=excluded.aggregated_at,
          total_match_count=excluded.total_match_count,
          first_seen_global=excluded.first_seen_global,
          last_seen_global=excluded.last_seen_global
        ''', (r['template_id'], r['pattern'], r['sample_log'], r['normalized_sample'], r['version'], r['is_active'], r['semantic_info'], now, r['match_count'], r['first_seen'], r['last_seen']))
    a=db.query('SELECT template_id, mod, smod, SUM(observed_count) c, MIN(first_seen_in_ctx) fmin, MAX(last_seen_in_ctx) fmax FROM TEMPLATE_APPLICABILITY GROUP BY template_id, mod, smod')
    for it in a:
        db.execute('''
        INSERT INTO SUMMARY_TEMPLATE_APPLICABILITY(template_id, mod, smod, total_count, first_seen_in_ctx, last_seen_in_ctx, source, last_updated)
        VALUES(?,?,?,?,?,?,?,?)
        ON CONFLICT(template_id, mod, smod, source) DO UPDATE SET
          total_count=excluded.total_count,
          last_seen_in_ctx=excluded.last_seen_in_ctx,
          last_updated=excluded.last_updated
        ''', (it['template_id'], it['mod'], it['smod'], it['c'] or 0, it['fmin'], it['fmax'], 'mixed', now))
