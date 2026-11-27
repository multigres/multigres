-- Compare logical columns, not attnum: old rows gain new columns at the end.
SELECT string_agg(line, E'\n' ORDER BY line) FROM (
  SELECT 'col ' || c.relname || '.' || a.attname || ' ' || format_type(a.atttypid, a.atttypmod)
         || ' notnull=' || a.attnotnull || ' default=' || COALESCE(pg_get_expr(d.adbin, d.adrelid), '<none>')
         || ' identity=' || a.attidentity::text AS line
    FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
   WHERE n.nspname = 'multigres' AND a.attnum > 0 AND NOT a.attisdropped AND c.relkind IN ('r','v','c')
  UNION ALL SELECT 'con ' || c.relname || '.' || o.conname || ' ' || pg_get_constraintdef(o.oid)
    FROM pg_constraint o JOIN pg_class c ON c.oid = o.conrelid JOIN pg_namespace n ON n.oid = c.relnamespace
   WHERE n.nspname = 'multigres'
  UNION ALL SELECT 'idx ' || pg_get_indexdef(i.indexrelid)
    FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid JOIN pg_namespace n ON n.oid = c.relnamespace
   WHERE n.nspname = 'multigres'
  UNION ALL SELECT 'acl ' || c.relname || ' ' || COALESCE(c.relacl::text, '')
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
   WHERE n.nspname = 'multigres' AND c.relkind IN ('r','v')
  UNION ALL SELECT 'rel ' || c.relname || ' ' || c.relkind::text FROM pg_class c
    WHERE c.relnamespace = 'multigres'::regnamespace
  UNION ALL SELECT 'view ' || c.relname || ' ' || pg_get_viewdef(c.oid)
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
   WHERE n.nspname = 'multigres' AND c.relkind = 'v'
) s;
