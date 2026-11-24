-- pass full path to plan_result.json file
call DCM_DEMO.TOOLS.CHECK_DOWNSTREAM_IMPACT('snow://workspace/USER$JSOMMERFELD.PUBLIC.JS_SNOWBOX/versions/head/DCM_DEMOS/2_DCM_Pipeline_Demo/DCM_Platform_Demo/out/plan_result.json')
;

--setup 
create schema if not exists DCM_DEMO.TOOLS;

create or replace procedure DCM_DEMO.TOOLS.CHECK_DOWNSTREAM_IMPACT(
	PLAN_FILE varchar
)
	returns table (
		CHANGED_OBJECT varchar,
		IMPACTED_OBJECT varchar,
		ISSUE varchar
	)
	language python
	runtime_version = '3.11'
	packages = ('snowflake-snowpark-python')
	handler = 'run'
	execute as caller
as
$$
import json, re

RELATION_DOMAINS = {'TABLE', 'VIEW', 'DYNAMIC_TABLE', 'MATERIALIZED_VIEW', 'EXTERNAL_TABLE', 'ICEBERG_TABLE', 'EVENT_TABLE'}
# Consumers whose definition is re-resolved against the source on every read/refresh.
DEFINITION_CONSUMERS = {'VIEW', 'DYNAMIC_TABLE', 'MATERIALIZED_VIEW', 'SECURE_VIEW', 'SEMANTIC_VIEW'}
DDL_DOMAIN = {'VIEW': 'VIEW', 'DYNAMIC_TABLE': 'DYNAMIC_TABLE', 'MATERIALIZED_VIEW': 'VIEW'}
SEVERITY_ORDER = {'BREAKING': 0, 'LIKELY_BREAKING': 1, 'WARNING': 2, 'INFO': 3}


def unq(name):
	return '.'.join(p.strip('"') for p in re.findall(r'"[^"]*"|[^.]+', name or ''))


def lit(s):
	return "'" + str(s).replace("\\", "\\\\").replace("'", "''") + "'"


def load_plan(session, path):
	with session.file.get_stream(path.strip()) as f:
		return json.loads(f.read())


def project_entities(session, project):
	if not project:
		return set()
	return {e['name'].upper() for e in session.sql(f'show entities in dcm project {project}').collect()}


def lineage(session, name, domain, distance):
	rows = session.sql(
		f"select * from table(SNOWFLAKE.CORE.GET_LINEAGE({lit(name)}, {lit(domain)}, 'DOWNSTREAM', {int(distance)}))"
	).collect()
	out = []
	for r in rows:
		d = r.as_dict()
		d['TGT'] = f"{d['TARGET_OBJECT_DATABASE']}.{d['TARGET_OBJECT_SCHEMA']}.{d['TARGET_OBJECT_NAME']}".upper()
		d['SRC'] = f"{d['SOURCE_OBJECT_DATABASE']}.{d['SOURCE_OBJECT_SCHEMA']}.{d['SOURCE_OBJECT_NAME']}".upper()
		if d.get('TARGET_STATUS') not in (None, 'ACTIVE'):
			continue
		out.append(d)
	return out


_ddl_cache = {}
def get_ddl(session, fqn, domain):
	key = (fqn, domain)
	if key not in _ddl_cache:
		try:
			_ddl_cache[key] = session.sql(f"select get_ddl({lit(DDL_DOMAIN.get(domain, 'TABLE'))}, {lit(fqn)}) as D").collect()[0]['D']
		except Exception:
			_ddl_cache[key] = None
	return _ddl_cache[key]


def body_of(ddl):
	m = re.search(r'\bas\b(.*)$', ddl or '', re.I | re.S)
	return m.group(1) if m else (ddl or '')


def uses_star(ddl):
	return bool(re.search(r'\bselect\s+(distinct\s+)?([A-Za-z0-9_$"]+\.)?\*', body_of(ddl), re.I))


def references_column(ddl, col):
	return bool(re.search(r'(?<![A-Za-z0-9_$])"?' + re.escape(col) + r'"?(?![A-Za-z0-9_$])', body_of(ddl), re.I))


def parse_type(t):
	m = re.match(r'\s*([A-Z_ ]+?)\s*(\(([^)]*)\))?\s*$', (t or '').upper())
	if not m:
		return (t, [])
	args = [int(a) for a in re.findall(r'\d+', m.group(3) or '')]
	return (m.group(1).strip(), args)


def is_widening(prev, new):
	(pf, pa), (nf, na) = parse_type(prev), parse_type(new)
	num = {'NUMBER', 'DECIMAL', 'NUMERIC'}
	txt = {'VARCHAR', 'STRING', 'TEXT', 'CHAR', 'CHARACTER'}
	if pf in num and nf in num and len(pa) == 2 and len(na) == 2:
		return na[1] >= pa[1] and (na[0] - na[1]) >= (pa[0] - pa[1])
	if pf in txt and nf in txt and len(pa) == 1 and len(na) == 1:
		return na[0] >= pa[0]
	return pf == nf and pa == na


def extract_changes(plan):
	"""Normalize the PLAN changeset into object drops and column-level changes."""
	drops, cols = [], []
	for c in plan.get('changeset', []):
		oid = c.get('object_id', {})
		domain = (oid.get('domain') or '').upper()
		if domain not in RELATION_DOMAINS:
			continue
		fqn = unq(oid.get('fqn')).upper()
		if c.get('type') == 'DROP':
			drops.append((fqn, domain))
		elif c.get('type') == 'ALTER':
			for coll in c.get('changes', []):
				if coll.get('collection_name') != 'columns':
					continue
				for item in coll.get('changes', []):
					col = str(item.get('item_id')).strip('"').upper()
					attrs = {a.get('attribute_name'): a for a in item.get('changes', [])}
					cols.append((fqn, domain, col, item.get('kind'), attrs))
	return drops, cols


def run(session, plan_file):
	plan = load_plan(session, plan_file)
	project = unq(plan.get('metadata', {}).get('project_name', '')).upper()
	in_project = project_entities(session, project)
	impacted = {}
	evidence_label = {'OBJECT_LINEAGE': 'object lineage', 'COLUMN_LINEAGE': 'column lineage',
		'SELECT_STAR': 'SELECT * in definition', 'DDL_REFERENCE': 'column name found in definition'}

	def add(sev, change, obj, col, cons, cdom, ccol, dist, via, evidence, reason):
		if sev == 'INFO' or cons in in_project:  # INFO = type widening; in-project consumers are covered by the PLAN itself
			return
		changed = f'{obj}.{col}' if col else obj
		target = f'{cdom} column {ccol}' if ccol and ccol != '*' else cdom
		issue = f'{sev}: {change} affects {target} (detected via {evidence_label[evidence]}). {reason}'
		issues = impacted.setdefault((changed, cons), [])
		if issue not in issues:
			issues.append(issue)

	drops, cols = extract_changes(plan)

	# Layer 3: object-level lineage for dropped relations (includes transitive consumers).
	for fqn, domain in drops:
		for r in lineage(session, fqn, 'TABLE', 1):
			dom = r['TARGET_OBJECT_DOMAIN']
			if r['DISTANCE'] == 1:
				if dom in DEFINITION_CONSUMERS:
					add('BREAKING', f'DROP {domain}', fqn, None, r['TGT'], dom, None, 1, None, 'OBJECT_LINEAGE',
						f'{dom} reads directly from the dropped object and will fail on next read/refresh.')
				else:
					add('WARNING', f'DROP {domain}', fqn, None, r['TGT'], dom, None, 1, None, 'OBJECT_LINEAGE',
						'Data was copied from the dropped object (CTAS/INSERT/COPY). Existing data stays, the process that loads it will fail.')
			else:
				sev = 'BREAKING' if dom in DEFINITION_CONSUMERS else 'WARNING'
				add(sev, f'DROP {domain}', fqn, None, r['TGT'], dom, None, r['DISTANCE'], r['SRC'], 'OBJECT_LINEAGE',
					f'Transitive consumer via {r["SRC"]}.')

	# Column changes, grouped per table so object-level dependents are fetched once.
	by_table = {}
	for fqn, domain, col, kind, attrs in cols:
		by_table.setdefault(fqn, []).append((domain, col, kind, attrs))

	for fqn, changes in by_table.items():
		direct = [r for r in lineage(session, fqn, 'TABLE', 1) if r['TARGET_OBJECT_DOMAIN'] in DEFINITION_CONSUMERS]
		for domain, col, kind, attrs in changes:
			if kind == 'removed':
				change = f'DROP COLUMN'
				hit = set()
				# Layer 1: column lineage (projected columns only).
				for r in lineage(session, f'{fqn}.{col}', 'COLUMN', 1):
					dom = r['TARGET_OBJECT_DOMAIN']
					hit.add(r['TGT'])
					if dom in DEFINITION_CONSUMERS:
						add('BREAKING', change, fqn, col, r['TGT'], dom, r['TARGET_COLUMN_NAME'], r['DISTANCE'],
							None if r['DISTANCE'] == 1 else r['SRC'], 'COLUMN_LINEAGE',
							'Consumer column is derived from the dropped column.')
					else:
						add('WARNING', change, fqn, col, r['TGT'], dom, r['TARGET_COLUMN_NAME'], r['DISTANCE'],
							None if r['DISTANCE'] == 1 else r['SRC'], 'COLUMN_LINEAGE',
							'Column data was copied from the dropped column. The loading process will fail.')
				# Layer 2: DDL scan for filter/join-only references and SELECT * views.
				for r in direct:
					ddl = get_ddl(session, r['TGT'], r['TARGET_OBJECT_DOMAIN'])
					if r['TARGET_OBJECT_DOMAIN'] in ('VIEW', 'MATERIALIZED_VIEW') and uses_star(ddl):
						add('BREAKING', change, fqn, col, r['TGT'], r['TARGET_OBJECT_DOMAIN'], '*', 1, None, 'SELECT_STAR',
							'SELECT * view has a frozen column list; any column count change on the source invalidates it.')
					elif r['TGT'] not in hit and references_column(ddl, col):
						add('LIKELY_BREAKING', change, fqn, col, r['TGT'], r['TARGET_OBJECT_DOMAIN'], None, 1, None, 'DDL_REFERENCE',
							'Column name appears in the consumer definition (e.g. WHERE/JOIN) but not in column lineage.')
			elif kind == 'added':
				for r in direct:
					ddl = get_ddl(session, r['TGT'], r['TARGET_OBJECT_DOMAIN'])
					if r['TARGET_OBJECT_DOMAIN'] in ('VIEW', 'MATERIALIZED_VIEW') and uses_star(ddl):
						add('BREAKING', 'ADD COLUMN', fqn, col, r['TGT'], r['TARGET_OBJECT_DOMAIN'], '*', 1, None, 'SELECT_STAR',
							'SELECT * view has a frozen column list; the added column invalidates it until the view is recreated.')
			elif kind == 'modified' and 'datatype' in attrs:
				prev, new = attrs['datatype'].get('prev_value'), attrs['datatype'].get('value')
				widening = is_widening(prev, new)
				sev = 'INFO' if widening else 'WARNING'
				reason = (f'Type widened {prev} -> {new}; consumers keep working.' if widening
					else f'Type changed {prev} -> {new}; consumers may fail on casts, truncation or comparisons.')
				for r in lineage(session, f'{fqn}.{col}', 'COLUMN', 1):
					add(sev, 'ALTER COLUMN TYPE', fqn, col, r['TGT'], r['TARGET_OBJECT_DOMAIN'], r['TARGET_COLUMN_NAME'], r['DISTANCE'],
						None if r['DISTANCE'] == 1 else r['SRC'], 'COLUMN_LINEAGE', reason)

	rows = [(c, i, ' | '.join(v)) for (c, i), v in sorted(impacted.items())]
	if not rows:
		rows = [(None, None, 'No impacted downstream objects found outside the project.')]
	return session.create_dataframe(rows, schema=['CHANGED_OBJECT', 'IMPACTED_OBJECT', 'ISSUE'])
$$;
