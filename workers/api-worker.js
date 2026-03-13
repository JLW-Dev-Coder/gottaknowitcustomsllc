const JSON_HEADERS = {
  'content-type': 'application/json; charset=utf-8',
  'cache-control': 'no-store'
};

const PUBLIC_OPERATOR_FIELDS = [
  'business_address',
  'business_hours',
  'business_name',
  'business_phone',
  'operator_bio',
  'operator_name',
  'profile_slug',
  'profile_type',
  'support_email',
  'updated_at'
];

export default {
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url);

      if (!url.pathname.startsWith('/api/')) {
        return env.ASSETS.fetch(request);
      }

      if (url.pathname === '/api/health' && request.method === 'GET') {
        return jsonResponse({
          ok: true,
          service: 'gki-operator-api',
          timestamp: new Date().toISOString()
        });
      }

      if (url.pathname === '/api/operators' && request.method === 'GET') {
        return handleListOperators(request, env);
      }

      const operatorMatch = url.pathname.match(/^\/api\/operators\/([a-z0-9_-]+)$/i);
      if (operatorMatch) {
        const profileSlug = operatorMatch[1];

        if (request.method === 'GET') {
          return handleGetOperator(request, env, profileSlug);
        }

        if (request.method === 'PUT') {
          return handlePutOperator(request, env, profileSlug);
        }
      }

      return jsonResponse(
        {
          error: 'Not found',
          path: url.pathname
        },
        404
      );
    } catch (error) {
      console.error('Worker error:', error);
      return jsonResponse(
        {
          error: 'Internal server error',
          message: error instanceof Error ? error.message : 'Unknown error'
        },
        500
      );
    }
  }
};

async function handleGetOperator(request, env, profileSlug) {
  const url = new URL(request.url);
  const view = (url.searchParams.get('view') || 'full').toLowerCase();

  const operator = await getOperatorRecord(env, profileSlug);

  if (!operator) {
    return jsonResponse(
      {
        error: 'Operator not found',
        profile_slug: profileSlug
      },
      404
    );
  }

  const payload = view === 'public' ? toPublicOperatorPayload(operator) : operator;

  return jsonResponse({
    ok: true,
    profile_slug: profileSlug,
    source: operator._source || 'unknown',
    data: payload
  });
}

async function handleListOperators(request, env) {
  requireBinding(env, 'DB');

  const url = new URL(request.url);
  const q = (url.searchParams.get('q') || '').trim();
  const limit = clampInt(url.searchParams.get('limit'), 1, 100, 25);

  let statement;
  if (q) {
    const like = `%${q}%`;
    statement = env.DB.prepare(`
      SELECT
        profile_slug,
        profile_type,
        operator_name,
        business_name,
        business_phone,
        support_email,
        business_address,
        updated_at
      FROM operators
      WHERE business_name LIKE ?1
         OR operator_name LIKE ?1
         OR profile_slug LIKE ?1
      ORDER BY updated_at DESC
      LIMIT ?2
    `).bind(like, limit);
  } else {
    statement = env.DB.prepare(`
      SELECT
        profile_slug,
        profile_type,
        operator_name,
        business_name,
        business_phone,
        support_email,
        business_address,
        updated_at
      FROM operators
      ORDER BY updated_at DESC
      LIMIT ?1
    `).bind(limit);
  }

  const result = await statement.all();

  return jsonResponse({
    ok: true,
    count: result.results.length,
    operators: result.results
  });
}

async function handlePutOperator(request, env, profileSlug) {
  requireBinding(env, 'DB');
  requireBinding(env, 'OPERATOR_BLOBS');

  await requireApiToken(request, env);

  const body = await parseJsonBody(request);
  const payload = normalizeOperatorPayload(body, profileSlug);

  validateOperatorPayload(payload);

  const now = new Date().toISOString();
  payload.updated_at = now;

  const latestKey = getLatestBlobKey(profileSlug);
  const versionedKey = getVersionedBlobKey(profileSlug, now);

  await env.OPERATOR_BLOBS.put(latestKey, JSON.stringify(payload, null, 2), {
    httpMetadata: { contentType: 'application/json' }
  });

  await env.OPERATOR_BLOBS.put(versionedKey, JSON.stringify(payload, null, 2), {
    httpMetadata: { contentType: 'application/json' }
  });

  await env.DB.prepare(`
    INSERT INTO operators (
      profile_slug,
      profile_type,
      operator_name,
      business_name,
      business_phone,
      support_email,
      business_address,
      updated_at
    )
    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
    ON CONFLICT(profile_slug) DO UPDATE SET
      profile_type = excluded.profile_type,
      operator_name = excluded.operator_name,
      business_name = excluded.business_name,
      business_phone = excluded.business_phone,
      support_email = excluded.support_email,
      business_address = excluded.business_address,
      updated_at = excluded.updated_at
  `).bind(
    payload.profile_slug,
    payload.profile_type || 'operator',
    payload.operator_name || '',
    payload.business_name || '',
    payload.business_phone || '',
    payload.support_email || '',
    payload.business_address || '',
    payload.updated_at
  ).run();

  const actor = getActorFromRequest(request);

  await env.DB.prepare(`
    INSERT INTO audit_log (
      action,
      actor,
      profile_slug,
      payload_json,
      created_at
    )
    VALUES (?1, ?2, ?3, ?4, ?5)
  `).bind(
    'operator.upsert',
    actor,
    payload.profile_slug,
    JSON.stringify(payload),
    now
  ).run();

  return jsonResponse({
    ok: true,
    message: 'Operator saved',
    profile_slug: payload.profile_slug,
    latest_blob_key: latestKey,
    versioned_blob_key: versionedKey,
    updated_at: now
  });
}

async function getOperatorRecord(env, profileSlug) {
  const fromR2 = await getOperatorFromR2(env, profileSlug);
  if (fromR2) {
    return {
      ...fromR2,
      _source: 'r2'
    };
  }

  const fromStatic = await getOperatorFromStaticJson(env, profileSlug);
  if (fromStatic) {
    return {
      ...fromStatic,
      _source: 'static-json'
    };
  }

  return null;
}

async function getOperatorFromR2(env, profileSlug) {
  if (!env.OPERATOR_BLOBS) return null;

  const key = getLatestBlobKey(profileSlug);
  const object = await env.OPERATOR_BLOBS.get(key);
  if (!object) return null;

  try {
    return await object.json();
  } catch (error) {
    console.error(`Failed to parse R2 JSON for ${profileSlug}:`, error);
    return null;
  }
}

async function getOperatorFromStaticJson(env, profileSlug) {
  if (!env.ASSETS) return null;

  const candidates = [
    `/assets/operator/home/${profileSlug}/data.json`,
    '/assets/operator/home/sample/data.json'
  ];

  for (const path of candidates) {
    const assetRequest = new Request(`https://internal${path}`, {
      method: 'GET'
    });

    const response = await env.ASSETS.fetch(assetRequest);
    if (!response.ok) continue;

    try {
      return await response.json();
    } catch (error) {
      console.error(`Failed to parse static JSON at ${path}:`, error);
    }
  }

  return null;
}

function normalizeOperatorPayload(input, profileSlug) {
  const payload = isPlainObject(input) ? { ...input } : {};

  payload.profile_slug = profileSlug;
  payload.profile_type = typeof payload.profile_type === 'string' && payload.profile_type.trim()
    ? payload.profile_type.trim()
    : 'operator';

  return payload;
}

function validateOperatorPayload(payload) {
  if (!payload.profile_slug || !/^[a-z0-9_-]+$/i.test(payload.profile_slug)) {
    throw new Error('Invalid profile_slug');
  }

  const requiredStringFields = [
    'business_name',
    'operator_name'
  ];

  for (const field of requiredStringFields) {
    if (typeof payload[field] !== 'string' || !payload[field].trim()) {
      throw new Error(`Missing required field: ${field}`);
    }
  }
}

function toPublicOperatorPayload(payload) {
  const result = {};
  for (const key of PUBLIC_OPERATOR_FIELDS) {
    if (key in payload) {
      result[key] = payload[key];
    }
  }
  return result;
}

async function requireApiToken(request, env) {
  const configuredToken = env.OPERATOR_API_TOKEN;
  if (!configuredToken) {
    throw new Error('Missing OPERATOR_API_TOKEN secret');
  }

  const authHeader = request.headers.get('authorization') || '';
  const token = authHeader.startsWith('Bearer ')
    ? authHeader.slice('Bearer '.length).trim()
    : '';

  if (!token || token !== configuredToken) {
    const error = new Error('Unauthorized');
    error.status = 401;
    throw error;
  }
}

function getActorFromRequest(request) {
  return (
    request.headers.get('x-operator-actor') ||
    request.headers.get('cf-access-authenticated-user-email') ||
    'api-token'
  );
}

async function parseJsonBody(request) {
  try {
    return await request.json();
  } catch {
    const error = new Error('Request body must be valid JSON');
    error.status = 400;
    throw error;
  }
}

function getLatestBlobKey(profileSlug) {
  return `operators/${profileSlug}/latest.json`;
}

function getVersionedBlobKey(profileSlug, isoTimestamp) {
  const safeTimestamp = isoTimestamp.replaceAll(':', '-');
  return `operators/${profileSlug}/versions/${safeTimestamp}.json`;
}

function clampInt(value, min, max, fallback) {
  const parsed = Number.parseInt(String(value || ''), 10);
  if (Number.isNaN(parsed)) return fallback;
  return Math.max(min, Math.min(max, parsed));
}

function jsonResponse(payload, status = 200) {
  return new Response(JSON.stringify(payload, null, 2), {
    status,
    headers: JSON_HEADERS
  });
}

function isPlainObject(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function requireBinding(env, key) {
  if (!env[key]) {
    throw new Error(`Missing required binding: ${key}`);
  }
}