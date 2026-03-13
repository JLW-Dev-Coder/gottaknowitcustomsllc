export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const path = url.pathname;
      const method = request.method;

      console.log("incoming_request", { method, path });

      if (path === "/api/health") {
        return json({
          hasAssets: !!env.ASSETS,
          hasDb: !!env.DB,
          hasR2: !!env.OPERATOR_BLOBS,
          ok: true,
          service: "gottaknowitcustomsllc"
        });
      }

      const operatorMatch = path.match(/^\/api\/operators\/([^/]+)\/(public-profile|settings|platform-control)$/);

      if (!operatorMatch) {
        return json({ error: "Not found" }, 404);
      }

      const profileSlug = operatorMatch[1];
      const slice = operatorMatch[2];

      console.log("matched_route", { method, path, profileSlug, slice });

      if (method === "GET") {
        return await handleGet(profileSlug, slice, env);
      }

      if (method === "POST") {
        const authError = await requireAuth(request, env);
        if (authError) return authError;
        return await handleWrite(profileSlug, slice, request, env);
      }

      return json({ error: "Method not allowed" }, 405);
    } catch (error) {
      console.error("UNCAUGHT_WORKER_ERROR", serializeError(error));
      return json(
        {
          error: "Worker exception",
          message: error?.message || "Unknown error"
        },
        500
      );
    }
  }
};

async function handleGet(profileSlug, slice, env) {
  console.log("handleGet:start", { profileSlug, slice });

  const data = await loadOperator(profileSlug, env);

  if (!data) {
    return json({ error: "Operator not found" }, 404);
  }

  if (slice === "public-profile") {
    return json(publicProfile(data));
  }

  if (slice === "settings") {
    return json(settingsProfile(data));
  }

  if (slice === "platform-control") {
    return json(platformProfile(data));
  }

  return json({ error: "Invalid slice" }, 400);
}

async function handleWrite(profileSlug, slice, request, env) {
  console.log("handleWrite:start", { profileSlug, slice });

  if (slice !== "settings" && slice !== "platform-control") {
    return json({ error: "Write not allowed for this route" }, 405);
  }

  let body;
  try {
    body = await request.json();
  } catch (error) {
    console.error("handleWrite:invalid_json", serializeError(error));
    return json({ error: "Invalid JSON body" }, 400);
  }

  if (!isPlainObject(body)) {
    return json({ error: "JSON body must be an object" }, 400);
  }

  console.log("handleWrite:body_keys", Object.keys(body));

  const existing = (await loadOperator(profileSlug, env)) || {};
  console.log("handleWrite:existing_loaded", {
    hasExisting: Object.keys(existing).length > 0
  });

  const merged = {
    ...existing,
    ...body,
    profile_slug: profileSlug,
    profile_type: normalizeString(body.profile_type || existing.profile_type || "operator"),
    updated_at: new Date().toISOString()
  };

  console.log("handleWrite:before_saveOperator");
  await saveOperator(profileSlug, merged, env);

  console.log("handleWrite:before_upsertOperatorRow");
  await upsertOperatorRow(merged, env);

  console.log("handleWrite:before_insertAuditLog");
  await insertAuditLog(profileSlug, slice, body, env);

  console.log("handleWrite:success", {
    profileSlug,
    slice,
    updated_at: merged.updated_at
  });

  return json({
    ok: true,
    profile_slug: profileSlug,
    updated_at: merged.updated_at
  });
}

async function loadOperator(profileSlug, env) {
  ensureR2Binding(env);

  const key = `operators/${profileSlug}/current.json`;
  console.log("loadOperator:key", { key });

  const object = await env.OPERATOR_BLOBS.get(key);

  if (!object) {
    console.log("loadOperator:not_found", { key });
    return null;
  }

  try {
    const data = await object.json();
    if (!isPlainObject(data)) {
      throw new Error("Stored operator JSON is not an object");
    }
    return data;
  } catch (error) {
    console.error("loadOperator:json_parse_failed", {
      key,
      ...serializeError(error)
    });
    throw new Error(`Failed to parse stored operator JSON for ${profileSlug}`);
  }
}

async function saveOperator(profileSlug, data, env) {
  ensureR2Binding(env);

  const timestamp = new Date().toISOString();
  const currentKey = `operators/${profileSlug}/current.json`;
  const versionKey = `operators/${profileSlug}/versions/${timestamp}.json`;
  const payload = JSON.stringify(data, null, 2);

  console.log("saveOperator:put_current", { currentKey });
  await env.OPERATOR_BLOBS.put(currentKey, payload, {
    httpMetadata: { contentType: "application/json" }
  });

  console.log("saveOperator:put_version", { versionKey });
  await env.OPERATOR_BLOBS.put(versionKey, payload, {
    httpMetadata: { contentType: "application/json" }
  });
}

async function upsertOperatorRow(data, env) {
  ensureDbBinding(env);

  const updatedAt = normalizeString(data.updated_at || new Date().toISOString());
  const profileSlug = normalizeString(data.profile_slug);
  const profileType = normalizeString(data.profile_type || "operator");
  const operatorName = normalizeString(data.operator_name || data.public_contact_name || "");
  const businessName = normalizeString(data.business_name || "");
  const businessPhone = normalizeString(data.business_phone || "");
  const supportEmail = normalizeString(data.support_email || data.notification_email || "");
  const businessAddress = normalizeString(data.business_address || "");

  if (!profileSlug) {
    throw new Error("profile_slug is required for operator upsert");
  }

  console.log("upsertOperatorRow:bind_values", {
    businessName,
    operatorName,
    profileSlug,
    profileType,
    updatedAt
  });

  const stmt = env.DB.prepare(`
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
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(profile_slug) DO UPDATE SET
      profile_type = excluded.profile_type,
      operator_name = excluded.operator_name,
      business_name = excluded.business_name,
      business_phone = excluded.business_phone,
      support_email = excluded.support_email,
      business_address = excluded.business_address,
      updated_at = excluded.updated_at
  `);

  const result = await stmt
    .bind(
      profileSlug,
      profileType,
      operatorName,
      businessName,
      businessPhone,
      supportEmail,
      businessAddress,
      updatedAt
    )
    .run();

  console.log("upsertOperatorRow:result", {
    success: result?.success ?? true,
    meta: result?.meta || null
  });
}

async function insertAuditLog(profileSlug, slice, payload, env) {
  ensureDbBinding(env);

  const createdAt = new Date().toISOString();
  const action = `update:${slice}`;
  const actor = "api";

  console.log("insertAuditLog:start", { action, actor, profileSlug });

  const stmt = env.DB.prepare(`
    INSERT INTO audit_log (
      action,
      actor,
      profile_slug,
      payload_json,
      created_at
    )
    VALUES (?, ?, ?, ?, ?)
  `);

  const result = await stmt
    .bind(
      action,
      actor,
      normalizeString(profileSlug),
      JSON.stringify(payload ?? {}),
      createdAt
    )
    .run();

  console.log("insertAuditLog:result", {
    success: result?.success ?? true,
    meta: result?.meta || null
  });
}

async function requireAuth(request, env) {
  const authHeader = request.headers.get("Authorization") || "";
  const expected = env.OPERATOR_API_TOKEN || "";

  if (!expected) {
    return json({ error: "Server auth not configured" }, 500);
  }

  if (!authHeader.startsWith("Bearer ")) {
    return json({ error: "Unauthorized" }, 401);
  }

  const supplied = authHeader.slice("Bearer ".length).trim();

  if (!supplied || supplied !== expected) {
    return json({ error: "Unauthorized" }, 401);
  }

  return null;
}

function publicProfile(data) {
  return {
    business_city: normalizeString(data.business_city || ""),
    business_email: normalizeString(data.business_email || data.support_email || ""),
    business_name: normalizeString(data.business_name || ""),
    business_phone: normalizeString(data.business_phone || ""),
    business_state: normalizeString(data.business_state || ""),
    hero_headline: normalizeString(data.hero_headline || ""),
    hero_tagline: normalizeString(data.hero_tagline || ""),
    operator_bio: normalizeString(data.operator_bio || ""),
    operator_image: normalizeString(data.operator_image || ""),
    operator_name: normalizeString(data.operator_name || data.public_contact_name || ""),
    profile_slug: normalizeString(data.profile_slug || ""),
    projects_featured: Array.isArray(data.projects_featured) ? data.projects_featured : [],
    projects_gallery: Array.isArray(data.projects_gallery) ? data.projects_gallery : [],
    support_email: normalizeString(data.support_email || ""),
    updated_at: normalizeString(data.updated_at || ""),
    website: normalizeString(data.website || "")
  };
}

function settingsProfile(data) {
  return {
    analytics_owner: normalizeString(data.analytics_owner || ""),
    backup_location: normalizeString(data.backup_location || ""),
    billing_instructions: normalizeString(data.billing_instructions || ""),
    business_address: normalizeString(data.business_address || ""),
    business_hours: normalizeString(data.business_hours || ""),
    default_deposit_percentage: normalizeNumberOrEmpty(data.default_deposit_percentage),
    deposit_policy_text: normalizeString(data.deposit_policy_text || ""),
    dns_notes: normalizeString(data.dns_notes || ""),
    dns_provider: normalizeString(data.dns_provider || ""),
    edge_platform: normalizeString(data.edge_platform || ""),
    git_provider: normalizeString(data.git_provider || ""),
    handoff_brand_assets: normalizeBoolean(data.handoff_brand_assets),
    handoff_email_accounts: normalizeBoolean(data.handoff_email_accounts),
    handoff_recovery_codes: normalizeBoolean(data.handoff_recovery_codes),
    handoff_subscriptions: normalizeBoolean(data.handoff_subscriptions),
    handoff_tax_docs: normalizeBoolean(data.handoff_tax_docs),
    handoff_vendor_access: normalizeBoolean(data.handoff_vendor_access),
    hosting_account: normalizeString(data.hosting_account || ""),
    notification_email: normalizeString(data.notification_email || ""),
    operator_id: normalizeString(data.operator_id || data.profile_slug || ""),
    payment_instructions: normalizeString(data.payment_instructions || ""),
    privacy_policy_text: normalizeString(data.privacy_policy_text || ""),
    profile_slug: normalizeString(data.profile_slug || ""),
    public_contact_name: normalizeString(data.public_contact_name || ""),
    registrar: normalizeString(data.registrar || ""),
    registrar_login_owner: normalizeString(data.registrar_login_owner || ""),
    repo_admin_username: normalizeString(data.repo_admin_username || ""),
    repository_url: normalizeString(data.repository_url || ""),
    secrets_confirmed: typeof data.secrets_confirmed === "boolean"
      ? data.secrets_confirmed
      : normalizeString(data.secrets_confirmed || ""),
    stripe_publishable_key: normalizeString(data.stripe_publishable_key || ""),
    stripe_secret_key: normalizeString(data.stripe_secret_key || ""),
    stripe_webhook_secret: normalizeString(data.stripe_webhook_secret || ""),
    terms_of_service_text: normalizeString(data.terms_of_service_text || ""),
    transfer_notes: normalizeString(data.transfer_notes || ""),
    updated_at: normalizeString(data.updated_at || "")
  };
}

function platformProfile(data) {
  return {
    analytics_owner: normalizeString(data.analytics_owner || ""),
    backup_location: normalizeString(data.backup_location || ""),
    dns_notes: normalizeString(data.dns_notes || ""),
    dns_provider: normalizeString(data.dns_provider || ""),
    edge_platform: normalizeString(data.edge_platform || ""),
    git_provider: normalizeString(data.git_provider || ""),
    handoff_brand_assets: normalizeBoolean(data.handoff_brand_assets),
    handoff_email_accounts: normalizeBoolean(data.handoff_email_accounts),
    handoff_recovery_codes: normalizeBoolean(data.handoff_recovery_codes),
    handoff_subscriptions: normalizeBoolean(data.handoff_subscriptions),
    handoff_tax_docs: normalizeBoolean(data.handoff_tax_docs),
    handoff_vendor_access: normalizeBoolean(data.handoff_vendor_access),
    hosting_account: normalizeString(data.hosting_account || ""),
    primary_domain: normalizeString(data.primary_domain || ""),
    profile_slug: normalizeString(data.profile_slug || ""),
    registrar: normalizeString(data.registrar || ""),
    registrar_login_owner: normalizeString(data.registrar_login_owner || ""),
    repo_admin_username: normalizeString(data.repo_admin_username || ""),
    repository_url: normalizeString(data.repository_url || ""),
    secrets_confirmed: typeof data.secrets_confirmed === "boolean"
      ? data.secrets_confirmed
      : normalizeString(data.secrets_confirmed || ""),
    transfer_notes: normalizeString(data.transfer_notes || ""),
    updated_at: normalizeString(data.updated_at || "")
  };
}

function ensureDbBinding(env) {
  if (!env.DB) {
    throw new Error("D1 binding DB is not configured");
  }
}

function ensureR2Binding(env) {
  if (!env.OPERATOR_BLOBS) {
    throw new Error("R2 binding OPERATOR_BLOBS is not configured");
  }
}

function isPlainObject(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function normalizeBoolean(value) {
  if (typeof value === "boolean") return value;
  if (typeof value === "string") {
    const lowered = value.trim().toLowerCase();
    if (lowered === "true") return true;
    if (lowered === "false") return false;
  }
  return Boolean(value);
}

function normalizeNumberOrEmpty(value) {
  if (value === null || value === undefined || value === "") return "";
  const num = Number(value);
  return Number.isFinite(num) ? num : "";
}

function normalizeString(value) {
  if (value === null || value === undefined) return "";
  return String(value).trim();
}

function serializeError(error) {
  return {
    message: error?.message || String(error),
    name: error?.name || "Error",
    stack: error?.stack || null
  };
}

function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=UTF-8"
    }
  });
}