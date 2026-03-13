export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;
    const method = request.method;

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

    if (method === "GET") {
      return handleGet(profileSlug, slice, env);
    }

    if (method === "POST") {
      const authError = await requireAuth(request, env);
      if (authError) return authError;
      return handleWrite(profileSlug, slice, request, env);
    }

    return json({ error: "Method not allowed" }, 405);
  }
};

async function handleGet(profileSlug, slice, env) {
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
  if (slice !== "settings" && slice !== "platform-control") {
    return json({ error: "Write not allowed for this route" }, 405);
  }

  const body = await request.json();
  const existing = (await loadOperator(profileSlug, env)) || {};

  const merged = {
    ...existing,
    ...body,
    profile_slug: profileSlug,
    updated_at: new Date().toISOString()
  };

  await saveOperator(profileSlug, merged, env);
  await upsertOperatorRow(merged, env);
  await insertAuditLog(profileSlug, slice, body, env);

  return json({
    ok: true,
    profile_slug: profileSlug,
    updated_at: merged.updated_at
  });
}

async function loadOperator(profileSlug, env) {
  const key = `operators/${profileSlug}/current.json`;
  const object = await env.OPERATOR_BLOBS.get(key);

  if (!object) return null;

  return await object.json();
}

async function saveOperator(profileSlug, data, env) {
  const timestamp = new Date().toISOString();
  const currentKey = `operators/${profileSlug}/current.json`;
  const versionKey = `operators/${profileSlug}/versions/${timestamp}.json`;
  const payload = JSON.stringify(data, null, 2);

  await env.OPERATOR_BLOBS.put(currentKey, payload, {
    httpMetadata: { contentType: "application/json" }
  });

  await env.OPERATOR_BLOBS.put(versionKey, payload, {
    httpMetadata: { contentType: "application/json" }
  });
}

async function upsertOperatorRow(data, env) {
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
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(profile_slug) DO UPDATE SET
      profile_type = excluded.profile_type,
      operator_name = excluded.operator_name,
      business_name = excluded.business_name,
      business_phone = excluded.business_phone,
      support_email = excluded.support_email,
      business_address = excluded.business_address,
      updated_at = excluded.updated_at
  `).bind(
    data.profile_slug,
    data.profile_type || "operator",
    data.operator_name || "",
    data.business_name || "",
    data.business_phone || "",
    data.support_email || "",
    data.business_address || "",
    data.updated_at || new Date().toISOString()
  ).run();
}

async function insertAuditLog(profileSlug, slice, payload, env) {
  await env.DB.prepare(`
    INSERT INTO audit_log (
      action,
      actor,
      profile_slug,
      payload_json,
      created_at
    )
    VALUES (?, ?, ?, ?, ?)
  `).bind(
    `update:${slice}`,
    "api",
    profileSlug,
    JSON.stringify(payload),
    new Date().toISOString()
  ).run();
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

  if (supplied !== expected) {
    return json({ error: "Unauthorized" }, 401);
  }

  return null;
}

function publicProfile(data) {
  return {
    business_city: data.business_city || "",
    business_email: data.business_email || data.support_email || "",
    business_name: data.business_name || "",
    business_phone: data.business_phone || "",
    business_state: data.business_state || "",
    hero_headline: data.hero_headline || "",
    hero_tagline: data.hero_tagline || "",
    operator_bio: data.operator_bio || "",
    operator_image: data.operator_image || "",
    operator_name: data.operator_name || "",
    profile_slug: data.profile_slug || "",
    projects_featured: data.projects_featured || [],
    projects_gallery: data.projects_gallery || [],
    support_email: data.support_email || "",
    updated_at: data.updated_at || "",
    website: data.website || ""
  };
}

function settingsProfile(data) {
  return {
    analytics_owner: data.analytics_owner || "",
    backup_location: data.backup_location || "",
    billing_instructions: data.billing_instructions || "",
    business_address: data.business_address || "",
    business_hours: data.business_hours || "",
    default_deposit_percentage: data.default_deposit_percentage || "",
    deposit_policy_text: data.deposit_policy_text || "",
    dns_notes: data.dns_notes || "",
    dns_provider: data.dns_provider || "",
    edge_platform: data.edge_platform || "",
    git_provider: data.git_provider || "",
    handoff_brand_assets: Boolean(data.handoff_brand_assets),
    handoff_email_accounts: Boolean(data.handoff_email_accounts),
    handoff_recovery_codes: Boolean(data.handoff_recovery_codes),
    handoff_subscriptions: Boolean(data.handoff_subscriptions),
    handoff_tax_docs: Boolean(data.handoff_tax_docs),
    handoff_vendor_access: Boolean(data.handoff_vendor_access),
    hosting_account: data.hosting_account || "",
    notification_email: data.notification_email || "",
    operator_id: data.operator_id || data.profile_slug || "",
    payment_instructions: data.payment_instructions || "",
    privacy_policy_text: data.privacy_policy_text || "",
    profile_slug: data.profile_slug || "",
    public_contact_name: data.public_contact_name || "",
    registrar: data.registrar || "",
    registrar_login_owner: data.registrar_login_owner || "",
    repo_admin_username: data.repo_admin_username || "",
    repository_url: data.repository_url || "",
    secrets_confirmed: data.secrets_confirmed || "",
    stripe_publishable_key: data.stripe_publishable_key || "",
    stripe_secret_key: data.stripe_secret_key || "",
    stripe_webhook_secret: data.stripe_webhook_secret || "",
    terms_of_service_text: data.terms_of_service_text || "",
    transfer_notes: data.transfer_notes || "",
    updated_at: data.updated_at || ""
  };
}

function platformProfile(data) {
  return {
    analytics_owner: data.analytics_owner || "",
    backup_location: data.backup_location || "",
    dns_notes: data.dns_notes || "",
    dns_provider: data.dns_provider || "",
    edge_platform: data.edge_platform || "",
    git_provider: data.git_provider || "",
    handoff_brand_assets: Boolean(data.handoff_brand_assets),
    handoff_email_accounts: Boolean(data.handoff_email_accounts),
    handoff_recovery_codes: Boolean(data.handoff_recovery_codes),
    handoff_subscriptions: Boolean(data.handoff_subscriptions),
    handoff_tax_docs: Boolean(data.handoff_tax_docs),
    handoff_vendor_access: Boolean(data.handoff_vendor_access),
    hosting_account: data.hosting_account || "",
    primary_domain: data.primary_domain || "",
    profile_slug: data.profile_slug || "",
    registrar: data.registrar || "",
    registrar_login_owner: data.registrar_login_owner || "",
    repo_admin_username: data.repo_admin_username || "",
    repository_url: data.repository_url || "",
    secrets_confirmed: data.secrets_confirmed || "",
    transfer_notes: data.transfer_notes || "",
    updated_at: data.updated_at || ""
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