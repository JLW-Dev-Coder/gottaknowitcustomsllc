export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const path = url.pathname;
      const method = request.method.toUpperCase();

      if (method === "OPTIONS") {
        return withCors(new Response(null, { status: 204 }));
      }

      if (path === "/api/auth/login") {
        if (method === "POST") {
          return handleAuthLogin(request, env);
        }

        return methodNotAllowed(["POST", "OPTIONS"]);
      }

      if (path === "/api/auth/logout") {
        if (method === "POST") {
          return handleAuthLogout();
        }

        return methodNotAllowed(["POST", "OPTIONS"]);
      }

      if (path === "/api/auth/session") {
        if (method === "GET") {
          return handleAuthSession(request, env);
        }

        return methodNotAllowed(["GET", "OPTIONS"]);
      }

      if (path === "/api/build-requests" && method === "POST") {
        return handleBuildRequestCreate(request, env);
      }

      const buildRequestMatch = path.match(/^\/api\/build-requests\/([^/]+)$/);
      if (buildRequestMatch) {
        const orderNumber = safeOrderNumber(buildRequestMatch[1]);

        if (method === "GET") {
          const authError = await requireOperatorAuth(request, env);
          if (authError) return authError;
          return handleBuildRequestGet(orderNumber, env);
        }

        return methodNotAllowed(["GET", "OPTIONS"]);
      }

      if (path === "/api/dashboard-summary") {
        if (method === "GET") {
          const authError = await requireOperatorAuth(request, env);
          if (authError) return authError;
          return handleDashboardSummary(env);
        }

        return methodNotAllowed(["GET", "OPTIONS"]);
      }

      const estimateMatch = path.match(/^\/api\/estimates\/([^/]+)$/);
      if (estimateMatch) {
        const orderNumber = safeOrderNumber(estimateMatch[1]);

        if (method === "GET") {
          const authError = await requireOperatorAuth(request, env);
          if (authError) return authError;
          return handleEstimateGet(orderNumber, env);
        }

        if (method === "POST") {
          const authError = await requireOperatorAuth(request, env);
          if (authError) return authError;
          return handleEstimateWrite(orderNumber, request, env);
        }

        return methodNotAllowed(["GET", "POST", "OPTIONS"]);
      }

      if (path === "/api/health" && method === "GET") {
        return json({
          hasAssets: !!env.ASSETS,
          hasDb: !!env.DB,
          hasR2: !!env.OPERATOR_BLOBS,
          ok: true,
          service: "gottaknowitcustomsllc"
        });
      }

      const operatorMatch = path.match(/^\/api\/operators\/([^/]+)\/(platform-control|public-profile|settings)$/);
      if (operatorMatch) {
        const profileSlug = safeSlug(operatorMatch[1]);
        const slice = operatorMatch[2];

        if (method === "GET") {
          return handleOperatorGet(profileSlug, slice, env);
        }

        if (method === "POST") {
          const authError = await requireOperatorAuth(request, env);
          if (authError) return authError;
          return handleOperatorWrite(profileSlug, slice, request, env);
        }

        return methodNotAllowed(["GET", "POST", "OPTIONS"]);
      }

      if (path === "/api/orders") {
        if (method === "GET") {
          const authError = await requireOperatorAuth(request, env);
          if (authError) return authError;
          return handleOrdersList(request, env);
        }

        return methodNotAllowed(["GET", "OPTIONS"]);
      }

      const orderNotesMatch = path.match(/^\/api\/orders\/([^/]+)\/notes$/);
      if (orderNotesMatch) {
        const orderNumber = safeOrderNumber(orderNotesMatch[1]);

        if (method === "POST") {
          const authError = await requireOperatorAuth(request, env);
          if (authError) return authError;
          return handleOrderNoteCreate(orderNumber, request, env);
        }

        return methodNotAllowed(["POST", "OPTIONS"]);
      }

      const orderStatusPatchMatch = path.match(/^\/api\/orders\/([^/]+)\/status$/);
      if (orderStatusPatchMatch) {
        const orderNumber = safeOrderNumber(orderStatusPatchMatch[1]);

        if (method === "PATCH") {
          const authError = await requireOperatorAuth(request, env);
          if (authError) return authError;
          return handleOrderStatusPatch(orderNumber, request, env);
        }

        return methodNotAllowed(["PATCH", "OPTIONS"]);
      }

      const orderMatch = path.match(/^\/api\/orders\/([^/]+)$/);
      if (orderMatch) {
        const orderNumber = safeOrderNumber(orderMatch[1]);

        if (method === "GET") {
          const authError = await requireOperatorAuth(request, env);
          if (authError) return authError;
          return handleOrderDetailGet(orderNumber, env);
        }

        return methodNotAllowed(["GET", "OPTIONS"]);
      }

      const paymentRequestMatch = path.match(/^\/api\/payment-requests\/([^/]+)$/);
      if (paymentRequestMatch) {
        const orderNumber = safeOrderNumber(paymentRequestMatch[1]);

        if (method === "GET") {
          const authError = await requireOperatorAuth(request, env);
          if (authError) return authError;
          return handlePaymentRequestGet(orderNumber, env);
        }

        if (method === "POST") {
          const authError = await requireOperatorAuth(request, env);
          if (authError) return authError;
          return handlePaymentRequestWrite(orderNumber, request, env);
        }

        return methodNotAllowed(["GET", "POST", "OPTIONS"]);
      }

      const statusLookupMatch = path.match(/^\/api\/status-lookup\/([^/]+)$/);
      if (statusLookupMatch) {
        const orderNumber = safeOrderNumber(statusLookupMatch[1]);

        if (method === "GET") {
          return handleStatusLookupGet(orderNumber, env);
        }

        return methodNotAllowed(["GET", "OPTIONS"]);
      }

      return json({ error: "Not found" }, 404);
    } catch (error) {
      console.error("Unhandled worker error:", error);
      return json({
        error: "Internal server error",
        message: error instanceof Error ? error.message : String(error)
      }, 500);
    }
  }
};

/* =========================
   Auth routes
   ========================= */

async function handleAuthLogin(request, env) {
  const body = await parseJsonBody(request);
  if (body instanceof Response) return body;

  const email = safeString(body.email).toLowerCase();
  const code = safeString(body.code);

  const expectedEmail = safeString(env.OPERATOR_LOGIN_EMAIL).toLowerCase();
  const expectedCode = safeString(env.OPERATOR_LOGIN_CODE);

  if (!expectedEmail || !expectedCode || !safeString(env.OPERATOR_SESSION_SECRET)) {
    return json({ error: "Auth not configured" }, 500);
  }

  if (!email || !code) {
    return json({ error: "email and code are required" }, 400);
  }

  if (email !== expectedEmail || code !== expectedCode) {
    return json({ error: "Unauthorized" }, 401);
  }

  const token = await createSessionToken({
    email,
    exp: Math.floor(Date.now() / 1000) + (60 * 60 * 8)
  }, env.OPERATOR_SESSION_SECRET);

  return withCorsAndCookie(
    json({
      email,
      ok: true
    }),
    buildSessionCookie(token)
  );
}

async function handleAuthLogout() {
  return withCorsAndCookie(
    json({ ok: true }),
    clearSessionCookie()
  );
}

async function handleAuthSession(request, env) {
  const session = await getSessionFromCookie(request, env);

  if (!session) {
    return json({
      authenticated: false,
      ok: true
    });
  }

  return json({
    authenticated: true,
    email: session.email,
    ok: true
  });
}

/* =========================
   Operator routes
   ========================= */

async function handleOperatorGet(profileSlug, slice, env) {
  const data = await loadOperator(profileSlug, env);

  if (!data) {
    return json({ error: "Operator not found" }, 404);
  }

  if (slice === "platform-control") {
    return json(platformProfile(data));
  }

  if (slice === "public-profile") {
    return json(publicProfile(data));
  }

  if (slice === "settings") {
    return json(settingsProfile(data));
  }

  return json({ error: "Invalid slice" }, 400);
}

async function handleOperatorWrite(profileSlug, slice, request, env) {
  if (slice !== "platform-control" && slice !== "settings") {
    return json({ error: "Write not allowed for this route" }, 405);
  }

  const body = await parseJsonBody(request);
  if (body instanceof Response) return body;

  const existing = (await loadOperator(profileSlug, env)) || {};
  const merged = {
    ...existing,
    ...body,
    profile_slug: profileSlug,
    profile_type: body.profile_type || existing.profile_type || "operator",
    updated_at: new Date().toISOString()
  };

  await saveOperator(profileSlug, merged, env);
  await upsertOperatorRow(merged, env);
  await insertAuditLog(profileSlug, `update:${slice}`, body, env);

  return json({
    ok: true,
    profile_slug: profileSlug,
    updated_at: merged.updated_at
  });
}

async function loadOperator(profileSlug, env) {
  const currentKey = `operators/${profileSlug}/current.json`;
  const object = await env.OPERATOR_BLOBS.get(currentKey);

  if (object) {
    return await object.json();
  }

  const staticCandidates = [
    `/assets/operator/home/${profileSlug}/data.json`,
    `/assets/operator/home/sample/data.json`
  ];

  for (const assetPath of staticCandidates) {
    try {
      if (!env.ASSETS || typeof env.ASSETS.fetch !== "function") break;

      const response = await env.ASSETS.fetch(new Request(`https://internal${assetPath}`));
      if (!response.ok) continue;

      const data = await response.json();
      return {
        ...data,
        profile_slug: data.profile_slug || profileSlug
      };
    } catch (error) {
      console.warn(`Failed to load static operator asset ${assetPath}`, error);
    }
  }

  return null;
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

/* =========================
   Build request routes
   ========================= */

async function handleBuildRequestCreate(request, env) {
  const body = await parseJsonBody(request);
  if (body instanceof Response) return body;

  const validationError = validateBuildRequest(body);
  if (validationError) return json({ error: validationError }, 400);

  const now = new Date().toISOString();
  const orderNumber = body.order_number || generateOrderNumber();
  const normalizedServices = normalizeServiceArray(body.services || []);

  const buildRequest = {
    contract: "build-request.v1",
    created_at: now,
    customer: {
      email: safeString(body.customer?.email),
      name: safeString(body.customer?.name),
      phone: safeString(body.customer?.phone)
    },
    customer_visible_notes: safeString(body.customer_visible_notes),
    operator_notes: safeString(body.operator_notes),
    order_number: orderNumber,
    project_notes: safeString(body.project_notes),
    services: normalizedServices,
    status: "Received",
    updated_at: now,
    uploads: normalizeUploads(body.uploads || []),
    vehicle: {
      make: safeString(body.vehicle?.make),
      model: safeString(body.vehicle?.model),
      year: safeNumber(body.vehicle?.year)
    }
  };

  const statusLookup = buildStatusLookupFromBuildRequest(buildRequest);
  const notesDoc = buildEmptyNotesDocument(orderNumber);

  await saveVersionedBlob(`order-notes/${orderNumber}`, notesDoc, env);
  await saveVersionedBlob(`orders/${orderNumber}`, buildRequest, env);
  await saveVersionedBlob(`status-lookup/${orderNumber}`, statusLookup, env);
  await insertAuditLog(orderNumber, "create:build-request", { order_number: orderNumber }, env);

  return json({
    created_at: now,
    ok: true,
    order_number: orderNumber,
    status: buildRequest.status
  }, 201);
}

async function handleBuildRequestGet(orderNumber, env) {
  const data = await loadCurrentBlob(`orders/${orderNumber}`, env);

  if (!data) {
    return json({ error: "Build request not found" }, 404);
  }

  return json(data);
}

/* =========================
   Dashboard routes
   ========================= */

async function handleDashboardSummary(env) {
  const orders = await loadAllCurrentOrders(env);
  const paymentRequests = await loadAllCurrentPaymentRequests(env);

  let completed = 0;
  let inProgress = 0;
  let pending = 0;
  let revenueCollected = 0;

  for (const order of orders) {
    const normalized = normalizeStatusForFilter(order.status);

    if (normalized === "completed") {
      completed += 1;
      continue;
    }

    if (normalized === "in_progress") {
      inProgress += 1;
      continue;
    }

    pending += 1;
  }

  for (const payment of paymentRequests) {
    if (safeString(payment.status) === "Paid") {
      revenueCollected += safeNumber(payment.amount);
    }
  }

  return json({
    completed,
    in_progress: inProgress,
    ok: true,
    pending,
    revenue_collected: roundCurrency(revenueCollected),
    total_orders: orders.length
  });
}

/* =========================
   Orders routes
   ========================= */

async function handleOrdersList(request, env) {
  const url = new URL(request.url);
  const limit = clampInt(url.searchParams.get("limit"), 25, 1, 100);
  const statusFilter = normalizeStatusForFilter(url.searchParams.get("status"));

  const orders = await loadAllCurrentOrders(env);
  const filtered = orders
    .filter((order) => !statusFilter || normalizeStatusForFilter(order.status) === statusFilter)
    .sort((a, b) => {
      const aDate = Date.parse(a.updated_at || a.created_at || 0);
      const bDate = Date.parse(b.updated_at || b.created_at || 0);
      return bDate - aDate;
    })
    .slice(0, limit)
    .map((order) => orderListProjection(order));

  return json({
    count: filtered.length,
    items: filtered,
    ok: true
  });
}

async function handleOrderDetailGet(orderNumber, env) {
  const buildRequest = await loadCurrentBlob(`orders/${orderNumber}`, env);

  if (!buildRequest) {
    return json({ error: "Order not found" }, 404);
  }

  const activityLog = await loadAuditLog(orderNumber, env);
  const estimate = await loadCurrentBlob(`estimates/${orderNumber}`, env);
  const notesDoc = (await loadCurrentBlob(`order-notes/${orderNumber}`, env)) || buildEmptyNotesDocument(orderNumber);
  const paymentRequest = await loadCurrentBlob(`payment-requests/${orderNumber}`, env);
  const statusLookup = await loadCurrentBlob(`status-lookup/${orderNumber}`, env);

  return json(orderDetailProjection(
    buildRequest,
    statusLookup,
    estimate,
    paymentRequest,
    notesDoc.items || [],
    activityLog
  ));
}

async function handleOrderNoteCreate(orderNumber, request, env) {
  const body = await parseJsonBody(request);
  if (body instanceof Response) return body;

  const buildRequest = await loadCurrentBlob(`orders/${orderNumber}`, env);
  if (!buildRequest) {
    return json({ error: "Order not found" }, 404);
  }

  const noteText = safeString(body.note);
  if (!noteText) {
    return json({ error: "note is required" }, 400);
  }

  const author = safeString(body.author || "Admin");
  const createdAt = new Date().toISOString();
  const visibility = normalizeNoteVisibility(body.visibility);

  const existing = (await loadCurrentBlob(`order-notes/${orderNumber}`, env)) || buildEmptyNotesDocument(orderNumber);
  const entry = {
    author,
    created_at: createdAt,
    id: `note_${Date.now()}_${Math.floor(Math.random() * 10000)}`,
    note: noteText,
    visibility
  };

  const nextDoc = {
    ...existing,
    items: [...(Array.isArray(existing.items) ? existing.items : []), entry],
    order_number: orderNumber,
    updated_at: createdAt
  };

  await saveVersionedBlob(`order-notes/${orderNumber}`, nextDoc, env);
  await insertAuditLog(orderNumber, "add:order-note", entry, env);

  return json({
    note: entry,
    ok: true,
    order_number: orderNumber
  }, 201);
}

async function handleOrderStatusPatch(orderNumber, request, env) {
  const body = await parseJsonBody(request);
  if (body instanceof Response) return body;

  const buildRequest = await loadCurrentBlob(`orders/${orderNumber}`, env);
  if (!buildRequest) {
    return json({ error: "Order not found" }, 404);
  }

  const nextStatus = normalizeOrderStatus(body.status);
  if (!nextStatus) {
    return json({ error: "status is required" }, 400);
  }

  const now = new Date().toISOString();
  const updatedBuildRequest = {
    ...buildRequest,
    customer_visible_notes: body.customer_visible_notes !== undefined
      ? safeString(body.customer_visible_notes)
      : safeString(buildRequest.customer_visible_notes),
    operator_notes: body.operator_notes !== undefined
      ? safeString(body.operator_notes)
      : safeString(buildRequest.operator_notes),
    status: nextStatus,
    updated_at: now
  };

  await saveVersionedBlob(`orders/${orderNumber}`, updatedBuildRequest, env);

  const existingStatusLookup = await loadCurrentBlob(`status-lookup/${orderNumber}`, env);
  const updatedStatusLookup = {
    ...(existingStatusLookup || buildStatusLookupFromBuildRequest(updatedBuildRequest)),
    contract: "status-lookup.v1",
    customer_name: safeString(updatedBuildRequest.customer?.name),
    customer_visible_notes: safeString(updatedBuildRequest.customer_visible_notes),
    last_updated: now,
    order_number: safeString(updatedBuildRequest.order_number),
    payment_status: existingStatusLookup?.payment_status || {
      deposit: "Not Requested",
      final: "Not Requested"
    },
    project_notes: safeString(updatedBuildRequest.project_notes),
    services: Array.isArray(updatedBuildRequest.services) ? updatedBuildRequest.services : [],
    status: nextStatus,
    status_label: nextStatus,
    updated_at: now,
    vehicle: {
      make: safeString(updatedBuildRequest.vehicle?.make),
      model: safeString(updatedBuildRequest.vehicle?.model),
      year: safeNumber(updatedBuildRequest.vehicle?.year)
    }
  };

  await saveVersionedBlob(`status-lookup/${orderNumber}`, updatedStatusLookup, env);
  await insertAuditLog(orderNumber, "update:order-status", {
    customer_visible_notes: updatedBuildRequest.customer_visible_notes,
    operator_notes: updatedBuildRequest.operator_notes,
    status: nextStatus
  }, env);

  return json({
    ok: true,
    order_number: orderNumber,
    status: nextStatus,
    updated_at: now
  });
}

/* =========================
   Status lookup routes
   ========================= */

async function handleStatusLookupGet(orderNumber, env) {
  const data = await loadCurrentBlob(`status-lookup/${orderNumber}`, env);

  if (!data) {
    return json({ error: "Status lookup not found" }, 404);
  }

  return json(data);
}

/* =========================
   Estimate routes
   ========================= */

async function handleEstimateGet(orderNumber, env) {
  const data = await loadCurrentBlob(`estimates/${orderNumber}`, env);

  if (!data) {
    return json({ error: "Estimate not found" }, 404);
  }

  return json(data);
}

async function handleEstimateWrite(orderNumber, request, env) {
  const body = await parseJsonBody(request);
  if (body instanceof Response) return body;

  const buildRequest = await loadCurrentBlob(`orders/${orderNumber}`, env);
  if (!buildRequest) {
    return json({ error: "Cannot create estimate for unknown order" }, 404);
  }

  const now = new Date().toISOString();
  const existing = (await loadCurrentBlob(`estimates/${orderNumber}`, env)) || {};

  const laborHours = safeNumber(body.labor_hours, existing.labor_hours);
  const laborRate = safeNumber(body.labor_rate, existing.labor_rate);
  const laborTotal =
    body.labor_total !== undefined
      ? safeNumber(body.labor_total)
      : roundCurrency(laborHours * laborRate);

  const estimate = {
    adjustment_amount: safeNumber(body.adjustment_amount, existing.adjustment_amount),
    contract: "estimate.v1",
    customer_visible_notes: safeString(body.customer_visible_notes, existing.customer_visible_notes),
    deposit_required: safeNumber(body.deposit_required, existing.deposit_required),
    estimate_total: safeNumber(body.estimate_total, existing.estimate_total),
    labor_hours: laborHours,
    labor_rate: laborRate,
    labor_total: laborTotal,
    materials_cost: safeNumber(body.materials_cost, existing.materials_cost),
    operator_notes: safeString(body.operator_notes, existing.operator_notes),
    order_number: orderNumber,
    parts_cost: safeNumber(body.parts_cost, existing.parts_cost),
    sent_at: body.sent_at ? safeString(body.sent_at) : (existing.sent_at || now)
  };

  await saveVersionedBlob(`estimates/${orderNumber}`, estimate, env);
  await insertAuditLog(orderNumber, "update:estimate", { order_number: orderNumber }, env);

  return json({
    ok: true,
    order_number: orderNumber,
    sent_at: estimate.sent_at
  });
}

/* =========================
   Payment request routes
   ========================= */

async function handlePaymentRequestGet(orderNumber, env) {
  const data = await loadCurrentBlob(`payment-requests/${orderNumber}`, env);

  if (!data) {
    return json({ error: "Payment request not found" }, 404);
  }

  return json(data);
}

async function handlePaymentRequestWrite(orderNumber, request, env) {
  const body = await parseJsonBody(request);
  if (body instanceof Response) return body;

  const buildRequest = await loadCurrentBlob(`orders/${orderNumber}`, env);
  if (!buildRequest) {
    return json({ error: "Cannot create payment request for unknown order" }, 404);
  }

  const existing = (await loadCurrentBlob(`payment-requests/${orderNumber}`, env)) || {};
  const now = new Date().toISOString();
  const paymentType = normalizePaymentType(body.payment_type || existing.payment_type || "deposit");
  const status = normalizePaymentStatus(body.status || existing.status || "Requested");
  const amount = safeNumber(body.amount, existing.amount);
  const currency = safeString(body.currency, existing.currency || "USD");
  const estimateReference =
    safeString(body.estimate_reference, existing.estimate_reference) ||
    `EST-${orderNumber}-V1`;

  const paymentRequest = {
    amount,
    contract: "payment-request.v1",
    currency,
    customer_visible_notes: safeString(body.customer_visible_notes, existing.customer_visible_notes),
    estimate_reference: estimateReference,
    operator_notes: safeString(body.operator_notes, existing.operator_notes),
    order_number: orderNumber,
    paid_at: status === "Paid"
      ? safeString(body.paid_at, existing.paid_at || now)
      : body.paid_at === null
        ? null
        : safeNullableString(body.paid_at, existing.paid_at),
    payment_link: safeString(body.payment_link, existing.payment_link),
    payment_type: paymentType,
    processor_reference:
      safeString(body.processor_reference, existing.processor_reference) ||
      `PAY-${orderNumber}-${paymentType.toUpperCase()}-1`,
    requested_at: safeString(body.requested_at, existing.requested_at || now),
    status
  };

  await saveVersionedBlob(`payment-requests/${orderNumber}`, paymentRequest, env);
  await updateStatusLookupPayment(orderNumber, paymentRequest, env);
  await insertAuditLog(orderNumber, "update:payment-request", { order_number: orderNumber }, env);

  return json({
    ok: true,
    order_number: orderNumber,
    payment_type: paymentRequest.payment_type,
    status: paymentRequest.status
  });
}

/* =========================
   Blob storage helpers
   ========================= */

async function loadAllCurrentOrders(env) {
  const listed = await env.OPERATOR_BLOBS.list({ prefix: "orders/" });
  const keys = (listed.objects || [])
    .map((object) => object.key)
    .filter((key) => key.endsWith("/current.json"));

  const orders = [];
  for (const key of keys) {
    const object = await env.OPERATOR_BLOBS.get(key);
    if (!object) continue;
    orders.push(await object.json());
  }

  return orders;
}

async function loadAllCurrentPaymentRequests(env) {
  const listed = await env.OPERATOR_BLOBS.list({ prefix: "payment-requests/" });
  const keys = (listed.objects || [])
    .map((object) => object.key)
    .filter((key) => key.endsWith("/current.json"));

  const paymentRequests = [];
  for (const key of keys) {
    const object = await env.OPERATOR_BLOBS.get(key);
    if (!object) continue;
    paymentRequests.push(await object.json());
  }

  return paymentRequests;
}

async function loadCurrentBlob(prefix, env) {
  const key = `${prefix}/current.json`;
  const object = await env.OPERATOR_BLOBS.get(key);
  if (!object) return null;
  return await object.json();
}

async function saveVersionedBlob(prefix, data, env) {
  const timestamp = new Date().toISOString();
  const currentKey = `${prefix}/current.json`;
  const versionKey = `${prefix}/versions/${timestamp}.json`;
  const payload = JSON.stringify(data, null, 2);

  await env.OPERATOR_BLOBS.put(currentKey, payload, {
    httpMetadata: { contentType: "application/json" }
  });

  await env.OPERATOR_BLOBS.put(versionKey, payload, {
    httpMetadata: { contentType: "application/json" }
  });
}

/* =========================
   D1 helpers
   ========================= */

async function insertAuditLog(profileSlug, action, payload, env) {
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
    safeString(action),
    "api",
    safeString(profileSlug),
    JSON.stringify(payload ?? {}),
    new Date().toISOString()
  ).run();
}

async function loadAuditLog(profileSlug, env, limit = 100) {
  const result = await env.DB.prepare(`
    SELECT
      action,
      actor,
      created_at,
      payload_json,
      profile_slug
    FROM audit_log
    WHERE profile_slug = ?
    ORDER BY created_at DESC
    LIMIT ?
  `).bind(
    safeString(profileSlug),
    clampInt(limit, 100, 1, 500)
  ).all();

  return (result.results || []).map((row, index) => {
    let payload = {};
    try {
      payload = row.payload_json ? JSON.parse(row.payload_json) : {};
    } catch {
      payload = {};
    }

    return {
      action: safeString(row.action),
      actor: safeString(row.actor),
      created_at: safeString(row.created_at),
      id: `log_${index}_${safeString(row.created_at)}`,
      payload
    };
  });
}

async function upsertOperatorRow(data, env) {
  await env.DB.prepare(`
    INSERT INTO operators (
      profile_slug,
      profile_type,
      operator_name,
      business_address,
      business_name,
      business_phone,
      support_email,
      updated_at
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(profile_slug) DO UPDATE SET
      profile_type = excluded.profile_type,
      operator_name = excluded.operator_name,
      business_address = excluded.business_address,
      business_name = excluded.business_name,
      business_phone = excluded.business_phone,
      support_email = excluded.support_email,
      updated_at = excluded.updated_at
  `).bind(
    safeString(data.profile_slug),
    safeString(data.profile_type || "operator"),
    safeString(data.operator_name),
    safeString(data.business_address),
    safeString(data.business_name),
    safeString(data.business_phone),
    safeString(data.support_email),
    safeString(data.updated_at || new Date().toISOString())
  ).run();
}

/* =========================
   Auth helpers
   ========================= */

async function requireOperatorAuth(request, env) {
  const bearerOk = await validateBearerAuth(request, env);
  if (bearerOk === true) return null;
  if (bearerOk instanceof Response) return bearerOk;

  const session = await getSessionFromCookie(request, env);
  if (session) return null;

  return json({ error: "Unauthorized" }, 401);
}

async function validateBearerAuth(request, env) {
  const authHeader = request.headers.get("Authorization") || "";
  const expected = env.OPERATOR_API_TOKEN || "";

  if (!authHeader) return false;
  if (!expected) return json({ error: "Server auth not configured" }, 500);
  if (!authHeader.startsWith("Bearer ")) return false;

  const supplied = authHeader.slice("Bearer ".length).trim();
  if (!supplied || supplied !== expected) {
    return json({ error: "Unauthorized" }, 401);
  }

  return true;
}

async function getSessionFromCookie(request, env) {
  const secret = safeString(env.OPERATOR_SESSION_SECRET);
  if (!secret) return null;

  const cookies = parseCookies(request.headers.get("Cookie") || "");
  const token = cookies.operator_session;
  if (!token) return null;

  const payload = await verifySessionToken(token, secret);
  if (!payload) return null;

  if (!payload.email || !payload.exp) return null;
  if (payload.exp < Math.floor(Date.now() / 1000)) return null;

  return payload;
}

function parseCookies(cookieHeader) {
  const out = {};
  for (const part of cookieHeader.split(";")) {
    const [rawKey, ...rest] = part.trim().split("=");
    if (!rawKey) continue;
    out[rawKey] = rest.join("=");
  }
  return out;
}

function buildSessionCookie(token) {
  return [
    `operator_session=${token}`,
    "HttpOnly",
    "Max-Age=28800",
    "Path=/",
    "SameSite=Lax",
    "Secure"
  ].join("; ");
}

function clearSessionCookie() {
  return [
    "operator_session=",
    "HttpOnly",
    "Max-Age=0",
    "Path=/",
    "SameSite=Lax",
    "Secure"
  ].join("; ");
}

async function createSessionToken(payload, secret) {
  const encoder = new TextEncoder();
  const body = base64UrlEncode(JSON.stringify(payload));
  const key = await crypto.subtle.importKey(
    "raw",
    encoder.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );

  const signature = await crypto.subtle.sign(
    "HMAC",
    key,
    encoder.encode(body)
  );

  return `${body}.${base64UrlEncodeBytes(new Uint8Array(signature))}`;
}

async function verifySessionToken(token, secret) {
  const parts = token.split(".");
  if (parts.length !== 2) return null;

  const [body, signature] = parts;
  const encoder = new TextEncoder();

  const key = await crypto.subtle.importKey(
    "raw",
    encoder.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["verify"]
  );

  const valid = await crypto.subtle.verify(
    "HMAC",
    key,
    base64UrlDecodeToBytes(signature),
    encoder.encode(body)
  );

  if (!valid) return null;

  try {
    return JSON.parse(base64UrlDecodeToString(body));
  } catch {
    return null;
  }
}

function base64UrlEncode(value) {
  const bytes = new TextEncoder().encode(value);
  return base64UrlEncodeBytes(bytes);
}

function base64UrlEncodeBytes(bytes) {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function base64UrlDecodeToBytes(value) {
  const padded = value.replace(/-/g, "+").replace(/_/g, "/") + "===".slice((value.length + 3) % 4);
  const binary = atob(padded);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
}

function base64UrlDecodeToString(value) {
  const bytes = base64UrlDecodeToBytes(value);
  return new TextDecoder().decode(bytes);
}

/* =========================
   JSON helpers
   ========================= */

async function parseJsonBody(request) {
  const contentType = request.headers.get("content-type") || "";
  if (!contentType.toLowerCase().includes("application/json")) {
    return json({ error: "Content-Type must be application/json" }, 415);
  }

  try {
    return await request.json();
  } catch (error) {
    console.error("Invalid JSON body:", error);
    return json({ error: "Invalid JSON body" }, 400);
  }
}

/* =========================
   Contract projections
   ========================= */

function buildEmptyNotesDocument(orderNumber) {
  return {
    contract: "order-notes.v1",
    items: [],
    order_number: orderNumber,
    updated_at: new Date().toISOString()
  };
}

function buildStatusLookupFromBuildRequest(buildRequest) {
  return {
    contract: "status-lookup.v1",
    customer_name: safeString(buildRequest.customer?.name),
    customer_visible_notes: safeString(buildRequest.customer_visible_notes),
    last_updated: safeString(buildRequest.updated_at || buildRequest.created_at),
    order_number: safeString(buildRequest.order_number),
    payment_status: {
      deposit: "Not Requested",
      final: "Not Requested"
    },
    project_notes: safeString(buildRequest.project_notes),
    services: Array.isArray(buildRequest.services) ? buildRequest.services : [],
    status: safeString(buildRequest.status || "Received"),
    status_label: safeString(buildRequest.status || "Received"),
    updated_at: safeString(buildRequest.updated_at || buildRequest.created_at),
    vehicle: {
      make: safeString(buildRequest.vehicle?.make),
      model: safeString(buildRequest.vehicle?.model),
      year: safeNumber(buildRequest.vehicle?.year)
    }
  };
}

function orderDetailProjection(buildRequest, statusLookup, estimate, paymentRequest, notes, activityLog) {
  return {
    activity_log: Array.isArray(activityLog) ? activityLog : [],
    created_at: safeString(buildRequest.created_at),
    customer: {
      email: safeString(buildRequest.customer?.email),
      name: safeString(buildRequest.customer?.name),
      phone: safeString(buildRequest.customer?.phone)
    },
    customer_visible_notes: safeString(buildRequest.customer_visible_notes),
    estimate: estimate || null,
    notes: Array.isArray(notes) ? notes : [],
    operator_notes: safeString(buildRequest.operator_notes),
    order_number: safeString(buildRequest.order_number),
    payment_request: paymentRequest || null,
    project_notes: safeString(buildRequest.project_notes),
    services: Array.isArray(buildRequest.services) ? buildRequest.services : [],
    status: safeString(buildRequest.status),
    status_label: safeString(statusLookup?.status_label || buildRequest.status),
    status_lookup: statusLookup || null,
    updated_at: safeString(buildRequest.updated_at || statusLookup?.updated_at || buildRequest.created_at),
    uploads: Array.isArray(buildRequest.uploads) ? buildRequest.uploads : [],
    vehicle: {
      make: safeString(buildRequest.vehicle?.make),
      model: safeString(buildRequest.vehicle?.model),
      year: safeNumber(buildRequest.vehicle?.year)
    }
  };
}

function orderListProjection(order) {
  return {
    created_at: safeString(order.created_at),
    customer_email: safeString(order.customer?.email),
    customer_name: safeString(order.customer?.name),
    order_number: safeString(order.order_number),
    services: Array.isArray(order.services) ? order.services : [],
    status: safeString(order.status),
    status_label: safeString(order.status),
    updated_at: safeString(order.updated_at || order.created_at),
    vehicle: {
      make: safeString(order.vehicle?.make),
      model: safeString(order.vehicle?.model),
      year: safeNumber(order.vehicle?.year)
    }
  };
}

function platformProfile(data) {
  return {
    analytics_owner: safeString(data.analytics_owner),
    backup_location: safeString(data.backup_location),
    dns_notes: safeString(data.dns_notes),
    dns_provider: safeString(data.dns_provider),
    edge_platform: safeString(data.edge_platform),
    git_provider: safeString(data.git_provider),
    handoff_brand_assets: Boolean(data.handoff_brand_assets),
    handoff_email_accounts: Boolean(data.handoff_email_accounts),
    handoff_recovery_codes: Boolean(data.handoff_recovery_codes),
    handoff_subscriptions: Boolean(data.handoff_subscriptions),
    handoff_tax_docs: Boolean(data.handoff_tax_docs),
    handoff_vendor_access: Boolean(data.handoff_vendor_access),
    hosting_account: safeString(data.hosting_account),
    primary_domain: safeString(data.primary_domain),
    profile_slug: safeString(data.profile_slug),
    registrar: safeString(data.registrar),
    registrar_login_owner: safeString(data.registrar_login_owner),
    repo_admin_username: safeString(data.repo_admin_username),
    repository_url: safeString(data.repository_url),
    secrets_confirmed: typeof data.secrets_confirmed === "boolean" ? data.secrets_confirmed : safeString(data.secrets_confirmed),
    transfer_notes: safeString(data.transfer_notes),
    updated_at: safeString(data.updated_at)
  };
}

function publicProfile(data) {
  return {
    business_city: safeString(data.business_city),
    business_email: safeString(data.business_email || data.support_email),
    business_name: safeString(data.business_name),
    business_phone: safeString(data.business_phone),
    business_state: safeString(data.business_state),
    hero_headline: safeString(data.hero_headline),
    hero_tagline: safeString(data.hero_tagline),
    operator_bio: safeString(data.operator_bio),
    operator_image: safeString(data.operator_image),
    operator_name: safeString(data.operator_name),
    profile_slug: safeString(data.profile_slug),
    projects_featured: Array.isArray(data.projects_featured) ? data.projects_featured : [],
    projects_gallery: Array.isArray(data.projects_gallery) ? data.projects_gallery : [],
    support_email: safeString(data.support_email),
    updated_at: safeString(data.updated_at),
    website: safeString(data.website)
  };
}

function settingsProfile(data) {
  return {
    analytics_owner: safeString(data.analytics_owner),
    backup_location: safeString(data.backup_location),
    billing_instructions: safeString(data.billing_instructions),
    business_address: safeString(data.business_address),
    business_hours: safeString(data.business_hours),
    business_name: safeString(data.business_name),
    business_phone: safeString(data.business_phone),
    default_deposit_percentage: safeNumber(data.default_deposit_percentage),
    deposit_policy_text: safeString(data.deposit_policy_text),
    dns_notes: safeString(data.dns_notes),
    dns_provider: safeString(data.dns_provider),
    edge_platform: safeString(data.edge_platform),
    git_provider: safeString(data.git_provider),
    handoff_brand_assets: Boolean(data.handoff_brand_assets),
    handoff_email_accounts: Boolean(data.handoff_email_accounts),
    handoff_recovery_codes: Boolean(data.handoff_recovery_codes),
    handoff_subscriptions: Boolean(data.handoff_subscriptions),
    handoff_tax_docs: Boolean(data.handoff_tax_docs),
    handoff_vendor_access: Boolean(data.handoff_vendor_access),
    hosting_account: safeString(data.hosting_account),
    notification_email: safeString(data.notification_email),
    operator_id: safeString(data.operator_id || data.profile_slug),
    operator_name: safeString(data.operator_name),
    payment_instructions: safeString(data.payment_instructions),
    payout_note: safeString(data.payout_note),
    privacy_policy_text: safeString(data.privacy_policy_text),
    profile_slug: safeString(data.profile_slug),
    profile_type: safeString(data.profile_type || "operator"),
    public_contact_name: safeString(data.public_contact_name),
    registrar: safeString(data.registrar),
    registrar_login_owner: safeString(data.registrar_login_owner),
    repo_admin_username: safeString(data.repo_admin_username),
    repository_url: safeString(data.repository_url),
    secrets_confirmed: typeof data.secrets_confirmed === "boolean" ? data.secrets_confirmed : safeString(data.secrets_confirmed),
    stripe_publishable_key: safeString(data.stripe_publishable_key),
    stripe_secret_key: safeString(data.stripe_secret_key),
    stripe_webhook_secret: safeString(data.stripe_webhook_secret),
    support_email: safeString(data.support_email),
    terms_of_service_text: safeString(data.terms_of_service_text),
    transfer_notes: safeString(data.transfer_notes),
    updated_at: safeString(data.updated_at)
  };
}

async function updateStatusLookupPayment(orderNumber, paymentRequest, env) {
  const existing = await loadCurrentBlob(`status-lookup/${orderNumber}`, env);
  if (!existing) return;

  const paymentStatus = {
    deposit: safeString(existing.payment_status?.deposit || "Not Requested"),
    final: safeString(existing.payment_status?.final || "Not Requested")
  };

  const mappedStatus = mapPaymentStatusForLookup(paymentRequest.status);
  if (paymentRequest.payment_type === "deposit") {
    paymentStatus.deposit = mappedStatus;
  }
  if (paymentRequest.payment_type === "final") {
    paymentStatus.final = mappedStatus;
  }

  const now = new Date().toISOString();
  const updated = {
    ...existing,
    last_updated: now,
    payment_status: paymentStatus,
    updated_at: now
  };

  await saveVersionedBlob(`status-lookup/${orderNumber}`, updated, env);
}

/* =========================
   Validation + normalization
   ========================= */

function clampInt(value, fallback, min, max) {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(max, Math.max(min, parsed));
}

function generateOrderNumber() {
  const now = new Date();
  const suffix = String(Math.floor(Math.random() * 9000) + 1000);
  const year = now.getUTCFullYear();
  return `GKI-${year}-${suffix}`;
}

function mapPaymentStatusForLookup(status) {
  if (status === "Paid") return "Paid";
  if (status === "Requested") return "Requested";
  return "Not Requested";
}

function normalizeNoteVisibility(value) {
  const normalized = safeString(value).toLowerCase();
  return normalized === "customer" ? "customer" : "internal";
}

function normalizeOrderStatus(value) {
  return safeString(value);
}

function normalizePaymentStatus(value) {
  const normalized = safeString(value).toLowerCase();
  if (normalized === "cancelled") return "Cancelled";
  if (normalized === "paid") return "Paid";
  return "Requested";
}

function normalizePaymentType(value) {
  const normalized = safeString(value).toLowerCase();
  return normalized === "final" ? "final" : "deposit";
}

function normalizeServiceArray(services) {
  if (!Array.isArray(services)) return [];
  const allowed = new Set(["audio", "fabrication", "lighting", "wraps"]);
  return [...new Set(
    services
      .map((item) => safeString(item).toLowerCase())
      .filter((item) => allowed.has(item))
  )];
}

function normalizeStatusForFilter(value) {
  return safeString(value).toLowerCase().replace(/\s+/g, "_");
}

function normalizeUploads(uploads) {
  if (!Array.isArray(uploads)) return [];
  return uploads
    .filter((item) => item && typeof item === "object")
    .map((item) => ({
      file_name: safeString(item.file_name),
      r2_key: safeString(item.r2_key)
    }))
    .filter((item) => item.file_name || item.r2_key);
}

function roundCurrency(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return 0;
  return Math.round(num * 100) / 100;
}

function safeNullableString(value, fallback = null) {
  if (value === null) return null;
  if (value === undefined) return fallback;
  return String(value).trim();
}

function safeNumber(value, fallback = 0) {
  if (value === null || value === undefined || value === "") return fallback;
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function safeOrderNumber(value) {
  return safeString(value).replace(/[^a-zA-Z0-9_-]/g, "").toUpperCase();
}

function safeSlug(value) {
  return safeString(value).replace(/[^a-zA-Z0-9_-]/g, "");
}

function safeString(value, fallback = "") {
  if (value === null || value === undefined) return fallback;
  return String(value).trim();
}

function validateBuildRequest(body) {
  if (!body || typeof body !== "object") return "Body must be a JSON object";

  if (!body.customer || typeof body.customer !== "object") {
    return "customer is required";
  }

  if (!body.vehicle || typeof body.vehicle !== "object") {
    return "vehicle is required";
  }

  if (!safeString(body.customer.email)) return "customer.email is required";
  if (!safeString(body.customer.name)) return "customer.name is required";
  if (!safeString(body.customer.phone)) return "customer.phone is required";
  if (!safeString(body.vehicle.make)) return "vehicle.make is required";
  if (!safeString(body.vehicle.model)) return "vehicle.model is required";

  const year = Number(body.vehicle.year);
  if (!Number.isFinite(year)) return "vehicle.year must be a number";

  return null;
}

/* =========================
   Response helpers
   ========================= */

function json(data, status = 200) {
  return withCors(new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=UTF-8"
    }
  }));
}

function methodNotAllowed(allowed) {
  return withCors(new Response(JSON.stringify({ error: "Method not allowed" }, null, 2), {
    status: 405,
    headers: {
      allow: allowed.join(", "),
      "content-type": "application/json; charset=UTF-8"
    }
  }));
}

function withCors(response) {
  const headers = new Headers(response.headers);
  headers.set("access-control-allow-origin", "*");
  headers.set("access-control-allow-methods", "GET, OPTIONS, PATCH, POST");
  headers.set("access-control-allow-headers", "Authorization, Content-Type");

  return new Response(response.body, {
    headers,
    status: response.status,
    statusText: response.statusText
  });
}

function withCorsAndCookie(response, cookieValue) {
  const wrapped = withCors(response);
  const headers = new Headers(wrapped.headers);
  headers.append("set-cookie", cookieValue);

  return new Response(wrapped.body, {
    headers,
    status: wrapped.status,
    statusText: wrapped.statusText
  });
}