export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/api/health") {
      return Response.json({
        ok: true,
        service: "gottaknowitcustomsllc",
        hasAssets: Boolean(env.ASSETS),
        hasDb: Boolean(env.DB),
        hasR2: Boolean(env.OPERATOR_BLOBS)
      });
    }

    if (url.pathname.startsWith("/api/")) {
      return Response.json(
        { error: "Not found" },
        { status: 404 }
      );
    }

    return env.ASSETS.fetch(request);
  }
};