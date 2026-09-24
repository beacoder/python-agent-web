// Foundation shared by every feature module: the DOM helper, the
// mutable app state, and the fetch wrapper.  Imports nothing else, so
// it sits at the bottom of the dependency graph.

export const $ = (id) => document.getElementById(id);

export function escapeHtml(s) {
  return String(s).replace(
    /[&<>"']/g,
    (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c],
  );
}

// Cross-module mutable state.  It lives on one object because an
// imported binding can't be reassigned from another module — but its
// PROPERTIES can, so every feature reads/writes state.currentConv etc.
export const state = {
  currentConv: null,
  currentRun: null,
  es: null,
  pendingFiles: [], // uploaded file rows for the current conversation
  accountProfile: null, // cached /account/profile read model
};

export const api = {
  token: sessionStorage.getItem("token") || "",
  // Set by auth.js so a 401 can bounce to the login screen without the
  // foundation having to import a feature module (keeps the graph acyclic
  // at this layer).
  onUnauthorized: null,
  async req(path, opts = {}) {
    const res = await fetch(path, {
      ...opts,
      headers: {
        ...(opts.body instanceof FormData ? {} : { "Content-Type": "application/json" }),
        ...(this.token ? { Authorization: "Bearer " + this.token } : {}),
        ...(opts.headers || {}),
      },
    });
    if (res.status === 401) {
      if (this.onUnauthorized) this.onUnauthorized();
      throw new Error("unauthorized");
    }
    return res;
  },
};

export function errMsg(res) {
  return res
    .json()
    .then((j) => {
      const d = j.detail ?? j;
      return Array.isArray(d)
        ? d.map((e) => `${(e.loc || []).slice(1).join(".")}: ${e.msg}`).join("; ")
        : String(d);
    })
    .catch(() => res.statusText || "request failed");
}
