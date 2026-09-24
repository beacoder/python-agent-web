// Entry point: importing each feature module runs its top-level DOM
// wiring (buttons, inputs, document listeners).  Order is not important
// — the modules only call each other from event handlers, never at
// load time — but core first reads clearest.
import { api } from "./core.js";
import { showApp, showAuth } from "./auth.js";
import "./account.js";
import "./conversations.js";
import "./chat.js";

// A saved token skips the login screen; the cache fix (else branch)
// makes the initial view deterministic on a fresh/cleared session.
if (api.token) showApp();
else showAuth();
