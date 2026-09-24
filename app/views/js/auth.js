// Auth + top-level view switching (login screen ⇄ app ⇄ profile) and
// the theme toggle.
import { $, api, errMsg, state } from "./core.js";
import { loadMe } from "./account.js";
import { loadConversations } from "./conversations.js";

export function showAuth(msg) {
  $("app").style.display = "none";
  $("auth").style.display = "flex";
  $("profile").style.display = "none";
  $("authMsg").textContent = msg || "";
  sessionStorage.removeItem("token");
  api.token = "";
  state.accountProfile = null; // don't leak one user's credits to the next
}

export function showApp() {
  $("auth").style.display = "none";
  $("app").style.display = "block";
  $("profile").style.display = "none";
  loadConversations();
  loadMe();
}

// A 401 from any api.req bounces back to the login screen.
api.onUnauthorized = () => showAuth("登录已过期，请重新登录");

$("loginBtn").onclick = async () => {
  const email = $("email").value.trim(),
    password = $("password").value;
  let res = await fetch("/auth/login", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email, password }),
  });
  if (res.status === 401 || res.status === 403) {
    res = await fetch("/auth/register", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email, password }),
    });
  }
  if (!res.ok) {
    $("authMsg").textContent = "登录失败: " + (await errMsg(res));
    return;
  }
  const tokens = await res.json();
  api.token = tokens.access_token;
  sessionStorage.setItem("token", api.token);
  showApp();
};
$("password").addEventListener("keydown", (e) => {
  if (e.key === "Enter") $("loginBtn").click();
});

$("themeBtn").onclick = () => {
  const dark = document.documentElement.dataset.theme === "dark";
  document.documentElement.dataset.theme = dark ? "" : "dark";
  $("themeBtn").textContent = dark ? "🌙 深色" : "☀️ 浅色";
};
