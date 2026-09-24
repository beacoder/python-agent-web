// Account dropdown + 个人中心 profile page.
// Real data: name/email/avatar, plan, points/expiry (from
// /account/profile), and "我的创作" = the user's conversations.  The
// social counters (收藏/点赞/互动) and phone binding have no backend yet
// and remain placeholders until endpoints exist.
import { $, api, state } from "./core.js";
import { showApp, showAuth } from "./auth.js";
import { openConversation } from "./conversations.js";

export async function loadMe() {
  const res = await api.req("/auth/me");
  if (!res.ok) return;
  const me = await res.json();
  // No display-name field on the backend yet — derive one from the email
  // local part so the menu reads like the mock rather than a raw address.
  const name = me.email.split("@")[0];
  $("avatar").textContent = me.email[0].toUpperCase();
  $("acctName").textContent = name;
  $("menuName").textContent = name;
  $("menuEmail").textContent = me.email;
  loadCredits();
}

// The account panel and 个人中心 page share one read model:
// GET /account/profile.
async function loadProfile(force = false) {
  if (state.accountProfile && !force) return state.accountProfile;
  const res = await api.req("/account/profile");
  state.accountProfile = res.ok ? await res.json() : null;
  return state.accountProfile;
}

async function loadCredits() {
  const p = await loadProfile();
  if (!p) return;
  const expiry = p.points_expire_at ? p.points_expire_at.slice(0, 10) : "无有效期";
  $("menuPlan").textContent = (p.plan || "free").toUpperCase();
  $("stPoints").textContent = p.points;
  $("stPlanPoints").textContent = p.plan_points;
  $("stPackPoints").textContent = p.pack_points;
  $("stExpiry").textContent = expiry;
  $("acctPts").textContent = "🪙 " + p.points;
}

/* ---------- account menu ---------- */
function toggleAccountMenu(open) {
  const menu = $("accountMenu");
  const show = open ?? !menu.classList.contains("open");
  menu.classList.toggle("open", show);
  $("accountBtn").setAttribute("aria-expanded", String(show));
}
$("accountBtn").onclick = (e) => {
  e.stopPropagation();
  toggleAccountMenu();
};
// click-outside and Esc close the menu
document.addEventListener("click", (e) => {
  if (!$("account").contains(e.target)) toggleAccountMenu(false);
});
document.addEventListener("keydown", (e) => {
  if (e.key === "Escape") toggleAccountMenu(false);
});
$("menuLogout").onclick = async () => {
  toggleAccountMenu(false);
  // revoke server-side (best-effort) before dropping the local token
  try {
    await api.req("/auth/logout", { method: "POST" });
  } catch {
    /* already unauthenticated / offline — clear locally anyway */
  }
  showAuth("已退出登录");
};
$("menuProfile").onclick = () => {
  toggleAccountMenu(false);
  showProfile();
};
$("menuUsage").onclick = () => {
  toggleAccountMenu(false);
};

/* ---------- profile page (个人中心) ---------- */
async function showProfile() {
  $("app").style.display = "none";
  $("auth").style.display = "none";
  $("profile").style.display = "block";
  const p = await loadProfile(true);
  if (p) {
    $("pAvatar").textContent = p.email[0].toUpperCase();
    $("pName").textContent = p.name;
    $("pPhone").textContent = p.phone || "未绑定";
    $("pBind").style.display = p.phone ? "none" : "";
    $("pPlan").textContent = (p.plan || "free").toUpperCase();
    $("pPoints").textContent = p.points;
    $("pPlanPoints").textContent = p.plan_points;
    $("pPackPoints").textContent = p.pack_points;
    const expiry = p.points_expire_at ? p.points_expire_at.slice(0, 10) : "无有效期";
    $("pExpiry").textContent = expiry;
    $("pSubExpiry").textContent = expiry;
  }
  selectProfileTab("works");
}

function selectProfileTab(which) {
  for (const [id, key] of [
    ["tabWorks", "works"],
    ["tabFav", "fav"],
    ["tabLike", "like"],
  ]) {
    $(id).classList.toggle("active", key === which);
  }
  if (which === "works") loadWorks();
  else renderWorks([]); // 收藏/点赞 have no backend — always empty
}

async function loadWorks() {
  const res = await api.req("/conversations");
  const list = res.ok ? await res.json() : [];
  $("tabWorksN").textContent = list.length;
  $("worksCount").textContent = list.length;
  renderWorks(list);
}

function renderWorks(list) {
  const box = $("works");
  if (!list.length) {
    box.innerHTML =
      '<div class="empty"><div class="eic">📄</div>' +
      '<div class="big">暂无创作</div>' +
      "<div>生成结果后会自动保存在这里，便于回顾和编辑</div></div>";
    return;
  }
  const grid = document.createElement("div");
  grid.className = "worksGrid";
  for (const c of list) {
    const card = document.createElement("div");
    card.className = "work";
    const t = document.createElement("div");
    t.className = "t";
    t.textContent = c.title || "未命名对话";
    const m = document.createElement("div");
    m.className = "m";
    m.textContent = (c.created_at || "").slice(0, 10);
    card.append(t, m);
    card.onclick = () => {
      showApp();
      openConversation(c.id, c.title || "");
    };
    grid.appendChild(card);
  }
  box.innerHTML = "";
  box.appendChild(grid);
}

$("tabWorks").onclick = () => selectProfileTab("works");
$("tabFav").onclick = () => selectProfileTab("fav");
$("tabLike").onclick = () => selectProfileTab("like");
// 首页 returns to the main app from the profile page
$("navHome").onclick = (e) => {
  e.preventDefault();
  showApp();
};
