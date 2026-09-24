// Conversations: the sidebar list + CRUD, opening a conversation (with
// its message history), plus the per-conversation uploaded files and
// downloadable artifacts (all keyed by state.currentConv).
import { $, api, errMsg, escapeHtml, state } from "./core.js";
import { addMsg, replayRun, setRunStatus } from "./chat.js";

export async function loadConversations() {
  const res = await api.req("/conversations");
  const list = await res.json();
  const box = $("convList");
  box.innerHTML = "";
  if (!list.length) {
    box.innerHTML = '<div class="empty">暂无历史作品</div>';
    return;
  }
  for (const c of list) {
    const div = document.createElement("div");
    div.className = "cnv" + (c.id === state.currentConv ? " active" : "");
    div.innerHTML = `<span style="overflow:hidden;text-overflow:ellipsis">${escapeHtml(c.title)}</span><span class="del" title="删除">✕</span>`;
    div.onclick = (e) => {
      if (e.target.classList.contains("del")) {
        deleteConversation(c.id);
        return;
      }
      openConversation(c.id, c.title);
    };
    box.appendChild(div);
  }
}

async function deleteConversation(id) {
  if (!confirm("删除该对话及其上传文件？")) return;
  await api.req("/conversations/" + id, { method: "DELETE" });
  if (state.currentConv === id) newConversation();
  loadConversations();
}

export function newConversation() {
  state.currentConv = null;
  state.currentRun = null;
  if (state.es) {
    state.es.close();
    state.es = null;
  }
  state.pendingFiles = [];
  renderFiles();
  setRunStatus("", false);
  $("artifacts").style.display = "none";
  $("promptBox").value = "";
  $("charCount").textContent = "0";
  $("home").style.display = "block";
  $("chat").style.display = "none";
  loadConversations();
}
$("newConvBtn").onclick = newConversation;

export async function openConversation(id, title) {
  state.currentConv = id;
  if (state.es) {
    state.es.close();
    state.es = null;
  }
  state.currentRun = null;
  setRunStatus("", false);
  const res = await api.req("/conversations/" + id);
  const detail = await res.json();
  state.pendingFiles = detail.files || [];
  renderFiles();
  $("home").style.display = "none";
  $("chat").style.display = "block";
  const box = $("messages");
  box.innerHTML = "";
  for (const run of detail.runs) {
    addMsg("user", run.prompt);
    replayRun(run);
  }
  loadConversations();
  loadArtifacts();
}

/* ---------- artifacts ---------- */
export async function loadArtifacts() {
  const box = $("artifactChips");
  if (!state.currentConv) {
    $("artifacts").style.display = "none";
    return;
  }
  const res = await api.req(`/conversations/${state.currentConv}/artifacts`);
  if (!res.ok) {
    $("artifacts").style.display = "none";
    return;
  }
  const list = await res.json();
  box.innerHTML = "";
  for (const a of list) {
    const link = document.createElement("a");
    link.className = "artifact";
    link.href = "#";
    link.innerHTML = `⬇ ${escapeHtml(a.name)} <span class="size">${fmtSize(a.size)}</span>`;
    link.onclick = async (e) => {
      e.preventDefault();
      const r = await api.req(
        `/conversations/${state.currentConv}/artifacts/${encodeURIComponent(a.name)}`,
      );
      if (!r.ok) {
        alert("下载失败");
        return;
      }
      const url = URL.createObjectURL(await r.blob());
      const tmp = document.createElement("a");
      tmp.href = url;
      tmp.download = a.name;
      tmp.click();
      URL.revokeObjectURL(url);
    };
    box.appendChild(link);
  }
  $("artifacts").style.display = list.length ? "block" : "none";
}

function fmtSize(n) {
  if (n < 1024) return n + " B";
  if (n < 1024 * 1024) return (n / 1024).toFixed(1) + " KB";
  return (n / 1024 / 1024).toFixed(1) + " MB";
}

/* ---------- files ---------- */
function renderFiles() {
  for (const box of [$("fileChips"), $("fileChips2")]) {
    box.innerHTML = "";
    for (const f of state.pendingFiles) {
      const chip = document.createElement("span");
      chip.className = "chip";
      chip.innerHTML = `📄 ${escapeHtml(f.filename)} <span class="x" title="移除">✕</span>`;
      chip.querySelector(".x").onclick = async () => {
        await api.req(`/conversations/${state.currentConv}/files/${f.id}`, { method: "DELETE" });
        state.pendingFiles = state.pendingFiles.filter((x) => x.id !== f.id);
        renderFiles();
      };
      box.appendChild(chip);
    }
  }
}

async function uploadFiles(input) {
  const files = [...input.files];
  input.value = "";
  if (!files.length) return;
  if (!state.currentConv) {
    const res = await api.req("/conversations", {
      method: "POST",
      body: JSON.stringify({ title: "Excel 任务" }),
    });
    const c = await res.json();
    state.currentConv = c.id;
  }
  for (const f of files) {
    const fd = new FormData();
    fd.append("file", f);
    const res = await api.req(`/conversations/${state.currentConv}/files`, {
      method: "POST",
      body: fd,
    });
    if (!res.ok) {
      alert("上传失败: " + (await errMsg(res)));
      continue;
    }
    state.pendingFiles.push(await res.json());
  }
  renderFiles();
  loadConversations();
}
$("attachBtn").onclick = () => $("fileInput").click();
$("attachBtn2").onclick = () => $("fileInput2").click();
$("fileInput").onchange = (e) => uploadFiles(e.target);
$("fileInput2").onchange = (e) => uploadFiles(e.target);
