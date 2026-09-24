// Chat: the task-card presets, the composer/send flow, the run SSE
// stream, and per-turn rendering.
//
// Each assistant turn renders as a collapsible "执行过程" block holding
// the intermediate work — tool calls, progress, todos, logs, and the
// reasoning text that led to more tool calls — and, below it, the final
// assistant answer shown prominently.  The process is expanded live (so
// the user watches progress) and collapsed once the turn is replayed
// from history.
import { $, api, errMsg, escapeHtml, state } from "./core.js";
import { openConversation, loadConversations, loadArtifacts } from "./conversations.js";

const TASKS = [
  { icon: "🧹", name: "数据清洗", tpl: "请清洗上传的表格：去除重复行、处理缺失值、统一日期与数字格式，输出清洗后的文件。" },
  { icon: "🧮", name: "公式计算", tpl: "请在表格中新增计算列（如合计、占比、同比增长率），并说明使用的公式。" },
  { icon: "📊", name: "图表生成", tpl: "请根据表格数据生成合适的图表（柱状图/折线图/饼图），保存为图片并给出分析结论。" },
  { icon: "🎨", name: "格式美化", tpl: "请美化表格：设置表头样式、列宽、数字格式、冻结首行，并添加边框。" },
  { icon: "🔗", name: "数据合并", tpl: "请将上传的多个表格按关键列合并（VLOOKUP/JOIN 语义），输出合并后的结果。" },
  { icon: "📑", name: "报表生成", tpl: "请基于表格数据生成一份汇总报表：关键指标、分组统计，并输出为新的 Excel 文件。" },
];

/* ---------- task cards ---------- */
const grid = $("taskGrid");
for (const t of TASKS) {
  const btn = document.createElement("button");
  btn.className = "task";
  btn.innerHTML = `<div class="thumb">${t.icon}</div><div class="name">${t.name}</div>`;
  btn.onclick = () => {
    const box = $("promptBox");
    box.value = (box.value ? box.value + "\n" : "") + t.tpl;
    $("charCount").textContent = box.value.length;
    box.focus();
  };
  grid.appendChild(btn);
}
$("promptBox").addEventListener("input", (e) => {
  $("charCount").textContent = e.target.value.length;
});

/* ---------- small helpers ---------- */
function el(tag, cls) {
  const d = document.createElement(tag);
  if (cls) d.className = cls;
  return d;
}
function scrollMessages() {
  $("messages").scrollTop = $("messages").scrollHeight;
}

// A plain bubble — used for the user's prompt and for hard errors that
// must stay visible (never hidden inside a process block).
export function addMsg(kind, text) {
  const div = el("div", "msg " + kind);
  if (kind === "agent") div.appendChild(renderAnswer(text));
  else div.textContent = text;
  $("messages").appendChild(div);
  scrollMessages();
  return div;
}

// minimal markdown for the answer: code fences + bold
function renderAnswer(text) {
  const parts = String(text).split(/```/);
  const frag = document.createDocumentFragment();
  parts.forEach((p, i) => {
    if (i % 2 === 1) {
      const nl = p.indexOf("\n");
      const pre = document.createElement("pre");
      pre.textContent = nl >= 0 ? p.slice(nl + 1) : p;
      frag.appendChild(pre);
    } else {
      const span = document.createElement("span");
      span.innerHTML = escapeHtml(p)
        .replace(/\*\*(.+?)\*\*/g, "<b>$1</b>")
        .replace(/\n/g, "<br>");
      frag.appendChild(span);
    }
  });
  return frag;
}

/* ---------- assistant turn (collapsible process + answer) ---------- */
// A turn context bundles the DOM and the per-round bookkeeping so tool
// progress, intermediate reasoning, and the final answer stay separated.
function newTurn(collapsed) {
  const wrap = el("div", "turn");
  const proc = el("div", "proc" + (collapsed ? " collapsed" : ""));
  proc.style.display = "none"; // revealed on the first process event
  const head = el("button", "proc-head");
  head.type = "button";
  head.innerHTML =
    '<span class="chev">▸</span><span class="proc-label">执行过程</span><span class="proc-meta"></span>';
  const body = el("div", "proc-body");
  head.onclick = () => proc.classList.toggle("collapsed");
  proc.append(head, body);
  const ans = el("div", "answer msg agent");
  wrap.append(proc, ans);
  $("messages").appendChild(wrap);
  scrollMessages();
  return {
    wrap,
    proc,
    head,
    body,
    ans,
    span: null, // current answer text span
    live: null, // current in-place progress row for the round
    todos: null, // updating todos block
    used: false, // any process content yet?
    roundLabel: "",
    roundArgs: {},
  };
}

function revealProc(t) {
  if (!t.used) {
    t.proc.style.display = "";
    t.used = true;
  }
}

function procProgress(t, text, busy, fresh) {
  revealProc(t);
  if (!t.live || fresh) {
    t.live = el("div", "progress");
    t.body.appendChild(t.live);
  }
  t.live.innerHTML =
    (busy ? '<span class="spin">⚙️</span>' : "<span>✅</span>") +
    "<span>" +
    escapeHtml(text) +
    "</span>";
  scrollMessages();
}

// Every assistant text segment stays visible in front — a round that
// ended in tool calls "seals" its text as a rendered block and the next
// round's text starts a new block below it.  Only the tool execution is
// hidden (in the collapsed process block).
function sealSegment(t) {
  if (!t.span) return;
  const seg = t.span.parentElement;
  const text = t.span.textContent;
  if (text.trim()) {
    seg.innerHTML = "";
    seg.appendChild(renderAnswer(text)); // render markdown, keep visible
  } else {
    seg.remove(); // nothing was said this round
  }
  t.span = null;
}

// A retried round discarded its partial output (harness semantics), so
// drop the partial segment rather than showing it.
function dropSegment(t) {
  if (t.span) {
    t.span.parentElement.remove();
    t.span = null;
  }
}

function answerSpan(t) {
  if (!t.span) {
    const seg = el("div", "answer-seg");
    t.span = document.createElement("span");
    seg.appendChild(t.span);
    t.ans.appendChild(seg);
  }
  return t.span;
}

function renderTurnTodos(t, items) {
  revealProc(t);
  if (!t.todos) {
    t.todos = el("div", "proc-text");
    t.body.appendChild(t.todos);
  }
  if (!items || !items.length) {
    t.todos.textContent = "";
    return;
  }
  t.todos.textContent =
    "📋 任务进度\n" +
    items
      .map((td) => {
        const s = String(td.status || "");
        const icon = /done|complete/i.test(s) ? "✅" : /progress|active|current/i.test(s) ? "🔄" : "⬜";
        return `${icon} ${td.content || ""}`;
      })
      .join("\n");
}

function strData(d) {
  if (d == null) return "";
  if (typeof d === "string") return d;
  if (Array.isArray(d)) return d.map(strData).join(", ");
  return JSON.stringify(d);
}

// The harness's "tool_calls" payload -> "Bash(command='ls -la')".
function toolCallLabel(calls) {
  if (!Array.isArray(calls)) return "";
  return calls
    .map((c) => toolOneLabel(c))
    .filter(Boolean)
    .join(", ");
}
function toolOneLabel(c) {
  const name = (c && c.name) || "tool";
  const args = (c && typeof c.args === "object" && c.args) || {};
  const params = Object.keys(args)
    .map((k) => k + "=" + args[k])
    .join(" ");
  return params ? name + "(" + params + ")" : name;
}

// Harness log lines that are bookkeeping rather than progress.
const LOG_SKIP = [/^session titled\b/];

// Fold one harness event (live SSE or stored replay) into a turn:
// intermediate work goes to the process block, answer deltas grow the
// prominent answer, errors/questions stay visible.
function renderEvent(e, t, live = true) {
  if (e.type === "delta") {
    answerSpan(t).textContent += e.data?.text ?? "";
    scrollMessages();
  } else if (e.type === "notify" && e.data) {
    const kind = e.data.kind,
      data = e.data.data;
    if (kind === "tool_start") {
      sealSegment(t); // keep the round's text visible, start a new block after
      t.roundLabel = strData(data);
      t.roundArgs = {};
      procProgress(t, "准备执行: " + t.roundLabel, true, true);
    } else if (kind === "tool_calls") {
      const label = toolCallLabel(data);
      if (label) {
        t.roundLabel = label;
        for (const c of data) if (c && c.name) t.roundArgs[c.name] = toolOneLabel(c);
        procProgress(t, "准备执行: " + label, true);
      }
    } else if (kind === "tool_running") {
      const name = strData(data);
      procProgress(t, "正在执行: " + (t.roundArgs[name] || name), true);
    } else if (kind === "tool") {
      procProgress(t, t.roundLabel ? "完成: " + t.roundLabel : "工具执行完成", false);
    } else if (kind === "todos") {
      renderTurnTodos(t, data);
    } else if (kind === "error") {
      const d = el("div", "msg err");
      d.textContent = typeof data === "string" ? data : JSON.stringify(data);
      t.wrap.appendChild(d);
    } else if (kind === "retry") {
      dropSegment(t); // the partial output was discarded on retry
      procProgress(t, "模型调用重试中…", true, true);
    } else if (kind === "compact") {
      procProgress(t, "正在压缩上下文…", true);
    } else if (live && (kind === "ask" || kind === "confirm")) {
      addAsk(t, data, e.run_id || state.currentRun);
    }
  } else if (e.type === "log" && e.data && e.data.message) {
    const msg = String(e.data.message);
    if (!LOG_SKIP.some((re) => re.test(msg))) procProgress(t, msg, true, true);
  }
}

// Convert the streamed plain-text answer into rendered markdown.  Called
// when a turn is finalized (history replay); the live turn is replaced by
// a replay when the run ends, so live streaming stays plain.
// Render the final open segment as markdown when a turn is finalized
// (history replay). Earlier segments were already sealed at their
// tool_start boundaries.
function finalizeAnswer(t) {
  sealSegment(t);
}

// Render a finished run from stored data: process collapsed, final
// answer in front.  The stored delta events are already [FINAL CHECK]-
// filtered by the harness, so the reconstructed answer equals run.answer.
export function replayRun(run) {
  const t = newTurn(true);
  if (run.events && run.events.length) {
    for (const e of run.events) renderEvent(e, t, false);
  } else if (run.answer) {
    answerSpan(t).textContent = run.answer;
  }
  if (run.error) {
    const d = el("div", "msg err");
    d.textContent = run.error;
    t.wrap.appendChild(d);
  }
  if (run.duration_ms) {
    t.head.querySelector(".proc-meta").textContent = (run.duration_ms / 1000).toFixed(0) + "s";
  }
  finalizeAnswer(t);
}

function addAsk(t, data, runId) {
  const div = el("div", "msg agent");
  const questions = (data && data.questions) || [];
  div.innerHTML = '<div class="who">AI 提问</div>';
  for (const q of questions) {
    const qtext = q.question || q.text || JSON.stringify(q);
    const opts = q.options || [];
    const row = document.createElement("div");
    row.style.margin = "8px 0";
    row.innerHTML = `<div>${escapeHtml(qtext)}</div>`;
    const input = document.createElement("input");
    input.style.cssText =
      "width:100%;margin-top:6px;background:var(--bg);border:1px solid var(--line);border-radius:8px;padding:8px;color:var(--fg)";
    input.placeholder = opts.length ? "选择或输入…" : "输入回答…";
    row.appendChild(input);
    div.appendChild(row);
    for (const o of opts) {
      const label = typeof o === "string" ? o : o.label || JSON.stringify(o);
      const b = document.createElement("button");
      b.className = "attach-btn";
      b.style.margin = "0 6px 6px 0";
      b.textContent = label;
      b.onclick = () => {
        input.value = label;
      };
      row.appendChild(b);
    }
  }
  const send = document.createElement("button");
  send.style.cssText =
    "background:var(--accent);color:#fff;border:0;border-radius:8px;padding:7px 16px;margin-top:6px";
  send.textContent = "回答";
  send.onclick = async () => {
    const answers = [...div.querySelectorAll("input")].map((i) => i.value.trim()).filter(Boolean);
    if (!answers.length) return;
    send.disabled = true;
    const res = await api.req(`/conversations/${state.currentConv}/runs/${runId}/answer`, {
      method: "POST",
      body: JSON.stringify({ answers }),
    });
    if (!res.ok) {
      send.disabled = false;
      addMsg("err", await errMsg(res));
      return;
    }
    div.style.opacity = ".5";
  };
  div.appendChild(send);
  t.wrap.appendChild(div);
  scrollMessages();
}

export function setRunStatus(text, busy) {
  const box = $("runStatus");
  if (!text) {
    box.style.display = "none";
    return;
  }
  box.style.display = "flex";
  box.innerHTML =
    (busy ? '<span class="spin">⚙️</span>' : "") + "<span>" + escapeHtml(text) + "</span>";
}

/* ---------- run + stream ---------- */
export function startStream(runId) {
  state.currentRun = runId;
  $("stopBtn2").style.display = "inline-block";
  setRunStatus("正在启动…", true);
  const url = `/conversations/${state.currentConv}/runs/${runId}/stream?access_token=${encodeURIComponent(api.token)}`;
  state.es = new EventSource(url);
  const turn = newTurn(false); // live: process expanded so progress is visible
  state.es.onmessage = (ev) => {
    const e = JSON.parse(ev.data);
    if (e.type === "run") {
      state.es.close();
      state.es = null;
      state.currentRun = null;
      $("stopBtn2").style.display = "none";
      setRunStatus("", false);
      // reload: the transient live turn is replaced by the collapsed,
      // canonical replay of the stored run
      openConversation(state.currentConv, "");
      loadConversations();
      loadArtifacts();
      return;
    }
    renderEvent(e, turn, true);
  };
  state.es.onerror = () => {
    /* refreshed on run end */
  };
}

async function send() {
  const prompt = $("promptBox2").value.trim();
  if (!prompt) return;
  $("promptBox2").value = "";
  addMsg("user", prompt);
  if (!state.currentConv) {
    const res = await api.req("/conversations", {
      method: "POST",
      body: JSON.stringify({ title: prompt.slice(0, 40) }),
    });
    const c = await res.json();
    state.currentConv = c.id;
  }
  const res = await api.req(`/conversations/${state.currentConv}/runs`, {
    method: "POST",
    body: JSON.stringify({ prompt }),
  });
  if (!res.ok) {
    addMsg("err", await errMsg(res));
    return;
  }
  const run = await res.json();
  startStream(run.id);
}
$("sendBtn").onclick = () => {
  const p = $("promptBox").value.trim();
  if (!p) return;
  $("promptBox2").value = p;
  $("home").style.display = "none";
  $("chat").style.display = "block";
  send();
};
$("sendBtn2").onclick = send;
$("promptBox2").addEventListener("keydown", (e) => {
  if (e.key === "Enter" && !e.shiftKey) {
    e.preventDefault();
    send();
  }
});
$("stopBtn").onclick = $("stopBtn2").onclick = async () => {
  if (!state.currentRun) return;
  await api.req(`/conversations/${state.currentConv}/runs/${state.currentRun}/cancel`, {
    method: "POST",
  });
};
