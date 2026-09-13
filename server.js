import http from "node:http";
import { mkdir, readFile, writeFile, rename } from "node:fs/promises";
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const dbPath = process.env.DB_PATH || join(__dirname, "data", "core-slices.json");
const port = Number(process.env.PORT || 3025);

// 切片按"取样 → 切割 → 研磨 → 染色 → 观察"顺序推进
const taskSteps = ["取样", "切割", "研磨", "染色", "观察"];
const reworkSteps = ["取样", "切割", "研磨", "染色"]; // 观察不合格可退回的工序
const statuses = ["待切割", "制片中", "待观察", "已交付"];
// 每道工序的默认试剂配方（单位 ml，0 表示该工序不耗试剂）
const defaultRecipe = { 取样: 0, 切割: 2, 研磨: 1, 染色: 3, 观察: 0 };

// ------------------------- 持久化 -------------------------

const seed = {
  samples: [
    {
      id: "CORE-001",
      project: "东岭铜矿薄片",
      borehole: "ZK-17",
      coreBox: "BX-09",
      depth: "128.4-128.8m",
      owner: "陆川",
      status: "制片中",
      delivery: "未交付",
      slices: [
        {
          id: "SL-001-A",
          method: "茜素红染色",
          observation: "",
          status: "研磨",
          logs: [
            { at: "2026-06-12T10:00:00.000Z", step: "取样", note: "截取含矿化条带位置" },
            { at: "2026-06-13T11:20:00.000Z", step: "切割", note: "完成粗切" }
          ]
        }
      ]
    }
  ],
  // 批次级资源台账
  people: [
    { id: "P01", name: "陆川", titles: ["取样", "切割", "研磨", "染色", "观察"], validUntil: "2027-12-31" },
    { id: "P02", name: "赵芸", titles: ["染色", "观察"], validUntil: "2027-06-30" },
    { id: "P03", name: "孙磊", titles: ["切割", "研磨"], validUntil: "2025-12-31" } // 资质已过期（演示用）
  ],
  reagents: [
    { id: "R01", name: "茜素红染液", batchNo: "AR-2026-06", amount: 120, unit: "ml", expiresOn: "2026-12-31" },
    { id: "R02", name: "碳化硅磨料", batchNo: "GC-2026-03", amount: 80, unit: "ml", expiresOn: "2026-10-01" },
    { id: "R03", name: "切削冷却液", batchNo: "CL-2026-01", amount: 60, unit: "ml", expiresOn: "2026-03-01" } // 已过期（演示用）
  ],
  equipment: [
    { id: "E01", name: "岩芯切割机" },
    { id: "E02", name: "研磨抛光盘" },
    { id: "E03", name: "偏光显微镜" }
  ],
  batches: [],
  bookings: [], // 设备时段占用 { id, batchId, sliceId, equipmentId, startAt, endAt, step }
  recipes: [
    { method: "茜素红染色", amounts: { 染色: 5 } } // 配方覆盖：该染色法染色工序耗 5ml
  ]
};

async function loadDb() {
  if (!existsSync(dbPath)) {
    await mkdir(dirname(dbPath), { recursive: true });
    await writeFile(dbPath, JSON.stringify(seed, null, 2));
    return structuredClone(seed);
  }
  const db = JSON.parse(await readFile(dbPath, "utf8"));
  // 老数据迁移：补齐批次域字段，不覆盖已有内容
  let migrated = false;
  for (const key of ["people", "reagents", "equipment", "batches", "bookings", "recipes"]) {
    if (!Array.isArray(db[key])) { db[key] = structuredClone(seed[key]); migrated = true; }
  }
  if (migrated) await writeFile(dbPath, JSON.stringify(db, null, 2));
  return db;
}

// 原子落盘：写临时文件再 rename，保证重启不丢数据
async function saveDb(db) {
  await mkdir(dirname(dbPath), { recursive: true });
  const tmp = `${dbPath}.${process.pid}.tmp`;
  await writeFile(tmp, JSON.stringify(db, null, 2));
  await rename(tmp, dbPath);
}

// 全局写互斥：每次推进 / 交付都是一个串行事务，杜绝并发抢设备、抢试剂
let chain = Promise.resolve();
function tx(fn) {
  const run = chain.then(fn, fn);
  chain = run.then(() => {}, () => {});
  return run;
}

// ------------------------- 领域规则 -------------------------

class DomainError extends Error {
  constructor(reasons) {
    super(Array.isArray(reasons) ? reasons.join("；") : reasons);
    this.reasons = Array.isArray(reasons) ? reasons : [reasons];
  }
}

function todayISO(now = new Date()) {
  return now.toISOString().slice(0, 10); // UTC 日期，测试中显式传 expiresOn，判断口径一致即可
}

// 按方法查配方，返回每道工序的试剂用量
function recipeFor(db, method) {
  const amounts = { ...defaultRecipe };
  const override = db.recipes.find(r => r.method === method);
  if (override) Object.assign(amounts, override.amounts || {});
  for (const step of taskSteps) if (typeof amounts[step] !== "number") amounts[step] = 0;
  return amounts;
}

function findBatch(db, batchId) {
  const batch = db.batches.find(b => b.id === batchId);
  if (!batch) throw new DomainError("批次不存在");
  return batch;
}

function updateBatchStatus(batch) {
  const pending = batch.slices.map(s => s.status);
  if (batch.delivery === "已交付") { batch.status = "已交付"; return; }
  // 所有切片都到达"观察"（观察合格后的终态）才待观察交付
  if (pending.length && pending.every(step => step === "观察")) batch.status = "待观察";
  else batch.status = "制片中";
}

function personQualified(person, step, now) {
  if (!person) return false;
  if (!person.titles.includes(step)) return false;
  if (person.validUntil && person.validUntil < todayISO(now)) return false;
  return true;
}

function reagentUsable(reagent, need, now) {
  if (!reagent) return false;
  if (reagent.expiresOn && reagent.expiresOn < todayISO(now)) return false;
  if (reagent.amount + 1e-9 < need) return false;
  return true;
}

function overlap(aStart, aEnd, bStart, bEnd) {
  return aStart < bEnd && bStart < aEnd;
}

function equipmentConflict(db, equipmentId, startAt, endAt, ignoreBookingId) {
  return db.bookings.find(bk =>
    bk.equipmentId === equipmentId &&
    bk.id !== ignoreBookingId &&
    overlap(startAt, endAt, bk.startAt, bk.endAt)
  );
}

function nextIso(base, minutes) {
  return new Date(new Date(base).getTime() + minutes * 60000).toISOString();
}

/**
 * 切片推进（整次事务）：切片状态 = 下一道待执行工序
 * （取样 → 切割 → 研磨 → 染色 → 待观察 → 观察[合格终态]）。
 * 一次推进 = 执行当前待执行工序，绑定该工序合格人员、试剂批次（按配方）、设备时段；
 * 执行"观察"时同时给出结论：不合格退回指定工序（该工序变为新的待执行工序），复检通过前不得交付。
 * 任一约束不满足即抛 DomainError —— 切片状态、操作记录、试剂余量、设备占用一律不变。
 */
function advanceSlice(db, batchId, sliceId, input, now = new Date()) {
  const batch = findBatch(db, batchId);
  if (batch.delivery === "已交付") throw new DomainError("批次已交付锁定，禁止任何操作");

  const slice = batch.slices.find(s => s.id === sliceId);
  if (!slice) throw new DomainError("切片不存在");
  if (slice.status === "观察") throw new DomainError("切片已观察合格，等待批次交付");

  // 本次推进执行的工序
  const targetStep = slice.status === "待观察" ? "观察" : slice.status;
  if (!taskSteps.includes(targetStep)) throw new DomainError("切片工序状态异常");
  const isObservation = targetStep === "观察";

  const person = db.people.find(p => p.id === input.personId);
  const equipment = db.equipment.find(e => e.id === input.equipmentId);
  const reagent = input.reagentId ? db.reagents.find(r => r.id === input.reagentId) : null;

  const reasons = [];

  // 观察结论：不合格必须指定退回工序
  let pass = true;
  let returnTo = null;
  if (isObservation) {
    pass = input.pass !== false && input.pass !== "false";
    if (!pass) {
      returnTo = input.returnTo || "";
      if (!reworkSteps.includes(returnTo)) {
        reasons.push(`观察不合格需指定退回工序（${reworkSteps.join("、")}）`);
      }
    }
  }

  // 人员资质（含有效期）
  if (!person) reasons.push("人员不存在");
  else if (!person.titles || !person.titles.includes(targetStep)) {
    reasons.push(`人员 ${person.name} 无${targetStep}工序资质`);
  } else if (person.validUntil && person.validUntil < todayISO(now)) {
    reasons.push(`人员 ${person.name} 资质已过期（有效期至 ${person.validUntil}）`);
  }

  // 设备时段
  let startAt, endAt;
  if (!input.startAt || !input.endAt) {
    reasons.push("必须提供设备占用时段 startAt / endAt");
  } else {
    startAt = new Date(input.startAt).toISOString();
    endAt = new Date(input.endAt).toISOString();
    if (!(startAt < endAt)) reasons.push("设备时段结束时间必须晚于开始时间");
  }
  if (!equipment) reasons.push("设备不存在");
  if (startAt && equipment) {
    const clash = equipmentConflict(db, equipment.id, startAt, endAt);
    if (clash) {
      reasons.push(`设备 ${equipment.name} 时段冲突，已被切片 ${clash.sliceId} 占用（${clash.startAt} ~ ${clash.endAt}）`);
    }
  }

  // 试剂（按配方扣减；同批共享同一试剂批次余量）
  const recipe = recipeFor(db, slice.method);
  const need = recipe[targetStep] || 0;
  if (need > 0) {
    if (!input.reagentId) reasons.push(`${targetStep}工序需要试剂，但未提供试剂批次`);
    else if (!reagent) reasons.push("试剂批次不存在");
    else {
      if (reagent.expiresOn && reagent.expiresOn < todayISO(now)) {
        reasons.push(`试剂 ${reagent.name}（批次 ${reagent.batchNo}）已过期（${reagent.expiresOn}）`);
      }
      if (reagent.amount + 1e-9 < need) {
        reasons.push(`试剂 ${reagent.name} 余量不足：需 ${need}${reagent.unit}，余 ${reagent.amount}${reagent.unit}`);
      }
    }
  }

  if (reasons.length) throw new DomainError(reasons); // 整次失败，什么都不改

  // ---- 全部约束通过：提交 ----
  const at = now.toISOString();

  if (need > 0) {
    reagent.amount = Math.round((reagent.amount - need) * 1000) / 1000;
    batch.reagentUsage.push({
      at, sliceId, step: targetStep, reagentId: reagent.id,
      reagentName: reagent.name, batchNo: reagent.batchNo, amount: need, unit: reagent.unit
    });
  }

  const booking = {
    id: `BK-${Date.now()}-${Math.floor(now.getMilliseconds())}-${sliceId}`.replace(/[^A-Za-z0-9_-]/g, ""),
    batchId, sliceId, equipmentId: equipment.id,
    equipmentName: equipment.name, startAt, endAt,
    step: isObservation ? "观察" : targetStep
  };
  db.bookings.push(booking);

  const op = {
    at, sliceId, step: isObservation ? "观察" : targetStep,
    personId: person.id, personName: person.name,
    equipmentId: equipment.id, startAt, endAt,
    reagentId: need > 0 ? reagent.id : null, reagentAmount: need,
    pass: isObservation ? pass : null,
    returnTo: isObservation && !pass ? returnTo : null,
    note: input.note || ""
  };

  if (isObservation) {
    slice.observation = input.note || slice.observation;
    if (pass) {
      slice.status = "观察";
      slice.passed = true; // 观察合格（复检合格同样置真）
      slice.logs.push({ at, step: "观察", note: `观察合格：${input.note || "通过"}`, by: person.name });
    } else {
      slice.passed = false; // 未复检合格前不得交付
      slice.status = returnTo;
      slice.logs.push({ at, step: "观察", note: `观察不合格，退回${returnTo}：${op.note || ""}`, by: person.name });
    }
  } else {
    const idx = taskSteps.indexOf(targetStep);
    // 染色执行完进入"待观察"；其余工序执行完流转到下一待执行工序
    slice.status = idx === taskSteps.length - 2 ? "待观察" : taskSteps[idx + 1];
    if (slice.passed === true) slice.passed = false; // 退回后重新推进，旧合格结论作废
    slice.logs.push({ at, step: targetStep, note: input.note || "工序完成", by: person.name });
  }
  batch.operations.push(op);
  updateBatchStatus(batch);
  return { batch, operation: op, booking, consumed: need > 0 ? { reagentId: reagent.id, amount: need, remain: reagent.amount } : null };
}

/**
 * 交付前核对：完成度 / 试剂用量 / 设备释放。
 * 异常则拒绝并说明全部阻断原因；通过后锁定批次。
 */
function deliverBatch(db, batchId, now = new Date()) {
  const batch = findBatch(db, batchId);
  if (batch.delivery === "已交付") throw new DomainError("批次已交付锁定，不能重复交付");

  const reasons = [];

  // 1) 完成度：所有切片观察合格（退回后已复检）
  if (!batch.slices.length) reasons.push("批次下没有切片，无法交付");
  for (const slice of batch.slices) {
    if (slice.status === "待观察") reasons.push(`切片 ${slice.id} 已制片完成，尚未提交观察结论`);
    else if (slice.status !== "观察") {
      reasons.push(`切片 ${slice.id} 停留在「${slice.status}」工序，未完成制片${slice.passed === false ? "，且观察不合格尚未复检通过" : ""}`);
    } else if (slice.passed !== true) reasons.push(`切片 ${slice.id} 观察不合格且未复检通过，不得交付`);
  }

  // 2) 试剂用量：操作记录扣减汇总必须与批次台账一致
  const usage = new Map();
  for (const u of batch.reagentUsage) {
    const key = u.reagentId;
    const cur = usage.get(key) || { reagentId: u.reagentId, reagentName: u.reagentName, batchNo: u.batchNo, amount: 0, unit: u.unit };
    cur.amount = Math.round((cur.amount + u.amount) * 1000) / 1000;
    usage.set(key, cur);
  }
  const logged = batch.operations.filter(o => o.reagentId).map(o => ({ reagentId: o.reagentId, amount: o.reagentAmount }));
  const loggedSum = new Map();
  for (const l of logged) loggedSum.set(l.reagentId, Math.round(((loggedSum.get(l.reagentId) || 0) + l.amount) * 1000) / 1000);
  for (const [key, sum] of loggedSum) {
    if (!usage.has(key) || Math.abs(usage.get(key).amount - sum) > 1e-6) {
      reasons.push(`试剂 ${key} 用量台账与操作记录不一致`);
    }
  }

  // 3) 设备释放：本批所有设备时段必须已结束（无进行中/未来占用）
  const nowIso = now.toISOString();
  const active = db.bookings.filter(bk => bk.batchId === batchId && bk.endAt > nowIso);
  for (const bk of active) reasons.push(`设备 ${bk.equipmentName} 尚未释放（占用至 ${bk.endAt}）`);

  if (reasons.length) throw new DomainError(reasons);

  batch.delivery = "已交付";
  batch.deliveredAt = nowIso;
  batch.locked = true;
  updateBatchStatus(batch);
  return { batch, reagentUsage: [...usage.values()], releasedBookings: db.bookings.filter(bk => bk.batchId === batchId).length };
}

// ------------------------- HTTP 辅助 -------------------------

async function body(req) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  return chunks.length ? JSON.parse(Buffer.concat(chunks).toString("utf8")) : {};
}
function sendJson(res, status, data) {
  res.writeHead(status, { "Content-Type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(data, null, 2));
}
function fail(res, error, status = 400) {
  const blocked = error instanceof DomainError;
  return sendJson(res, blocked ? 409 : status, {
    error: blocked ? "constraint_blocked" : "bad_request",
    reasons: blocked ? error.reasons : [error.message]
  });
}
function updateSampleStatus(sample) {
  const sliceStatuses = sample.slices.map(slice => slice.status);
  if (sample.delivery === "已交付") sample.status = "已交付";
  else if (sliceStatuses.length && sliceStatuses.every(step => step === "观察")) sample.status = "待观察";
  else if (sliceStatuses.some(step => ["取样", "切割", "研磨", "染色"].includes(step))) sample.status = "制片中";
  else sample.status = "待切割";
}

// ------------------------- 页面 -------------------------

const page = `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>岩芯样本切片实验室</title>
  <style>
    :root { --bg:#f1f3ef; --panel:#fff; --ink:#242822; --muted:#687062; --line:#d7ddd1; --accent:#526f43; --stone:#73706a; }
    * { box-sizing:border-box; } body { margin:0; background:var(--bg); color:var(--ink); font-family:Arial,"PingFang SC",sans-serif; }
    header { padding:22px 28px; background:#fff; border-bottom:1px solid var(--line); display:flex; justify-content:space-between; align-items:center; gap:16px; }
    h1 { margin:0; font-size:26px; } main { display:grid; grid-template-columns:390px 1fr; gap:22px; padding:22px 28px; }
    form,.panel,.card,.stat { background:#fff; border:1px solid var(--line); border-radius:8px; padding:16px; } h2 { margin:0 0 12px; font-size:18px; }
    label { display:block; margin:10px 0 5px; color:var(--muted); font-size:13px; } input,select,textarea { width:100%; border:1px solid var(--line); border-radius:6px; padding:9px; font:inherit; background:#fff; } textarea { min-height:68px; }
    button { border:0; border-radius:6px; background:var(--accent); color:#fff; padding:10px 13px; font-weight:700; cursor:pointer; }
    .stats { display:grid; grid-template-columns:repeat(4,1fr); gap:10px; margin-bottom:14px; } .stat strong { display:block; font-size:24px; }
    .grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(310px,1fr)); gap:12px; } .card { display:grid; gap:8px; }
    .meta { color:var(--muted); font-size:13px; } .pill { display:inline-block; border:1px solid var(--line); border-radius:999px; padding:3px 8px; font-size:12px; }
    .slice { border-top:1px solid var(--line); padding-top:10px; } .toolbar { display:flex; gap:10px; flex-wrap:wrap; margin-bottom:14px; }
    a.nav { color:var(--accent); font-weight:700; text-decoration:none; }
    @media (max-width:950px){ header{display:block;padding:18px 16px;} main{grid-template-columns:1fr;padding:16px;} .stats{grid-template-columns:1fr 1fr;} }
  </style>
</head>
<body>
  <header><div><h1>岩芯样本切片实验室</h1><div class="meta">样本、切片任务、制片步骤和交付（旧入口）｜<a class="nav" href="/station">进入切片台（批次资源约束）→</a></div></div><button id="reload">刷新</button></header>
  <main>
    <form id="form">
      <h2>创建岩芯样本</h2>
      <label>项目</label><input name="project" required>
      <label>钻孔编号</label><input name="borehole" required>
      <label>岩芯箱号</label><input name="coreBox" required>
      <label>取样深度</label><input name="depth" required>
      <label>负责人</label><input name="owner" required>
      <label>初始切片编号</label><input name="sliceId" required>
      <label>染色方法</label><input name="method" required>
      <button>保存样本</button>
    </form>
    <section>
      <div class="stats" id="stats"></div>
      <div class="grid" id="samples"></div>
    </section>
  </main>
  <script>
    const statuses = ${JSON.stringify(statuses)};
    const steps = ${JSON.stringify(taskSteps)};
    const form = document.querySelector("#form");
    const stats = document.querySelector("#stats");
    const samplesEl = document.querySelector("#samples");
    let samples = [];
    async function api(path, options) {
      const res = await fetch(path, options && options.body ? { ...options, headers:{ "Content-Type":"application/json" } } : options);
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "请求失败");
      return data;
    }
    function render() {
      stats.innerHTML = statuses.map(s => '<div class="stat"><span>'+s+'</span><strong>'+samples.filter(item => item.status === s).length+'</strong></div>').join("");
      samplesEl.innerHTML = samples.map(sample => '<article class="card"><h3>'+sample.project+'</h3><span class="pill">'+sample.status+'</span><div class="meta">'+sample.borehole+' · '+sample.coreBox+' · '+sample.depth+' · '+sample.owner+'</div><label>新增切片</label><input data-new-slice="'+sample.id+'" placeholder="切片编号"><input data-method="'+sample.id+'" placeholder="染色方法"><button data-add="'+sample.id+'">添加切片</button>'+sample.slices.map(slice => '<div class="slice"><b>'+slice.id+'</b><div class="meta">'+slice.method+' · 当前步骤 '+slice.status+'</div><select data-step="'+sample.id+'|'+slice.id+'">'+steps.map(step => '<option>'+step+'</option>').join("")+'</select><textarea data-note="'+sample.id+'|'+slice.id+'" placeholder="步骤备注或观察结果"></textarea><button data-log="'+sample.id+'|'+slice.id+'">记录步骤</button><div class="meta">'+slice.logs.map(log => log.step+"："+log.note).join(" / ")+'</div></div>').join("")+'<button data-deliver="'+sample.id+'">标记交付</button></article>').join("");
      document.querySelectorAll("[data-step]").forEach(sel => {
        const [sampleId, sliceId] = sel.dataset.step.split("|");
        const slice = samples.find(s => s.id === sampleId).slices.find(s => s.id === sliceId);
        sel.value = slice.status;
      });
      document.querySelectorAll("[data-add]").forEach(btn => btn.onclick = async () => {
        const id = btn.dataset.add;
        await api('/api/samples/'+id+'/slices', { method:'POST', body: JSON.stringify({ id: document.querySelector('[data-new-slice="'+id+'"]').value, method: document.querySelector('[data-method="'+id+'"]').value || "未指定" }) });
        await load();
      });
      document.querySelectorAll("[data-log]").forEach(btn => btn.onclick = async () => {
        const [sampleId, sliceId] = btn.dataset.log.split("|");
        await api('/api/samples/'+sampleId+'/slices/'+sliceId+'/logs', { method:'POST', body: JSON.stringify({ step: document.querySelector('[data-step="'+sampleId+'|'+sliceId+'"]').value, note: document.querySelector('[data-note="'+sampleId+'|'+sliceId+'"]').value || "步骤完成" }) });
        await load();
      });
      document.querySelectorAll("[data-deliver]").forEach(btn => btn.onclick = async () => { await api('/api/samples/'+btn.dataset.deliver+'/deliver', { method:'POST', body: JSON.stringify({}) }); await load(); });
    }
    async function load(){ samples = await api("/api/samples"); render(); }
    document.querySelector("#reload").onclick = load;
    form.onsubmit = async event => {
      event.preventDefault();
      await api("/api/samples", { method:"POST", body: JSON.stringify(Object.fromEntries(new FormData(form).entries())) });
      form.reset(); await load();
    };
    load();
  </script>
</body>
</html>`;

const stationPage = `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>岩芯切片台 · 批次作业</title>
  <style>
    :root { --bg:#eef1ea; --panel:#fff; --ink:#242822; --muted:#687062; --line:#d7ddd1; --accent:#526f43; --warn:#b5473a; --ok:#3f7a4e; }
    * { box-sizing:border-box; } body { margin:0; background:var(--bg); color:var(--ink); font-family:Arial,"PingFang SC",sans-serif; }
    header { padding:20px 26px; background:#fff; border-bottom:1px solid var(--line); display:flex; justify-content:space-between; align-items:center; gap:14px; }
    h1 { margin:0; font-size:23px; } h2 { margin:0 0 10px; font-size:16px; }
    main { padding:20px 26px; display:grid; grid-template-columns:360px 1fr; gap:18px; align-items:start; }
    .panel,.card { background:#fff; border:1px solid var(--line); border-radius:8px; padding:14px; margin-bottom:14px; }
    label { display:block; margin:8px 0 4px; color:var(--muted); font-size:12px; }
    input,select,textarea { width:100%; border:1px solid var(--line); border-radius:6px; padding:8px; font:inherit; background:#fff; }
    button { border:0; border-radius:6px; background:var(--accent); color:#fff; padding:8px 12px; font-weight:700; cursor:pointer; }
    button.ghost { background:#fff; color:var(--accent); border:1px solid var(--accent); }
    .meta { color:var(--muted); font-size:12px; } .pill { display:inline-block; border:1px solid var(--line); border-radius:999px; padding:2px 8px; font-size:12px; margin-right:4px; }
    .pill.lock { background:#efe7e5; color:var(--warn); border-color:#e2c3bd; } .pill.ok { background:#e9f2ec; color:var(--ok); border-color:#c5ddcb; }
    table { width:100%; border-collapse:collapse; font-size:12px; } th,td { text-align:left; border-bottom:1px solid var(--line); padding:5px 6px; }
    .slice { border-top:1px dashed var(--line); padding:10px 0; } .slice:first-child { border-top:0; }
    .err { background:#fbeae7; border:1px solid #e2c3bd; color:var(--warn); border-radius:6px; padding:9px 11px; margin-bottom:12px; font-size:13px; white-space:pre-line; }
    .okmsg { background:#e9f2ec; border:1px solid #c5ddcb; color:var(--ok); border-radius:6px; padding:9px 11px; margin-bottom:12px; font-size:13px; }
    .row2 { display:grid; grid-template-columns:1fr 1fr; gap:8px; }
    a.nav { color:var(--accent); font-weight:700; text-decoration:none; }
    .red { color:var(--warn); font-weight:700; } .green { color:var(--ok); font-weight:700; }
    @media (max-width:980px){ main{grid-template-columns:1fr;} }
  </style>
</head>
<body>
  <header>
    <div><h1>岩芯切片台 · 批次作业</h1><div class="meta">批次建档 · 人员资质 / 试剂批次 / 设备时段约束 · 观察复检 · 交付核对锁定　｜　<a class="nav" href="/">← 旧入口</a></div></div>
    <button id="reload">刷新</button>
  </header>
  <main>
    <div>
      <div class="panel">
        <h2>批次建档</h2>
        <form id="batchForm">
          <label>项目</label><input name="project" required>
          <div class="row2"><div><label>钻孔编号</label><input name="borehole" required></div><div><label>岩芯箱号</label><input name="coreBox" required></div></div>
          <label>负责人</label><input name="owner" required>
          <label>染色方法（决定配方）</label><select name="method" id="methodSelect"></select>
          <label>初始切片编号（多个用逗号分隔）</label><input name="sliceIds" value="SL-1" required>
          <div style="margin-top:10px"><button>建立批次</button></div>
        </form>
      </div>
      <div class="panel">
        <h2>人员资质</h2><table id="peopleTbl"></table>
      </div>
      <div class="panel">
        <h2>试剂批次（同批共享余量）</h2><table id="reagentTbl"></table>
      </div>
      <div class="panel">
        <h2>设备与时段占用</h2><table id="equipTbl"></table>
      </div>
    </div>
    <div>
      <div id="msg"></div>
      <div id="batches"></div>
    </div>
  </main>
  <script>
    const steps = ${JSON.stringify(taskSteps)};
    const reworkSteps = ${JSON.stringify(reworkSteps)};
    let state = { batches:[], people:[], reagents:[], equipment:[], bookings:[], recipes:[] };
    async function api(path, options) {
      const res = await fetch(path, options && options.body ? { ...options, headers:{ "Content-Type":"application/json" } } : options);
      const data = await res.json();
      if (!res.ok) { const e = new Error((data.reasons||[data.error]).join("；")); e.reasons = data.reasons; throw e; }
      return data;
    }
    const $ = id => document.getElementById(id);
    function esc(s){ return String(s==null?"":s).replace(/[&<>"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c])); }
    function today(){ const d=new Date(Date.now()+60000); return d.toISOString().slice(0,16); }
    function showError(e){ $("msg").innerHTML = '<div class="err"><b>操作被阻断：</b>\\n'+esc(e.message||e);</div>'; }
    function showOk(t){ $("msg").innerHTML = '<div class="okmsg">'+esc(t)+'</div>'; }

    function renderResources() {
      $("methodSelect").innerHTML = state.recipes.map(r=>'<option>'+esc(r.method)+'</option>').join("") + '<option>常规制片</option>';
      const t = new Date().toISOString().slice(0,10);
      $("peopleTbl").innerHTML = '<tr><th>姓名</th><th>可操作工序</th><th>有效期</th></tr>' + state.people.map(p =>
        '<tr><td>'+esc(p.name)+'</td><td>'+esc(p.titles.join("、"))+'</td><td class="'+(p.validUntil<t?"red":"")+'">'+esc(p.validUntil)+'</td></tr>').join("");
      $("reagentTbl").innerHTML = '<tr><th>试剂/批号</th><th>余量</th><th>有效期</th></tr>' + state.reagents.map(r =>
        '<tr><td>'+esc(r.name)+'<div class="meta">'+esc(r.batchNo)+'</div></td><td class="'+(r.amount<=0?"red":"")+'">'+r.amount+esc(r.unit)+'</td><td class="'+(r.expiresOn<t?"red":"green")+'">'+esc(r.expiresOn)+'</td></tr>').join("");
      const nowIso = new Date().toISOString();
      $("equipTbl").innerHTML = '<tr><th>设备</th><th>占用情况</th></tr>' + state.equipment.map(eq => {
        const bks = state.bookings.filter(b=>b.equipmentId===eq.id);
        const active = bks.filter(b=>b.endAt>nowIso);
        return '<tr><td>'+esc(eq.name)+'</td><td>'+(active.length?'<span class="red">'+active.length+' 个时段未释放</span>':'<span class="green">空闲（历史占用 '+bks.length+'）</span>')+'</td></tr>';
      }).join("");
    }

    function nextStatus(status) {
      if (status === "待观察") return "观察";
      const i = steps.indexOf(status);
      return i === steps.length - 2 ? "待观察" : steps[i + 1];
    }
    function sliceCard(batch, sl) {
      const done = sl.status === "观察";
      const enteringObs = sl.status === "待观察";
      if (done) {
        return '<div class="slice"><b>'+esc(sl.id)+'</b> <span class="pill">观察合格</span><span class="pill ok">待批次交付</span>'
          +'<div class="meta">方法 '+esc(sl.method)+'　观察记录：'+esc(sl.observation||"")+'</div>'
          +'<div class="meta" style="margin-top:6px">'+sl.logs.slice(-3).map(l=>esc(l.step)+"："+esc(l.note)+(l.by?"（"+esc(l.by)+"）":"")).join("<br>")+'</div></div>';
      }
      const peopleOpts = state.people.map(p=>'<option value="'+p.id+'">'+esc(p.name)+(p.validUntil<new Date().toISOString().slice(0,10)?"（资质过期）":"")+'</option>').join("");
      const reagentOpts = '<option value="">（本工序不需试剂）</option>' + state.reagents.map(r=>'<option value="'+r.id+'">'+esc(r.name)+' '+esc(r.batchNo)+' · 余'+r.amount+esc(r.unit)+'</option>').join("");
      const equipOpts = state.equipment.map(e=>'<option value="'+e.id+'">'+esc(e.name)+'</option>').join("");
      const ts = today();
      const execStep = enteringObs ? "观察" : sl.status;
      return '<div class="slice"><b>'+esc(sl.id)+'</b> <span class="pill">待执行：'+esc(execStep)+'</span>'+(sl.passed===false?'<span class="pill lock">观察不合格·复检中</span>':'')
        +'<div class="meta">方法 '+esc(sl.method)+'　执行后流转：'+esc(nextStatus(sl.status))+'</div>'
        +'<div class="row2"><div><label>合格人员</label><select data-k="personId">'+peopleOpts+'</select></div>'
        +'<div><label>试剂批次</label><select data-k="reagentId">'+reagentOpts+'</select></div></div>'
        +'<div class="row2"><div><label>设备</label><select data-k="equipmentId">'+equipOpts+'</select></div>'
        +(enteringObs?'<div><label>观察结论</label><select data-k="pass"><option value="true">合格</option><option value="false">不合格→退回</option></select></div>':'<div><label>&nbsp;</label><input value="执行'+esc(execStep)+'工序" disabled></div>')+'</div>'
        +'<div class="row2"><div><label>时段开始</label><input type="datetime-local" data-k="startAt" value="'+ts+'"></div>'
        +'<div><label>时段结束</label><input type="datetime-local" data-k="endAt" value="'+new Date(Date.now()+1800000).toISOString().slice(0,16)+'"></div></div>'
        +(enteringObs?'<label>不合格时退回工序</label><select data-k="returnTo">'+reworkSteps.map(s=>'<option>'+s+'</option>').join("")+'</select>':'')
        +'<label>备注 / 观察结果</label><input data-k="note" placeholder="'+(enteringObs?"观察描述":"工序备注")+'">'
        +'<div style="margin-top:8px"><button data-advance="'+batch.id+'|'+esc(sl.id)+'">'+(enteringObs?"提交观察结论":"执行"+esc(execStep))+'</button></div>'
        +'<div class="meta" style="margin-top:6px">'+sl.logs.slice(-3).map(l=>esc(l.step)+"："+esc(l.note)+(l.by?"（"+esc(l.by)+"）":"")).join("<br>")+'</div></div>';
    }

    function renderBatches() {
      $("batches").innerHTML = state.batches.map(b => {
        const locked = b.delivery === "已交付";
        const usage = b.reagentUsage.map(u=>esc(u.reagentName)+" "+u.amount+esc(u.unit)).join("、") || "无";
        return '<div class="card"><h2>'+esc(b.id)+' · '+esc(b.project)+' <span class="pill">'+esc(b.status)+'</span>'
          +(locked?'<span class="pill lock">已交付锁定 '+esc(b.deliveredAt||"")+'</span>':'<span class="pill">未交付</span>')+'</h2>'
          +'<div class="meta">'+esc(b.borehole)+' · '+esc(b.coreBox)+' · 负责人 '+esc(b.owner)+'　｜　累计试剂：'+usage+'</div>'
          + b.slices.map(sl=>sliceCard(b,sl)).join("")
          +(locked?'':'<div style="margin-top:10px"><button class="ghost" data-deliver="'+b.id+'">交付前核对并交付</button></div>')+'</div>';
      }).join("") || '<div class="panel meta">还没有批次，先在左侧建档。</div>';

      document.querySelectorAll("[data-advance]").forEach(btn => btn.onclick = async () => {
        const [batchId, sliceId] = btn.dataset.advance.split("|");
        const root = btn.closest(".slice");
        const payload = { note: "" };
        root.querySelectorAll("[data-k]").forEach(el => {
          let v = el.value;
          if (el.type === "datetime-local") v = v ? new Date(v).toISOString() : v;
          payload[el.dataset.k] = v;
        });
        try { await api('/api/station/batches/'+batchId+'/slices/'+sliceId+'/advance', { method:'POST', body: JSON.stringify(payload) }); showOk("推进成功"); await load(); }
        catch(e){ showError(e); }
      });
      document.querySelectorAll("[data-deliver]").forEach(btn => btn.onclick = async () => {
        try { const r = await api('/api/station/batches/'+btn.dataset.deliver+'/deliver', { method:'POST', body:'{}' });
          showOk("交付成功，批次已锁定。试剂用量："+r.reagentUsage.map(u=>u.reagentName+" "+u.amount+u.unit).join("、")+"；设备时段均已释放。"); await load();
        } catch(e){ showError(e); }
      });
    }

    async function load() {
      state = await api("/api/station/state");
      renderResources(); renderBatches();
    }
    $("reload").onclick = load;
    $("batchForm").onsubmit = async e => {
      e.preventDefault();
      const fd = Object.fromEntries(new FormData($("batchForm")).entries());
      try {
        await api("/api/station/batches", { method:"POST", body: JSON.stringify({ ...fd, sliceIds: String(fd.sliceIds).split(",").map(s=>s.trim()).filter(Boolean) }) });
        $("batchForm").reset(); showOk("批次已建立"); await load();
      } catch(err){ showError(err); }
    };
    load();
  </script>
</body>
</html>`;

// ------------------------- 路由 -------------------------

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, `http://${req.headers.host}`);

    if (req.method === "GET" && url.pathname === "/") {
      res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
      return res.end(page);
    }
    if (req.method === "GET" && url.pathname === "/station") {
      res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
      return res.end(stationPage);
    }

    // ---------- 切片台：批次域 API（全部写操作串行事务） ----------
    if (req.method === "GET" && url.pathname === "/api/station/state") {
      const db = await tx(() => loadDb());
      return sendJson(res, 200, {
        batches: db.batches, people: db.people, reagents: db.reagents,
        equipment: db.equipment, bookings: db.bookings, recipes: db.recipes,
        steps: taskSteps
      });
    }

    if (req.method === "POST" && url.pathname === "/api/station/batches") {
      const input = await body(req);
      const result = await tx(async () => {
        const db = await loadDb();
        const missing = ["project", "borehole", "coreBox", "owner", "method"].filter(k => !input[k]);
        const ids = Array.isArray(input.sliceIds) ? input.sliceIds.map(String) : [];
        if (missing.length) throw new DomainError(`缺少字段：${missing.join("、")}`);
        if (!ids.length) throw new DomainError("至少需要一个初始切片编号");
        const dup = ids.filter((id, i) => ids.indexOf(id) !== i);
        if (dup.length) throw new DomainError(`切片编号重复：${[...new Set(dup)].join("、")}`);
        const batch = {
          id: input.id || `BATCH-${Date.now()}`,
          project: input.project, borehole: input.borehole, coreBox: input.coreBox,
          depth: input.depth || "", owner: input.owner, method: input.method,
          status: "制片中", delivery: "未交付", locked: false,
          slices: ids.map(id => ({ id, method: input.method, observation: "", status: "取样", passed: null, logs: [] })),
          operations: [], reagentUsage: [], createdAt: new Date().toISOString()
        };
        db.batches.unshift(batch);
        await saveDb(db);
        return batch;
      });
      return sendJson(res, 201, result);
    }

    // 资源登记/调整：人员资质、试剂批次（含补库）、设备、配方（供建档与测试造数据）
    if (req.method === "POST" && url.pathname === "/api/station/people") {
      const input = await body(req);
      try {
        const result = await tx(async () => {
          const db = await loadDb();
          if (!input.id || !input.name || !Array.isArray(input.titles) || !input.titles.length) {
            throw new DomainError("人员需要 id、name、titles（资质工序列表）");
          }
          const p = { id: String(input.id), name: input.name, titles: input.titles, validUntil: input.validUntil || "2099-12-31" };
          db.people = db.people.filter(x => x.id !== p.id);
          db.people.push(p);
          await saveDb(db);
          return p;
        });
        return sendJson(res, 201, result);
      } catch (error) { return fail(res, error); }
    }
    if (req.method === "POST" && url.pathname === "/api/station/reagents") {
      const input = await body(req);
      try {
        const result = await tx(async () => {
          const db = await loadDb();
          if (!input.id) throw new DomainError("试剂需要 id");
          const existing = db.reagents.find(x => x.id === input.id);
          if (input.adjust) { // 补库/盘点：试剂必须已存在
            if (!existing) throw new DomainError(`试剂 ${input.id} 不存在，无法补库`);
            existing.amount = Math.round((existing.amount + Number(input.amount || 0)) * 1000) / 1000;
            if (input.batchNo) existing.batchNo = input.batchNo;
            if (input.expiresOn) existing.expiresOn = input.expiresOn;
            await saveDb(db);
            return existing;
          }
          if (!input.name) throw new DomainError("试剂需要 name");
          const r = {
            id: String(input.id), name: input.name,
            batchNo: input.batchNo || `LOT-${input.id}`,
            amount: Number(input.amount ?? 0), unit: input.unit || "ml",
            expiresOn: input.expiresOn || "2099-12-31"
          };
          db.reagents = db.reagents.filter(x => x.id !== r.id);
          db.reagents.push(r);
          await saveDb(db);
          return r;
        });
        return sendJson(res, 201, result);
      } catch (error) { return fail(res, error); }
    }
    if (req.method === "POST" && url.pathname === "/api/station/equipment") {
      const input = await body(req);
      try {
        const result = await tx(async () => {
          const db = await loadDb();
          if (!input.id || !input.name) throw new DomainError("设备需要 id、name");
          const e = { id: String(input.id), name: input.name };
          db.equipment = db.equipment.filter(x => x.id !== e.id);
          db.equipment.push(e);
          await saveDb(db);
          return e;
        });
        return sendJson(res, 201, result);
      } catch (error) { return fail(res, error); }
    }

    const advanceMatch = url.pathname.match(/^\/api\/station\/batches\/([^/]+)\/slices\/([^/]+)\/advance$/);
    if (advanceMatch && req.method === "POST") {
      const input = await body(req);
      try {
        const result = await tx(async () => {
          const db = await loadDb();
          const r = advanceSlice(db, advanceMatch[1], advanceMatch[2], input, new Date());
          await saveDb(db);
          return r;
        });
        return sendJson(res, 200, result);
      } catch (error) { return fail(res, error); }
    }

    const stationDeliver = url.pathname.match(/^\/api\/station\/batches\/([^/]+)\/deliver$/);
    if (stationDeliver && req.method === "POST") {
      try {
        const result = await tx(async () => {
          const db = await loadDb();
          const r = deliverBatch(db, stationDeliver[1], new Date());
          await saveDb(db);
          return r;
        });
        return sendJson(res, 200, result);
      } catch (error) { return fail(res, error); }
    }

    // ---------- 旧入口 API：行为完全保留（同样串行落盘） ----------
    const db = await tx(() => loadDb());
    if (req.method === "GET" && url.pathname === "/api/samples") return sendJson(res, 200, db.samples);
    if (req.method === "POST" && url.pathname === "/api/samples") {
      const input = await body(req);
      const sample = { id: `CORE-${Date.now()}`, project: input.project, borehole: input.borehole, coreBox: input.coreBox, depth: input.depth, owner: input.owner, status: "待切割", delivery: "未交付", slices: [{ id: input.sliceId, method: input.method, observation: "", status: "取样", logs: [{ at: new Date().toISOString(), step: "取样", note: "创建初始切片任务" }] }] };
      updateSampleStatus(sample);
      db.samples.unshift(sample);
      await saveDb(db);
      return sendJson(res, 201, sample);
    }
    const addSlice = url.pathname.match(/^\/api\/samples\/([^/]+)\/slices$/);
    if (addSlice && req.method === "POST") {
      const sample = db.samples.find(item => item.id === addSlice[1]);
      if (!sample) return sendJson(res, 404, { error: "sample_not_found" });
      const input = await body(req);
      sample.slices.push({ id: input.id, method: input.method || "未指定", observation: "", status: "取样", logs: [{ at: new Date().toISOString(), step: "取样", note: "新增切片任务" }] });
      updateSampleStatus(sample);
      await saveDb(db);
      return sendJson(res, 201, sample);
    }
    const logMatch = url.pathname.match(/^\/api\/samples\/([^/]+)\/slices\/([^/]+)\/logs$/);
    if (logMatch && req.method === "POST") {
      const sample = db.samples.find(item => item.id === logMatch[1]);
      if (!sample) return sendJson(res, 404, { error: "sample_not_found" });
      const slice = sample.slices.find(item => item.id === logMatch[2]);
      if (!slice) return sendJson(res, 404, { error: "slice_not_found" });
      const input = await body(req);
      slice.status = input.step;
      if (input.step === "观察") slice.observation = input.note || slice.observation;
      slice.logs.push({ at: new Date().toISOString(), step: input.step, note: input.note || "" });
      updateSampleStatus(sample);
      await saveDb(db);
      return sendJson(res, 200, sample);
    }
    const deliverMatch = url.pathname.match(/^\/api\/samples\/([^/]+)\/deliver$/);
    if (deliverMatch && req.method === "POST") {
      const sample = db.samples.find(item => item.id === deliverMatch[1]);
      if (!sample) return sendJson(res, 404, { error: "sample_not_found" });
      sample.delivery = "已交付";
      updateSampleStatus(sample);
      await saveDb(db);
      return sendJson(res, 200, sample);
    }
    sendJson(res, 404, { error: "not_found" });
  } catch (error) {
    sendJson(res, 500, { error: error.message });
  }
});

server.listen(port, () => console.log(`Core slice lab app listening on http://localhost:${port}`));

export { advanceSlice, deliverBatch, recipeFor, defaultRecipe, taskSteps };
