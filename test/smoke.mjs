// 启动后实测：正常交付 / 并发抢设备 / 余量不足 / 过期资质 / 失败回滚 / 交付锁定
// 另含：观察退回复检、设备未释放阻断、重启数据不丢、旧入口保留。
// 运行：node test/smoke.mjs
import { spawn } from "node:child_process";
import { readFile, rm } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const PORT = 4325;
const DB = join(root, "test", "smoke-db.json");
const base = `http://127.0.0.1:${PORT}`;

let passCount = 0;
const failures = [];
function check(name, cond, detail = "") {
  if (cond) { passCount++; console.log(`  ✓ ${name}`); }
  else { failures.push(name); console.log(`  ✗ ${name} ${detail}`); }
}
function startServer() {
  return spawn(process.execPath, [join(root, "server.js")], {
    cwd: root,
    env: { ...process.env, PORT: String(PORT), DB_PATH: DB },
    stdio: ["ignore", "pipe", "inherit"]
  });
}
async function waitReady() {
  for (let i = 0; i < 60; i++) {
    try { if ((await fetch(`${base}/api/station/state`)).ok) return; } catch {}
    await new Promise(r => setTimeout(r, 150));
  }
  throw new Error("server not ready");
}
async function stopServer(child) { child.kill("SIGTERM"); await new Promise(r => child.on("exit", r)); }

async function call(method, path, payload) {
  const res = await fetch(base + path, {
    method,
    headers: payload ? { "Content-Type": "application/json" } : undefined,
    body: payload ? JSON.stringify(payload) : undefined
  });
  const data = await res.json().catch(() => ({}));
  return { status: res.status, data };
}
const reasonOf = r => (r.data.reasons || []).join("；");
const isBlocked = (r, kw) => r.status === 409 && (kw ? reasonOf(r).includes(kw) : true);

// 工序 → 设备/试剂/历史时段（历史时段保证交付时设备已释放）
const EQUIP = { 取样: "E01", 切割: "E01", 研磨: "E02", 染色: "E02", 观察: "E03" };
const REAGENT = { 切割: "RCUT", 研磨: "RGRIND", 染色: "RDYE" };
const STEPS = ["取样", "切割", "研磨", "染色", "观察"];
function daySlot(day, mins) { // 生成互不重叠的 30 分钟时段
  const h = 8 + Math.floor(mins / 2);
  const m = mins % 2 === 0 ? "00" : "30";
  const start = `2026-08-${String(day).padStart(2, "0")}T${String(h).padStart(2, "0")}:${m}:00Z`;
  const endM = m === "00" ? "30" : "00";
  const endH = m === "00" ? h : h + 1;
  const end = `2026-08-${String(day).padStart(2, "0")}T${String(endH).padStart(2, "0")}:${endM}:00Z`;
  return [start, end];
}

async function setupResources() {
  const people = [
    ["QA", "全科甲", STEPS, "2027-12-31"],
    ["QB", "全科乙", STEPS, "2027-12-31"],
    ["QE", "过期丙", STEPS, "2025-01-01"],
    ["QO", "仅观察丁", ["观察"], "2027-12-31"]
  ];
  for (const [id, name, titles, validUntil] of people)
    await call("POST", "/api/station/people", { id, name, titles, validUntil });
  for (const [id, name] of [["E01", "岩芯切割机"], ["E02", "研磨抛光盘"], ["E03", "偏光显微镜"]])
    await call("POST", "/api/station/equipment", { id, name });
  for (const [id, name, no] of [["RCUT", "切削冷却液", "CUT"], ["RGRIND", "碳化硅磨料", "GRIND"], ["RDYE", "茜素红染液", "DYE"]])
    await call("POST", "/api/station/reagents", { id, name, batchNo: `LOT-${no}`, amount: 100, unit: "ml", expiresOn: "2027-12-31" });
}

async function createBatch(id, sliceIds) {
  return call("POST", "/api/station/batches", {
    id, project: `实测-${id}`, borehole: "ZK-T", coreBox: "BX-T",
    owner: "测试员", method: "常规制片", sliceIds
  });
}

// 执行一次"当前待执行工序"。target 仅用来选设备/试剂/时段，实际工序由服务端按状态决定。
async function execStep(batchId, sliceId, target, opts = {}) {
  const slot = opts.slot || daySlot(opts.day ?? 1, opts.slotIndex ?? 0);
  return call("POST", `/api/station/batches/${batchId}/slices/${sliceId}/advance`, {
    personId: opts.person || "QA",
    equipmentId: opts.equipmentId || EQUIP[target],
    reagentId: Object.prototype.hasOwnProperty.call(opts, "reagentId") ? opts.reagentId : (REAGENT[target] || ""),
    startAt: slot[0], endAt: slot[1],
    pass: target === "观察" ? (opts.pass !== undefined ? opts.pass : true) : undefined,
    returnTo: target === "观察" ? opts.returnTo : undefined,
    note: opts.note || (target === "观察" ? "合格" : `${target}完成`)
  });
}
// 完整跑完一片（取样→观察合格），每天一片避免设备时段冲突
async function runFull(batchId, sliceId, day = 1) {
  for (let i = 0; i < STEPS.length; i++) {
    const target = STEPS[i];
    const r = await execStep(batchId, sliceId, target, { day, slotIndex: i, note: target === "观察" ? "合格" : undefined });
    if (r.status !== 200) throw new Error(`${target} 执行失败: ${reasonOf(r)}`);
  }
}
const getState = async () => (await call("GET", "/api/station/state")).data;

// ───────── 启动 ─────────
await rm(DB, { force: true });
console.log("\n[启动服务]");
const server = startServer();
await waitReady();
console.log("  ✓ 服务已启动");

try {
  await setupResources();

  // 0) 旧入口保留
  console.log("\n[旧入口保留]");
  const oldList = await call("GET", "/api/samples");
  check("GET /api/samples 仍可用（含种子 CORE-001）", oldList.status === 200 && oldList.data.some(s => s.id === "CORE-001"));
  const oldCreate = await call("POST", "/api/samples", { project: "旧入口项目", borehole: "ZK-O", coreBox: "BX-O", depth: "1m", owner: "甲", sliceId: "SL-OLD", method: "常规制片" });
  check("POST /api/samples 建档仍可用", oldCreate.status === 201 && oldCreate.data.id.startsWith("CORE-"));
  const homeHtml = await (await fetch(base + "/")).text();
  check("旧页面 / 仍提供且含切片台链接", homeHtml.includes("/station"));
  const stationHtml = await (await fetch(base + "/station")).text();
  check("新页面 /station 可访问", !!stationHtml);
  // 回归：两个页面下发的内联脚本必须能被 JS 引擎解析（防止模板转义导致整页脚本崩溃）
  const vm = await import("node:vm");
  for (const [name, html] of [["旧页 /", homeHtml], ["切片台 /station", stationHtml]]) {
    const inline = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)].map(m => m[1]);
    let parseOk = true;
    for (const code of inline) { try { new vm.Script(code); } catch { parseOk = false; } }
    check(`${name} 内联脚本语法可解析`, parseOk && inline.length > 0);
  }

  // 1) 正常交付
  console.log("\n[场景1：正常交付]");
  check("批次建档成功", (await createBatch("B-HAPPY", ["H1"])).status === 201);
  await runFull("B-HAPPY", "H1", 1);
  let st = await getState();
  let b = st.batches.find(x => x.id === "B-HAPPY");
  check("切片按顺序到达观察且标记合格", b.slices[0].status === "观察" && b.slices[0].passed === true);
  check("批次状态为待观察", b.status === "待观察");
  // 默认配方：切割2 + 研磨1 + 染色3（取样/观察不耗试剂）
  check("试剂按配方扣减（切2/磨1/染3）",
    st.reagents.find(x => x.id === "RCUT").amount === 98 &&
    st.reagents.find(x => x.id === "RGRIND").amount === 99 &&
    st.reagents.find(x => x.id === "RDYE").amount === 97);
  check("批次累计试剂用量台账", b.reagentUsage.length === 3);
  const deliver = await call("POST", "/api/station/batches/B-HAPPY/deliver", {});
  check("交付成功（历史时段设备均已释放）", deliver.status === 200, reasonOf(deliver));
  check("交付核对汇总试剂用量", deliver.data.reagentUsage.length === 3);
  check("交付后锁定", deliver.data.batch.locked === true && deliver.data.batch.delivery === "已交付");

  // 2) 并发抢设备
  console.log("\n[场景2：并发抢设备 · 整次失败]");
  await createBatch("B-CONF-A", ["A1"]);
  await createBatch("B-CONF-B", ["B1"]);
  // 两片都还在待执行"取样"，取样不耗试剂
  const w0 = ["2026-09-20T01:00:00Z", "2026-09-20T02:00:00Z"];
  const w1 = ["2026-09-20T01:30:00Z", "2026-09-20T02:30:00Z"]; // 与 w0 重叠
  const payload = w => ({ personId: "QA", equipmentId: "E01", reagentId: "", startAt: w[0], endAt: w[1], note: "抢设备" });
  const [c1, c2] = await Promise.all([
    call("POST", "/api/station/batches/B-CONF-A/slices/A1/advance", payload(w0)),
    call("POST", "/api/station/batches/B-CONF-B/slices/B1/advance", payload(w1))
  ]);
  const codes = [c1.status, c2.status].sort().join(",");
  check("重叠时段仅一方成功（200 + 409）", codes === "200,409", `got ${codes}`);
  const loser = c1.status === 409 ? c1 : c2;
  check("失败方收到设备冲突阻断原因", reasonOf(loser).includes("时段冲突"), reasonOf(loser));
  st = await getState();
  const ba = st.batches.find(x => x.id === "B-CONF-A");
  const bb = st.batches.find(x => x.id === "B-CONF-B");
  const lostBatch = ba.slices[0].status === "取样" ? ba : bb;
  const wonBatch = lostBatch === ba ? bb : ba;
  check("失败方切片状态不变（仍待取样）", lostBatch.slices[0].status === "取样");
  check("失败方无操作记录/无试剂台账", lostBatch.operations.length === 0 && lostBatch.reagentUsage.length === 0);
  check("失败方未占用设备时段", !st.bookings.some(bk => bk.batchId === lostBatch.id));
  check("胜出方完成取样、流转到切割", wonBatch.slices[0].status === "切割");

  // 3) 同批共享试剂 · 余量不足
  console.log("\n[场景3：同批共享试剂 · 余量不足整次失败]");
  await createBatch("B-SHARE", ["S1", "S2"]);
  // 两片先各自完成取样（不耗试剂），用不冲突时段
  check("S1 取样成功", (await execStep("B-SHARE", "S1", "取样", { day: 2, slotIndex: 0 })).status === 200);
  check("S2 取样成功", (await execStep("B-SHARE", "S2", "取样", { day: 2, slotIndex: 1 })).status === 200);
  // 把 RCUT 余量调成恰好 2ml：只够一片切割
  await call("POST", "/api/station/reagents", { id: "RCUT", name: "切削冷却液", batchNo: "LOT-CUT", amount: 2, unit: "ml", expiresOn: "2027-12-31" });
  const ok1 = await execStep("B-SHARE", "S1", "切割", { day: 3, slotIndex: 0, note: "S1切割" });
  check("第一片切割成功扣 2ml（余 0）", ok1.status === 200 && ok1.data.consumed.remain === 0, reasonOf(ok1));
  const low = await execStep("B-SHARE", "S2", "切割", { day: 3, slotIndex: 1, note: "S2切割" });
  check("第二片因同批共享余量不足被阻断", isBlocked(low, "余量不足"), reasonOf(low));
  st = await getState();
  b = st.batches.find(x => x.id === "B-SHARE");
  check("余量不足时试剂余量不变（仍 0）", st.reagents.find(x => x.id === "RCUT").amount === 0);
  check("S2 状态不变（仍待切割）", b.slices.find(x => x.id === "S2").status === "切割");
  // S2 此前已合规做过取样（1 条），失败的切割不得新增切割记录/占用
  check("失败的切割不新增操作记录/设备占用",
    b.operations.filter(o => o.sliceId === "S2" && o.step === "切割").length === 0 &&
    !st.bookings.some(bk => bk.sliceId === "S2" && bk.step === "切割"));
  // 补库后同一次操作可立即重试成功
  await call("POST", "/api/station/reagents", { id: "RCUT", adjust: true, amount: 10 });
  const retry = await execStep("B-SHARE", "S2", "切割", { day: 3, slotIndex: 1, note: "S2切割补库后" });
  check("补库后重试成功", retry.status === 200, reasonOf(retry));

  // 4) 试剂过期 + 人员无资质 / 资质过期
  console.log("\n[场景4：试剂过期 · 人员无资质/资质过期]");
  await call("POST", "/api/station/reagents", { id: "ROLD", name: "过期冷却液", batchNo: "LOT-OLD", amount: 50, unit: "ml", expiresOn: "2025-01-01" });
  await createBatch("B-BAD", ["D1"]); // D1 待执行取样（不耗试剂），先合规执行取样
  check("D1 取样成功", (await execStep("B-BAD", "D1", "取样", { day: 4, slotIndex: 0 })).status === 200);
  // 现在待执行切割：切割需要资质 + 试剂 + 设备
  const cutSlot = { slot: daySlot(4, 1) };
  const expR = await execStep("B-BAD", "D1", "切割", { reagentId: "ROLD", ...cutSlot, note: "用过试剂" });
  check("过期试剂整次失败并说明过期", isBlocked(expR, "已过期"), reasonOf(expR));
  const noTitle = await execStep("B-BAD", "D1", "切割", { person: "QO", ...cutSlot, note: "无切割资质" });
  check("无对应工序资质被阻断", isBlocked(noTitle, "无切割工序资质"), reasonOf(noTitle));
  const expP = await execStep("B-BAD", "D1", "切割", { person: "QE", ...cutSlot, note: "资质过期人员" });
  check("资质过期被阻断", isBlocked(expP, "资质已过期"), reasonOf(expP));
  st = await getState();
  b = st.batches.find(x => x.id === "B-BAD");
  check("连续失败后状态/记录/余量/占用均不变",
    b.slices[0].status === "切割" && b.operations.length === 1 /* 只有取样 */ &&
    !st.bookings.some(bk => bk.batchId === "B-BAD" && bk.step === "切割") &&
    st.reagents.find(x => x.id === "ROLD").amount === 50);

  // 5) 失败回滚（多重阻断一次性报出）
  console.log("\n[场景5：失败回滚 · 多重阻断]");
  await createBatch("B-ROLL", ["F1"]);
  check("F1 取样成功", (await execStep("B-ROLL", "F1", "取样", { day: 8, slotIndex: 0 })).status === 200);
  const before = await getState();
  // 当前待执行切割（需切割资质 + 试剂 + 设备 + 合法时段），一次触发四类错误
  const multi = await call("POST", "/api/station/batches/B-ROLL/slices/F1/advance", {
    personId: "QO", equipmentId: "NOPE", reagentId: "ROLD",
    startAt: "2026-10-03T02:00:00Z", endAt: "2026-10-03T01:00:00Z", note: "多重错误"
  });
  const why = reasonOf(multi);
  check("多重问题一次全部报出（资质/设备/试剂/时段）",
    multi.status === 409 && why.includes("无切割工序资质") && why.includes("设备不存在") && why.includes("已过期") && why.includes("结束时间必须晚于"), why);
  st = await getState();
  b = st.batches.find(x => x.id === "B-ROLL");
  check("回滚彻底：状态仍待切割、无切割记录/占用",
    b.slices[0].status === "切割" &&
    b.operations.filter(o => o.step === "切割").length === 0 &&
    !st.bookings.some(bk => bk.batchId === "B-ROLL" && bk.step === "切割"));
  check("回滚后资源总量与失败前一致",
    st.bookings.length === before.bookings.length &&
    st.reagents.find(x => x.id === "ROLD").amount === 50);

  // 6) 观察不合格退回，未复检不能交付；复检后交付
  console.log("\n[场景6：观察不合格退回 · 复检后交付]");
  await createBatch("B-REWORK", ["W1"]);
  // 依次执行 取样/切割/研磨/染色（day 5）
  for (let i = 0; i < 4; i++) {
    const r = await execStep("B-REWORK", "W1", STEPS[i], { day: 5, slotIndex: i });
    if (r.status !== 200) throw new Error(reasonOf(r));
  }
  st = await getState();
  check("染色完成后进入待观察", st.batches.find(x => x.id === "B-REWORK").slices[0].status === "待观察");
  // 执行观察：不合格，退回研磨
  const failObs = await execStep("B-REWORK", "W1", "观察", { day: 5, slotIndex: 4, pass: false, returnTo: "研磨", note: "厚度不均" });
  check("观察不合格退回研磨", failObs.status === 200 && failObs.data.operation.returnTo === "研磨", reasonOf(failObs));
  st = await getState();
  b = st.batches.find(x => x.id === "B-REWORK");
  check("切片状态=待研磨、passed=false（复检中）", b.slices[0].status === "研磨" && b.slices[0].passed === false);
  const early = await call("POST", "/api/station/batches/B-REWORK/deliver", {});
  check("未复检交付被拒绝并说明阻断原因", isBlocked(early) && reasonOf(early).includes("尚未复检通过"), reasonOf(early));
  // 复检必须重走 研磨 → 染色 → 观察（day 6）
  let r = await execStep("B-REWORK", "W1", "研磨", { day: 6, slotIndex: 0, note: "研磨返工" });
  check("复检：重做研磨成功", r.status === 200, reasonOf(r));
  r = await execStep("B-REWORK", "W1", "染色", { day: 6, slotIndex: 1, note: "染色返工" });
  check("复检：重做染色成功（消耗试剂）", r.status === 200 && r.data.consumed && r.data.consumed.amount === 3, reasonOf(r));
  r = await execStep("B-REWORK", "W1", "观察", { day: 6, slotIndex: 2, pass: true, note: "复检合格" });
  check("复检：观察合格", r.status === 200, reasonOf(r));
  const deliverRework = await call("POST", "/api/station/batches/B-REWORK/deliver", {});
  check("复检通过后交付成功", deliverRework.status === 200, reasonOf(deliverRework));

  // 7) 设备未释放阻断交付
  console.log("\n[场景7：设备未释放拒绝交付]");
  await createBatch("B-BUSY", ["U1"]);
  for (let i = 0; i < 4; i++) {
    const rr = await execStep("B-BUSY", "U1", STEPS[i], { day: 7, slotIndex: i });
    if (rr.status !== 200) throw new Error(reasonOf(rr));
  }
  // 观察占用远未来仍未结束的显微镜时段 → 观察本身合格，但设备未释放
  const fo = await execStep("B-BUSY", "U1", "观察", {
    slot: ["2099-01-01T00:00:00Z", "2099-01-02T00:00:00Z"], pass: true, note: "合格但显微镜占着"
  });
  check("观察步骤本身成功（未来时段）", fo.status === 200, reasonOf(fo));
  const busyDeliver = await call("POST", "/api/station/batches/B-BUSY/deliver", {});
  check("设备未释放时交付被阻断", isBlocked(busyDeliver, "尚未释放"), reasonOf(busyDeliver));

  // 8) 交付锁定
  console.log("\n[场景8：交付锁定]");
  const again = await call("POST", "/api/station/batches/B-HAPPY/deliver", {});
  check("重复交付被拒绝", isBlocked(again, "锁定"), reasonOf(again));
  const lockedAdvance = await call("POST", "/api/station/batches/B-HAPPY/slices/H1/advance", {
    personId: "QA", equipmentId: "E01", reagentId: "", startAt: "2026-11-01T00:00:00Z", endAt: "2026-11-01T01:00:00Z", note: "锁定后操作"
  });
  check("锁定后推进被拒绝", isBlocked(lockedAdvance, "锁定"), reasonOf(lockedAdvance));

} finally {
  await stopServer(server);
}

// ───────── 重启：数据不丢 ─────────
console.log("\n[重启数据不丢]");
const onDisk = JSON.parse(await readFile(DB, "utf8"));
check("磁盘上已交付批次保持锁定", onDisk.batches.find(b => b.id === "B-HAPPY").locked === true);
check("磁盘保留试剂扣减结果（RGRIND 被扣过）", onDisk.reagents.find(r => r.id === "RGRIND").amount < 100);
check("磁盘保留设备时段占用记录", Array.isArray(onDisk.bookings) && onDisk.bookings.length >= 5);
check("磁盘保留旧入口样本", onDisk.samples.some(s => s.project === "旧入口项目"));
check("磁盘保留退回返工的复检轨迹", onDisk.batches.find(b => b.id === "B-REWORK").slices[0].passed === true);

const server2 = startServer();
await waitReady(server2);
try {
  const st2 = await (await fetch(base + "/api/station/state")).json();
  const samples2 = await (await fetch(base + "/api/samples")).json();
  check("重启后批次/状态恢复（已交付）", st2.batches.find(b => b.id === "B-HAPPY").delivery === "已交付");
  check("重启后未交付批次仍阻断（设备未释放）", st2.batches.find(b => b.id === "B-BUSY").slices[0].status === "观察");
  check("重启后旧入口数据仍在", Array.isArray(samples2) && samples2.some(s => s.project === "旧入口项目"));
  const reDeliver = await fetch(base + "/api/station/batches/B-HAPPY/deliver", { method: "POST", body: "{}" });
  check("重启后锁定依然生效", reDeliver.status === 409);
  const reBusy = await fetch(base + "/api/station/batches/B-BUSY/deliver", { method: "POST", body: "{}" });
  check("重启后设备未释放阻断依然生效", reBusy.status === 409);
} finally {
  await stopServer(server2);
}

console.log(`\n结果：${passCount} 通过，${failures.length} 失败`);
if (failures.length) { console.error("失败项：\n - " + failures.join("\n - ")); process.exit(1); }
console.log("全部实测通过 ✔");
