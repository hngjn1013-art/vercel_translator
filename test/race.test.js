// 번역 요청 경주(api/_race.js) 시험. 실행: node --test test/
const test = require("node:test");
const assert = require("node:assert/strict");
const { raceFirstChunk, ProviderError } = require("../api/_race");

// 가짜 스트림: delay 뒤에 chunks를 내놓거나, error를 던진다. 취소 신호가 오면 AbortError로 멈춘다.
function fakeStream({ delay = 0, chunks = ["안녕"], error = null }) {
  return async function* (config, signal) {
    const log = fakeStream.log;
    log.push({ name: config.name, signal });
    await new Promise((resolve, reject) => {
      const timer = setTimeout(resolve, delay);
      signal.addEventListener("abort", () => {
        clearTimeout(timer);
        reject(Object.assign(new Error("aborted"), { name: "AbortError" }));
      }, { once: true });
    });
    if (error) throw error;
    for (const chunk of chunks) yield chunk;
  };
}

// 시도별로 다른 가짜 스트림을 쓰도록 묶는다.
function run(behaviors, { hedgeMs = 30, totalTimeoutMs = 2000, signal } = {}) {
  fakeStream.log = [];
  const attempts = behaviors.map((behavior, i) => ({ name: "try" + (i + 1), nextAfterMs: i < behaviors.length - 1 ? hedgeMs : null, behavior }));
  return raceFirstChunk({
    attempts,
    startStream: (config, signal) => fakeStream(config.behavior)(config, signal),
    totalTimeoutMs,
    signal
  });
}

const quiet = () => { const original = console.error; console.error = () => {}; return () => { console.error = original; }; };

test("1차가 빨리 오면 1차만 보내고 1차를 쓴다", async () => {
  const result = await run([{ delay: 5, chunks: ["가", "나"] }, {}, {}]);
  assert.equal(result.first, "가");
  assert.equal(result.attempt.config.name, "try1");
  assert.equal(fakeStream.log.length, 1);
  const rest = [];
  for await (const piece of result.attempt.iterator) rest.push(piece);
  assert.deepEqual(rest, ["나"]);
  result.attempt.stop();
});

test("1차가 늦으면 2차를 겹쳐 보내고, 먼저 온 2차를 쓰며 1차는 취소한다", async () => {
  const result = await run([{ delay: 500 }, { delay: 5, chunks: ["둘"] }, { delay: 500 }]);
  assert.equal(result.first, "둘");
  assert.equal(result.attempt.config.name, "try2");
  assert.equal(fakeStream.log.length, 2);
  assert.equal(fakeStream.log[0].signal.aborted, true);
  result.attempt.stop();
});

test("한도 초과(429)면 더 보내지 않고 바로 포기한다", async () => {
  const restore = quiet();
  try {
    const result = await run([{ delay: 1, error: new ProviderError("limit", 429) }, {}, {}]);
    assert.equal(result.attempt, undefined);
    assert.equal(result.error.status, 429);
    assert.equal(fakeStream.log.length, 1);
  } finally { restore(); }
});

test("실패하면 기다리지 않고 다음 요청을 보내고, 모두 실패하면 마지막 오류를 돌려준다", async () => {
  const restore = quiet();
  try {
    const result = await run([
      { delay: 1, error: new ProviderError("down", 500) },
      { delay: 1, error: new ProviderError("down", 503) },
      { delay: 1, error: new ProviderError("down", 502) }
    ], { hedgeMs: 1000 });
    assert.equal(result.attempt, undefined);
    assert.equal(result.error.status, 502);
    assert.equal(fakeStream.log.length, 3);
  } finally { restore(); }
});

test("빈 응답이 오면 실패로 보고 다음 요청으로 넘어간다", async () => {
  const restore = quiet();
  try {
    const result = await run([{ delay: 1, chunks: [] }, { delay: 1, chunks: ["다음"] }, {}], { hedgeMs: 1000 });
    assert.equal(result.first, "다음");
    assert.equal(result.attempt.config.name, "try2");
    result.attempt.stop();
  } finally { restore(); }
});

test("첫 글자를 기다리는 전체 시간이 지나면 모두 취소하고 실패한다", async () => {
  const restore = quiet();
  try {
    const result = await run([{ delay: 1000 }, { delay: 1000 }, { delay: 1000 }], { hedgeMs: 20, totalTimeoutMs: 100 });
    assert.equal(result.attempt, undefined);
    assert.equal(result.error.name, "AbortError");
    assert.ok(fakeStream.log.every(entry => entry.signal.aborted));
  } finally { restore(); }
});

test("바깥 취소 신호(사용자가 떠남)가 오면 진행 중인 요청을 모두 취소한다", async () => {
  const restore = quiet();
  try {
    const controller = new AbortController();
    setTimeout(() => controller.abort(), 50);
    const result = await run([{ delay: 1000 }, { delay: 1000 }, { delay: 1000 }], { hedgeMs: 20, signal: controller.signal });
    assert.equal(result.attempt, undefined);
    assert.ok(fakeStream.log.length >= 2);
    assert.ok(fakeStream.log.every(entry => entry.signal.aborted));
  } finally { restore(); }
});
