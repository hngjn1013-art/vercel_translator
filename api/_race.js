// 번역 요청을 시간차로 겹쳐 보내고, 가장 먼저 첫 글자를 준 요청 하나를 고른다.
// (파일 이름이 _로 시작하면 Vercel이 API 주소로 만들지 않는다)
//
// attempts: [{ nextAfterMs, ...그 요청에 필요한 설정 }]
//   앞 요청의 첫 글자가 nextAfterMs 안에 안 오면 다음 요청을 겹쳐 보낸다. 실패하면 기다리지 않고 바로 다음 요청을 보낸다.
// startStream(config, signal): 글자 조각을 차례로 내놓는 async iterator를 돌려준다. (실제로는 Gemini, 시험에서는 가짜)
// totalTimeoutMs: 첫 글자를 기다리는 최대 시간(여러 번 시도한 시간을 모두 합친 것)
// signal: 이 신호가 오면(예: 사용자가 페이지를 떠남) 진행 중인 요청을 모두 취소한다.
//
// 결과: { attempt, first } 또는 { error }
//   attempt.iterator로 나머지 조각을 읽고, attempt.arm(ms)로 '이 시간 동안 조각이 없으면 끊기'를 다시 걸고,
//   다 읽었으면 attempt.stop()으로 정리한다. 이긴 요청 말고는 이 함수가 모두 취소한다.

class ProviderError extends Error {
  constructor(message, status) {
    super(message);
    this.status = status;
  }
}

async function raceFirstChunk({ attempts, startStream, totalTimeoutMs, signal }) {
  const startedAt = Date.now();
  const remainingMs = () => totalTimeoutMs - (Date.now() - startedAt);

  // 요청 하나를 시작한다. result는 첫 글자가 오면 { ok: true }, 실패하면 { ok: false }로 끝난다. (거부되지 않는다)
  const launch = config => {
    const controller = new AbortController();
    const attempt = { config, controller, timeoutId: null };
    attempt.arm = ms => {
      clearTimeout(attempt.timeoutId);
      attempt.timeoutId = setTimeout(() => controller.abort(), ms);
    };
    attempt.stop = () => {
      clearTimeout(attempt.timeoutId);
      controller.abort();
    };
    attempt.arm(remainingMs());
    attempt.iterator = startStream(config, controller.signal);
    attempt.result = attempt.iterator.next()
      .then(first => {
        if (first.done || !first.value) throw new ProviderError("Empty response", 502);
        return { ok: true, attempt, first: first.value };
      })
      .catch(error => ({ ok: false, attempt, error }));
    return attempt;
  };

  const pending = new Set();
  let launched = 0;
  let lastError = null;
  let winner = null;
  let hedgeTimerId = null;
  let hedgeTimer = null;

  // 이전 요청들을 그대로 둔 채 다음 요청을 겹쳐 보낸다.
  const launchNext = () => {
    clearTimeout(hedgeTimerId);
    hedgeTimer = null;
    const config = attempts[launched];
    pending.add(launch(config));
    launched++;
    // 이 요청의 첫 글자가 늦으면 다음 요청을 보내기 위한 타이머
    if (config.nextAfterMs != null && launched < attempts.length) {
      hedgeTimer = new Promise(resolve => {
        hedgeTimerId = setTimeout(() => resolve({ hedge: true }), config.nextAfterMs);
      });
    }
  };
  const canLaunchMore = () => launched < attempts.length && remainingMs() > 0 && !(signal && signal.aborted);

  const stopAll = () => { for (const attempt of pending) attempt.stop(); };
  if (signal) {
    if (signal.aborted) return { error: new ProviderError("Aborted by client", 499) };
    signal.addEventListener("abort", stopAll, { once: true });
  }

  try {
    launchNext();
    while (pending.size > 0) {
      const waiting = [...pending].map(attempt => attempt.result);
      if (hedgeTimer) waiting.push(hedgeTimer);
      const event = await Promise.race(waiting);

      if (event.hedge) {
        if (canLaunchMore()) launchNext();
        else hedgeTimer = null;
        continue;
      }
      if (event.ok) {
        winner = event;
        break;
      }

      // 실패한 요청은 목록에서 빼고, 아직 여유가 있으면 바로 다시 보낸다.
      pending.delete(event.attempt);
      clearTimeout(event.attempt.timeoutId);
      lastError = event.error;
      console.error("gemini failed:", event.error.name, event.error.message);
      // 한도 초과(429)일 때는 다시 보내도 소용없으니 포기한다.
      if (event.error.status === 429) break;
      if (canLaunchMore()) launchNext();
    }
  } finally {
    clearTimeout(hedgeTimerId);
    if (signal) signal.removeEventListener("abort", stopAll);
  }

  // 이긴 쪽 말고는 모두 취소한다.
  for (const attempt of pending) {
    if (!winner || attempt !== winner.attempt) attempt.stop();
  }
  if (!winner) return { error: lastError };
  return { attempt: winner.attempt, first: winner.first };
}

module.exports = { raceFirstChunk, ProviderError };
