
exports.handler = async function(event, context) {
  // 1. POST 요청만 허용
  if (event.httpMethod !== "POST") {
    return {
      statusCode: 405,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: "Method Not Allowed" })
    };
  }

  try {
    // 2. 요청 데이터 검증
    let requestData;

    try {
      requestData = JSON.parse(event.body || "{}");
    } catch {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Invalid JSON request." })
      };
    }

    const { text, targetLang } = requestData;

    if (typeof text !== "string" || !text.trim()) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Translation text is required." })
      };
    }

    if (typeof targetLang !== "string" || !targetLang.trim()) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Target language is required." })
      };
    }

    // 입력 길이 제한: 불필요하게 긴 요청 방지
    if (text.length > 10000) {
      return {
        statusCode: 413,
        body: JSON.stringify({ error: "Text is too long to translate." })
      };
    }

    // 3. 서버 환경변수에서 API 키 가져오기
    const apiKey = process.env.GEMINI_API_KEY;

    if (!apiKey) {
      console.error("GEMINI_API_KEY is missing in Netlify environment.");

      return {
        statusCode: 500,
        body: JSON.stringify({
          error: "API key is not configured on the server."
        })
      };
    }

    // 4. 번역 지시문
    const systemPrompt =
      `You are a professional translator. ` +
      `Translate the user's text into ${targetLang}. ` +
      `Keep the original meaning and tone natural for conversation. ` +
      `Output only the translated text without explanations or quotation marks.`;

    // 5. Gemini API 호출
    const model = "gemini-3.5-flash-lite";
    const endpoint =
      `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent`;

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 25000);

    let response;

    try {
      response = await fetch(endpoint, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-goog-api-key": apiKey
        },
        body: JSON.stringify({
          systemInstruction: {
            parts: [{ text: systemPrompt }]
          },
          contents: [
            {
              role: "user",
              parts: [{ text: text.trim() }]
            }
          ],
          generationConfig: {
            temperature: 0.2,
            maxOutputTokens: 512
          }
        }),
        signal: controller.signal
      });
    } finally {
      clearTimeout(timeoutId);
    }

    // 6. Gemini API 오류 처리
    if (!response.ok) {
      const errorBody = await response.text();

      // API 키나 요청 헤더 등 민감한 정보는 기록하지 않음
      console.error("Gemini API error:", response.status, errorBody);

      return {
        statusCode: response.status >= 400 && response.status < 500
          ? response.status
          : 502,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          error: "Translation API failed.",
          status: response.status
        })
      };
    }

    // 7. 응답 데이터 확인
    const data = await response.json();
    const translated = data.candidates?.[0]?.content?.parts
      ?.map(part => part.text || "")
      .join("")
      .trim() || "";

    if (!translated) {
      console.error(
        "Gemini returned no translation:",
        JSON.stringify({
          promptFeedback: data.promptFeedback,
          finishReason: data.candidates?.[0]?.finishReason
        })
      );

      return {
        statusCode: 502,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          error: "The translation service returned an empty response."
        })
      };
    }

    // 8. 번역 성공
    return {
      statusCode: 200,
      headers: {
        "Content-Type": "application/json",
        "Cache-Control": "no-store"
      },
      body: JSON.stringify({ translated })
    };

  } catch (error) {
    // 9. 네트워크 오류 및 시간 초과 처리
    const isTimeout = error.name === "AbortError";

    console.error(
      isTimeout ? "Gemini request timed out." : "Translation function error:",
      error.message
    );

    return {
      statusCode: isTimeout ? 504 : 500,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        error: isTimeout
          ? "Translation request timed out. Please try again."
          : "An internal translation error occurred."
      })
    };
  }
};
