
module.exports = async function (req, res) {
  res.setHeader("Content-Type", "application/json");

  // POST 요청만 허용
  if (req.method !== "POST") {
    return res.status(405).json({ error: "Method Not Allowed" });
  }

  try {
    const { text, targetLang } = req.body || {};

    if (typeof text !== "string" || !text.trim()) {
      return res.status(400).json({
        error: "Translation text is required."
      });
    }

    if (typeof targetLang !== "string" || !targetLang.trim()) {
      return res.status(400).json({
        error: "Target language is required."
      });
    }

    if (text.length > 10000) {
      return res.status(413).json({
        error: "Text is too long to translate."
      });
    }

    const apiKey = process.env.GEMINI_API_KEY;

    if (!apiKey) {
      console.error("GEMINI_API_KEY is missing in Vercel environment.");

      return res.status(500).json({
        error: "API key is not configured on the server."
      });
    }

    const systemPrompt =
      `You are a professional translator. ` +
      `Translate the user's text into ${targetLang}. ` +
      `Keep the original meaning and tone natural for conversation. ` +
      `Output only the translated text without explanations or quotation marks.`;

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
          contents: [{
            role: "user",
            parts: [{ text: text.trim() }]
          }],
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

    if (!response.ok) {
      console.error("Gemini API error:", response.status);

      return res.status(
        response.status >= 400 && response.status < 500
          ? response.status
          : 502
      ).json({
        error: "Translation API failed.",
        status: response.status
      });
    }

    const data = await response.json();

    const translated = data.candidates?.[0]?.content?.parts
      ?.map(part => part.text || "")
      .join("")
      .trim() || "";

    if (!translated) {
      return res.status(502).json({
        error: "The translation service returned an empty response."
      });
    }

    res.setHeader("Cache-Control", "no-store");

    return res.status(200).json({ translated });

  } catch (error) {
    const isTimeout = error.name === "AbortError";

    console.error(
      isTimeout ? "Gemini request timed out." : "Translation function error:",
      error.message
    );

    return res.status(isTimeout ? 504 : 500).json({
      error: isTimeout
        ? "Translation request timed out. Please try again."
        : "An internal translation error occurred."
    });
  }
};
