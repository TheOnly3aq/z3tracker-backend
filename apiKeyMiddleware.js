const allowedKeys = (process.env.API_KEYS || "")
    .split(",")
    .map(k => k.trim())
    .filter(Boolean);

function checkApiKey(req, res, next) {
    const headerKey = req.header("x-api-key");
    const auth = req.header("authorization") || "";
    const authMatch = auth.match(/^ApiKey\s+(.+)$/i);
    const token = headerKey || (authMatch ? authMatch[1] : null);

    if (!token) {
        return res.status(401).json({error: "Missing API key"});
    }
    if (!allowedKeys.includes(token)) {
        return res.status(403).json({error: "Invalid API key"});
    }

    req.apiKey = token;
    next();
}

module.exports = checkApiKey;