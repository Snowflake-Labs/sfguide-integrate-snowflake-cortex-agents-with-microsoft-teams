require("dotenv").config({ path: __dirname + "/env/.env.dev" });

const { TeamsActivityHandler, TurnContext } = require("botbuilder");
const fs = require("fs");
const jwt = require("jsonwebtoken");
const crypto = require("crypto");
const snowflake = require('snowflake-sdk');
const dfd = require('danfojs-node');

/**
 * Utility function to prepare the account name for JWT.
 */
const prepareAccountNameForJWT = (rawAccount) => {
    let account = rawAccount.includes(".global") ? rawAccount.split("-")[0] : rawAccount.split(".")[0];
    return account.toUpperCase();
};

class JWTGenerator {
    constructor() {
        this.account = prepareAccountNameForJWT(process.env.ACCOUNT);
        this.user = process.env.DEMO_USER.toUpperCase();
        this.qualifiedUsername = `${this.account}.${this.user}`;
        this.lifetime = 180 * 60;
        this.renewalDelay = 180 * 60;
        this.privateKey = fs.readFileSync(process.env.PRIVATE_KEY_PATH, "utf8");
        this.renewTime = Date.now() / 1000;
        this.token = this.generateToken();
    }

    generateToken() {
        const now = Date.now() / 1000;
        this.renewTime = now + this.renewalDelay;
        const payload = {
            iss: `${this.qualifiedUsername}.${this.calculatePublicKeyFingerprint()}`,
            sub: this.qualifiedUsername,
            iat: now,
            exp: now + this.lifetime,
        };
        return jwt.sign(payload, this.privateKey, { algorithm: "RS256" });
    }

    getToken() {
        if (Date.now() / 1000 >= this.renewTime) {
            this.token = this.generateToken();
        }
        return this.token;
    }

    calculatePublicKeyFingerprint() {
        const publicKey = crypto.createPublicKey(this.privateKey);
        const derPublicKey = publicKey.export({ type: "spki", format: "der" });
        return `SHA256:${crypto.createHash("sha256").update(derPublicKey).digest("base64")}`;
    }
}

class CortexChat {
    constructor() {
        this.agentUrl = process.env.AGENT_ENDPOINT;
        this.model = process.env.MODEL;
        this.searchService = process.env.SEARCH_SERVICE;
        this.semanticModel = process.env.SEMANTIC_MODEL;
        this.jwtGenerator = new JWTGenerator();
    }

    async _retrieveResponse(query, limit = 1) {
        const headers = {
            'X-Snowflake-Authorization-Token-Type': 'KEYPAIR_JWT',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': `Bearer ${this.jwtGenerator.getToken()}`
        };

        const data = {
            model: this.model,
            messages: [{ role: "user", content: [{ type: "text", text: query }] }],
            tools: [
                { tool_spec: { type: "cortex_search", name: "vehicles_info_search" } },
                { tool_spec: { type: "cortex_analyst_text_to_sql", name: "supply_chain" } }
            ],
            tool_resources: {
                vehicles_info_search: {
                    name: this.searchService,
                    max_results: limit,
                    title_column: "title",
                    id_column: "relative_path"
                },
                supply_chain: { semantic_model_file: this.semanticModel }
            }
        };

        try {
            const response = await fetch(this.agentUrl, { method: "POST", headers, body: JSON.stringify(data) });
            if (!response.ok) throw new Error(`Response status: ${response.status}`);
            return await this._parseResponse(response);
        } catch (error) {
            console.error("Error fetching response:", error);
            return { text: "An error occurred." };
        }
    }

    async _parseResponse(response) {
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let accumulated = { text: "", tool_results: [] };
        let done = false;
    
        while (!done) {
            const { value, done: readerDone } = await reader.read();
            if (value) {
                const chunk = decoder.decode(value, { stream: true });
                const result = this._processSSELine(chunk);
                if (result.type === "message") {
                    accumulated.text += result.content.text;
                    accumulated.tool_results.push(...result.content.tool_results);
                }
            }
            done = readerDone;
        }
    
        let text = accumulated.text;
        let sql = "";
        let citations = "";
    
        // Process tool_results which contains objects with 'content' array
        if (Array.isArray(accumulated.tool_results)) {
            accumulated.tool_results.forEach(result => {
                if (result.content && Array.isArray(result.content)) {
                    result.content.forEach(contentItem => {
                        if (contentItem.json) {
                            // Check for SQL in the json object
                            if (contentItem.json.sql) {
                                sql = contentItem.json.sql;
                            }
    
                            // Check for searchResults in the json object
                            if (contentItem.json.searchResults) {
                                contentItem.json.searchResults.forEach(searchResult => {
                                    // citations += `\n[Source: ${searchResult.doc_id}]`;
                                    // text = text.replace(/【†[1-3]†】/g, "").replace(" .", ".") + "+";
                                    citations += `${searchResult.text}`;
                                    text = text.replace(/【†[1-3]†】/g, "").replace(" .", ".") + "+";
                                    citations = ` \n ${citations} \n\n[Source: ${searchResult.doc_id}]`;
                                });
                            }
                        }
                    });
                } else {
                    console.warn("Unexpected structure in content:", result.content);
                }
            });
        } else {
            console.warn("tool_results is not an array:", accumulated.tool_results);
        }
    
        return { text, sql, citations };
    }    
    
    _processSSELine(line) {
        try {
            const jsonStr = line.split("\n")[1]?.slice(6)?.trim();
            if (!jsonStr || jsonStr === "[DONE]") return { type: "done" };
            const data = JSON.parse(jsonStr);
            if (data.object === "message.delta" && data.delta.content) {
                return { type: "message", content: this._parseDeltaContent(data.delta.content) };
            }
            return { type: "other", data };
        } catch (error) {
            return { type: "error", message: `Failed to parse: ${line}` };
        }
    }

    _parseDeltaContent(content) {
        return content.reduce((acc, entry) => {
            if (entry.type === "text") acc.text += entry.text;
            else if (entry.type === "tool_results") acc.tool_results.push(entry.tool_results);
            return acc;
        }, { text: "", tool_results: [] });
    }
}

class SnowflakeQueryExecutor {
    constructor() {
        this.connection = null;
    }

    async connect() {
        if (this.connection) return this.connection;
        this.connection = snowflake.createConnection({
            account: process.env.ACCOUNT,
            username: process.env.DEMO_USER,
            password: process.env.DEMO_USER_PASSWORD,
            warehouse: process.env.WAREHOUSE,
            database: process.env.DEMO_DATABASE,
            schema: process.env.DEMO_SCHEMA
        });
        return new Promise((resolve, reject) => {
            this.connection.connect(err => (err ? reject(err) : resolve(this.connection)));
        });
    }

    executeQuery(sql) {
        return new Promise((resolve, reject) => {
            this.connection.execute({
                sqlText: sql,
                complete: (err, stmt, rows) => (err ? reject(err) : resolve(rows))
            });
        });
    }

    closeConnection() {
        if (this.connection) this.connection.destroy();
    }

    async runQuery(sql) {
        await this.connect();
        const results = await this.executeQuery(sql);
        return new dfd.DataFrame(results);
    }
}

module.exports.TeamsBot = class TeamsBot extends TeamsActivityHandler {
    constructor() {
        super();
        this.cortexChat = new CortexChat();
        this.onMessage(async (context, next) => {
            const prompt = TurnContext.removeRecipientMention(context.activity).trim();
            await context.sendActivity("Snowflake Cortex AI is generating a response...");
            const response = await this.cortexChat._retrieveResponse(prompt);

            if (response.sql) {
                const executor = new SnowflakeQueryExecutor();
                const df = await executor.runQuery(response.sql);
                await context.sendActivity(`\`\`\`\n${df}\n\`\`\``);
                executor.closeConnection();
            } else {
                await context.sendActivity(response.citations ? `${response.text}\nCitation: ${response.citations}` : response.text);
            }
            await next();
        });
    }
};
