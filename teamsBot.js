require("dotenv").config({ path: __dirname + "/env/.env.dev" });

const { TeamsActivityHandler, TurnContext } = require("botbuilder");
const CortexChat = require("./cortexChat");
const JWTGenerator = require("./jwtGenerator");
const SnowflakeQueryExecutor = require("./snowflakeQueryExecutor");

class TeamsBot extends TeamsActivityHandler {
    constructor() {
        super();

        // Load environment variables and pass them to CortexChat
        const jwt = new JWTGenerator().getToken();
        const agentUrl = process.env.AGENT_ENDPOINT;
        const model = process.env.MODEL;
        const searchService = process.env.SEARCH_SERVICE;
        const semanticModels = [
            process.env.SUPPORT_SEMANTIC_MODEL, 
            process.env.SUPPLY_CHAIN_SEMANTIC_MODEL
        ];

        this.cortexChat = new CortexChat(jwt, agentUrl, model, searchService, semanticModels);

        this.onMessage(async (context, next) => {
            const prompt = TurnContext.removeRecipientMention(context.activity).trim();
            await context.sendActivity("Snowflake Cortex AI is generating a response. Please wait...");
            const response = await this.cortexChat._retrieveResponse(prompt);

            if (response.sql) {
                const executor = new SnowflakeQueryExecutor();
                const df = await executor.runQuery(response.sql);
                await context.sendActivity(`\`\`\`\n${df}\n\`\`\``);
                executor.closeConnection();
            } else {
                await context.sendActivity(response.citations ? `${response.text}\n\r\n\r+Citation: ${response.citations}` : response.text);
            }
            await next();
        });
    }
}

module.exports = TeamsBot;
