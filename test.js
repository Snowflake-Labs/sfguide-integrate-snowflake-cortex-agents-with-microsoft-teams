require('dotenv').config({ path: 'env/.env.dev' });
const { exec } = require('child_process');
const JWTGenerator = require('./jwtGenerator');

const jwt = new JWTGenerator();
const jwtToken = jwt.getToken();

const curlCmd = `curl -X POST ${process.env.AGENT_ENDPOINT} \
-H "X-Snowflake-Authorization-Token-Type: KEYPAIR_JWT" \
-H "Authorization: Bearer ${jwtToken}" \
-H "Content-Type: application/json" \
-H "Accept: application/json" \
-d '{
  "model": "claude-3-5-sonnet",
  "messages": [
    {
      "role": "user",
      "content": [
        {
          "type": "text",
          "text": "Can you show me a breakdown of customer support tickets by service type cellular vs business internet?"
        }
      ]
    }
  ],
  "tools": [
    {
      "tool_spec": {
        "type": "cortex_search",
        "name": "vehicles_info_search"
      }
    },
    {
      "tool_spec": {
        "type": "cortex_analyst_text_to_sql",
        "name": "supply_chain"
      }
    }
  ],
  "tool_resources": {
    "vehicles_info_search": {
      "name": "${process.env.VEHICLE_SEARCH_SERVICE}",
      "max_results": 1,
      "title_column": "title",
      "id_column": "relative_path"
    },
    "supply_chain": {
      "semantic_model_file": "${process.env.SUPPORT_SEMANTIC_MODEL}"
    }
  }
}'`;

exec(curlCmd, (err, stdout, stderr) => {
  if (err) {
    console.error('❌ curl error:', err.message);
    return;
  }
  console.log('✅ Cortex Agents response:\n\n', stdout);
});
