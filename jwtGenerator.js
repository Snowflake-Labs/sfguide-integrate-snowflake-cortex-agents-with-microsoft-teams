const fs = require("fs");
const jwt = require("jsonwebtoken");
const crypto = require("crypto");

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
        this.privateKey = fs.readFileSync(process.env.RSA_PRIVATE_KEY_PATH, "utf8");
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

module.exports = JWTGenerator;
