const fs = require("fs");
const path = require("path");

class Env{

LoadEnv(file = ".env") { console.log(path.join(__dirname,file))
  const envPath = path.resolve(process.cwd(), path.join(__dirname,file));

  if (!fs.existsSync(envPath)) {
    throw new Error(`File ${file} tidak ditemukan`);
  }

  const data = fs.readFileSync(envPath, "utf8");

  data.split("\n").forEach((line) => {
    line = line.trim();

    // skip kosong & komentar
    if (!line || line.startsWith("#")) return;

    const [key, ...values] = line.split("=");
    const value = values.join("=").trim();

    // jangan overwrite env yang sudah ada
    if (!process.env[key]) {
      process.env[key] = value;
    }
  });
}
}


module.exports = new Env();
