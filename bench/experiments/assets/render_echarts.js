#!/usr/bin/env node
/* 服务端渲染：读 option JSON，用内置 echarts 出 SVG。
   用法: node render_echarts.js <option.json> <out.svg>
   option 里的 __width / __height 控制画布尺寸，渲染前摘掉。 */
const fs = require("fs");
const path = require("path");
const echarts = require(path.join(__dirname, "echarts.min.js"));
const addErrorBars = require(path.join(__dirname, "errorbar.js")).addErrorBars;
const addDumbbells = require(path.join(__dirname, "errorbar.js")).addDumbbells;

const [, , optPath, outPath] = process.argv;
if (!optPath || !outPath) {
  console.error("用法: node render_echarts.js <option.json> <out.svg>");
  process.exit(2);
}

const option = JSON.parse(fs.readFileSync(optPath, "utf8"));
const width = option.__width || 900;
const height = option.__height || 400;
delete option.__width;
delete option.__height;
addErrorBars(option);
addDumbbells(option);

const chart = echarts.init(null, null, { renderer: "svg", ssr: true, width, height });
chart.setOption(option);
fs.writeFileSync(outPath, chart.renderToSVGString());
chart.dispose();
process.exit(0);