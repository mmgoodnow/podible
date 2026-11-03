declare module "*.html" { const bundle: import("bun").HTMLBundle; export default bundle; }
declare module "*.svg" { const url: string; export default url; }
