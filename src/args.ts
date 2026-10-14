/** `--name value`, `--name=value` and bare `--flag`; everything else is positional */
export function parseArgs(argv: string[], booleans: string[]): { pos: string[]; flags: Record<string, string | true> } {
  const pos: string[] = [];
  const flags: Record<string, string | true> = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]!;
    if (!a.startsWith('--')) { pos.push(a); continue; }
    const eq = a.indexOf('=');
    const name = a.slice(2, eq < 0 ? undefined : eq);
    if (eq >= 0) flags[name] = a.slice(eq + 1);
    else if (booleans.includes(name) || i + 1 >= argv.length || argv[i + 1]!.startsWith('--')) flags[name] = true;
    else flags[name] = argv[++i]!;
  }
  return { pos, flags };
}
