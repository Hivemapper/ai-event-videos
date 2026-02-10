import { NextRequest, NextResponse } from "next/server";
import { readFile, writeFile } from "fs/promises";
import { join } from "path";

const CLAUDE_MD_PATH = join(process.cwd(), "CLAUDE.md");

export async function GET() {
  try {
    const content = await readFile(CLAUDE_MD_PATH, "utf-8");
    return NextResponse.json({ content });
  } catch (err: unknown) {
    if (err instanceof Error && "code" in err && (err as NodeJS.ErrnoException).code === "ENOENT") {
      return NextResponse.json({ content: "" });
    }
    return NextResponse.json({ error: "Failed to read CLAUDE.md" }, { status: 500 });
  }
}

export async function PUT(request: NextRequest) {
  try {
    const { content } = await request.json();
    if (typeof content !== "string") {
      return NextResponse.json({ error: "content must be a string" }, { status: 400 });
    }
    await writeFile(CLAUDE_MD_PATH, content, "utf-8");
    return NextResponse.json({ ok: true });
  } catch {
    return NextResponse.json({ error: "Failed to write CLAUDE.md" }, { status: 500 });
  }
}
