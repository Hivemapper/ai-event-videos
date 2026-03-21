import { NextResponse } from "next/server";
import { createCustomLabel, listLabels } from "@/lib/pipeline-store";

export function GET() {
  return NextResponse.json(listLabels());
}

export async function POST(request: Request) {
  const body = await request.json();
  const name = (body.name as string)?.trim().toLowerCase();

  if (!name) {
    return NextResponse.json({ error: "Name is required" }, { status: 400 });
  }

  try {
    const label = createCustomLabel(name);
    return NextResponse.json(label, { status: 201 });
  } catch (err: unknown) {
    if (err instanceof Error && err.message.includes("UNIQUE")) {
      return NextResponse.json({ error: "Label already exists" }, { status: 409 });
    }
    throw err;
  }
}
