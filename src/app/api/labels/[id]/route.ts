import { NextResponse } from "next/server";
import { deleteCustomLabel } from "@/lib/pipeline-store";

export async function DELETE(
  _request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  const { id } = await params;
  try {
    const deleted = await deleteCustomLabel(Number(id));
    if (!deleted) {
      return NextResponse.json({ error: "Label not found" }, { status: 404 });
    }
    return NextResponse.json({ ok: true });
  } catch (error) {
    return NextResponse.json(
      {
        error:
          error instanceof Error ? error.message : "Failed to delete label",
      },
      { status: 409 }
    );
  }
}
