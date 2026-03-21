import fs from "fs";
import path from "path";

export const runtime = "nodejs";

export async function GET(
  request: Request,
  { params }: { params: Promise<{ videoId: string; runId: string }> }
) {
  const { runId } = await params;
  const logPath = path.join(
    process.cwd(),
    "data",
    "pipeline-logs",
    `detection-${runId}.log`
  );

  const stream = new ReadableStream({
    start(controller) {
      const encoder = new TextEncoder();
      let lastSize = 0;

      const sendChunk = (text: string) => {
        controller.enqueue(
          encoder.encode(`data: ${JSON.stringify(text)}\n\n`)
        );
      };

      // Read existing content first
      try {
        const existing = fs.readFileSync(logPath, "utf-8");
        if (existing) {
          sendChunk(existing);
          lastSize = fs.statSync(logPath).size;
        }
      } catch {
        // File doesn't exist yet, that's ok
      }

      // Poll for new content every 500ms
      const interval = setInterval(() => {
        try {
          const stat = fs.statSync(logPath);
          if (stat.size > lastSize) {
            const fd = fs.openSync(logPath, "r");
            const buf = Buffer.alloc(stat.size - lastSize);
            fs.readSync(fd, buf, 0, buf.length, lastSize);
            fs.closeSync(fd);
            sendChunk(buf.toString("utf-8"));
            lastSize = stat.size;
          }
        } catch {
          // File may not exist yet
        }
      }, 500);

      // Clean up on client disconnect
      request.signal.addEventListener("abort", () => {
        clearInterval(interval);
        controller.close();
      });

      // Also cap at 5 minutes to prevent indefinite connections
      setTimeout(() => {
        clearInterval(interval);
        try {
          controller.close();
        } catch {
          // Already closed by abort
        }
      }, 5 * 60 * 1000);
    },
  });

  return new Response(stream, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
    },
  });
}
